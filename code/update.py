import argparse
import datetime
import itertools
import json
import logging
import pathlib
import resource
import sys
import time

import h5py
import remfile
import s3fs
import zarr

# Testing mode processes only this many items and writes to its own designated file
# (`derivatives/testing.jsonl`), leaving the real cache untouched.
_TESTING_LIMIT = 10
_CACHE_FILE_NAME = "valid_nwb_file_to_number_of_groups.jsonl"
_TESTING_FILE_NAME = "testing.jsonl"

# Each run writes its log to `logs/` next to `derivatives/`. In the pipeline that directory is
# an output of the recorded run, so the log of every completed update is committed to the
# `derivatives` branch alongside the results it produced.
_LOG_DIRECTORY_NAME = "logs"

logger = logging.getLogger("update")

# The input is the `content-id-to-valid-nwb-file` cache, registered as an input subdataset.
_INPUT_FILE_PATH = (
    pathlib.Path("sourcedata") / "content-id-to-valid-nwb-file" / "derivatives" / "content_id_to_valid_nwb_file.jsonl"
)

# The public DANDI archive S3 bucket. Every asset is content-addressed, so each valid NWB
# file is reachable directly from its content ID without consulting the DANDI API:
#   - HDF5 assets are stored as a single blob at `blobs/<c[:3]>/<c[3:6]>/<content_id>`.
#   - Zarr assets are stored as a directory store under `zarr/<content_id>/`.
# The content ID alone does not say which layout an entry uses, so the blob key is probed
# first and the entry is treated as Zarr when no such blob exists.
_BUCKET = "dandiarchive"
_BLOB_URL_TEMPLATE = "https://dandiarchive.s3.amazonaws.com/blobs/{prefix}/{infix}/{content_id}"


def _load_content_id_to_validity(file_path: pathlib.Path) -> dict:
    """Load the `{content_id: bool}` mapping from the input JSONL, or an empty dict if missing."""
    records: dict = {}
    if not file_path.exists():
        return records
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _load_previous_cache(file_path: pathlib.Path) -> dict:
    """Load the previously computed `{content_id: number_of_groups}` mapping (empty on bootstrap)."""
    records: dict = {}
    if not file_path.exists():
        return records
    with file_path.open(mode="r") as file_stream:
        for line in file_stream:
            if line.strip():
                records.update(json.loads(line))
    return records


def _write_cache(file_path: pathlib.Path, records: dict) -> None:
    """Write the `{content_id: number_of_groups}` mapping, one sorted content ID per line."""
    with file_path.open(mode="w") as file_stream:
        file_stream.writelines(f"{json.dumps({content_id: records[content_id]})}\n" for content_id in sorted(records))


def _configure_logging(log_directory: pathlib.Path, testing: bool) -> pathlib.Path:
    """Send the log to stdout and to a new timestamped file under `log_directory`; return the file.

    Stdout is the live view in the CI job log, and is flushed per record so that a run killed
    mid-batch still shows how far it got. The file is the persistent copy, kept with the
    results. A testing run gets its own file name prefix so it is never mistaken for an update
    of the real cache.
    """
    log_directory.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    log_file_path = log_directory / f"{'testing' if testing else 'update'}_{timestamp}.log"

    formatter = logging.Formatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    formatter.converter = time.gmtime  # UTC timestamps, matching the file name and the CI log.

    logger.setLevel(logging.INFO)
    for handler in (logging.StreamHandler(stream=sys.stdout), logging.FileHandler(filename=log_file_path)):
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return log_file_path


def _peak_memory_mib() -> float:
    """Peak resident set size of this process so far, in MiB (Linux reports `ru_maxrss` in KiB)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _count_hdf5_groups(content_id: str) -> tuple[int, str]:
    """Stream an HDF5 asset and count its groups, the root group included.

    Returns the count and a short description of the asset (its layout and size) for the log.
    """
    blob_url = _BLOB_URL_TEMPLATE.format(prefix=content_id[:3], infix=content_id[3:6], content_id=content_id)
    rem_file = remfile.File(url=blob_url)
    with h5py.File(name=rem_file, mode="r") as h5py_file:
        number_of_groups = 1  # The root `/` is itself a group; `visititems` does not visit it.

        def _visit(_name: str, obj: object) -> None:
            nonlocal number_of_groups
            if isinstance(obj, h5py.Group):
                number_of_groups += 1

        h5py_file.visititems(_visit)
    return number_of_groups, f"HDF5, {rem_file.length / 1e6:.1f} MB"


def _count_zarr_groups(s3_filesystem: s3fs.S3FileSystem, content_id: str) -> tuple[int, str]:
    """Stream a Zarr asset's metadata and count its groups, the root group included.

    Returns the count and a short description of the asset (its layout) for the log.
    """
    store = s3fs.S3Map(root=f"{_BUCKET}/zarr/{content_id}", s3=s3_filesystem, check=False)
    # DANDI writes consolidated metadata (`.zmetadata`) for every Zarr asset, so the whole
    # hierarchy loads in a single request and the walk below never touches the network again.
    # Fall back to the plain store for the rare asset that lacks it.
    try:
        root_group = zarr.open_consolidated(store=store, mode="r")
    except KeyError:
        root_group = zarr.open_group(store=store, mode="r")

    number_of_groups = 1  # The root group.

    def _walk(group: zarr.hierarchy.Group) -> None:
        nonlocal number_of_groups
        for _name, subgroup in group.groups():
            number_of_groups += 1
            _walk(subgroup)

    _walk(root_group)
    return number_of_groups, "Zarr"


def _count_groups(s3_filesystem: s3fs.S3FileSystem, content_id: str) -> tuple[int, str]:
    """Count the total number of groups in the valid NWB file identified by `content_id`.

    Returns the count and a short description of the asset for the log.
    """
    blob_key = f"{_BUCKET}/blobs/{content_id[:3]}/{content_id[3:6]}/{content_id}"
    if s3_filesystem.exists(blob_key):
        return _count_hdf5_groups(content_id=content_id)
    return _count_zarr_groups(s3_filesystem=s3_filesystem, content_id=content_id)


def _run(base_directory: pathlib.Path, testing: bool, limit: int | None) -> None:
    log_file_path = _configure_logging(log_directory=base_directory / _LOG_DIRECTORY_NAME, testing=testing)
    logger.info("Logging to %s.", log_file_path)

    content_id_to_validity = _load_content_id_to_validity(file_path=base_directory / _INPUT_FILE_PATH)
    # Only the assets the upstream cache marked valid ('true') are counted.
    valid_content_ids = {content_id for content_id, is_valid in content_id_to_validity.items() if is_valid is True}

    derivatives_directory = base_directory / "derivatives"
    derivatives_directory.mkdir(parents=True, exist_ok=True)
    cache_file_path = derivatives_directory / (_TESTING_FILE_NAME if testing else _CACHE_FILE_NAME)
    valid_nwb_file_to_number_of_groups = _load_previous_cache(file_path=cache_file_path)

    # Already-counted content IDs are exactly the keys already in the output, so re-runs skip
    # them and only pick up content IDs newly marked valid upstream.
    content_ids_to_process = sorted(valid_content_ids - valid_nwb_file_to_number_of_groups.keys())

    # A testing run caps the batch tightly; otherwise the optional `--limit` bounds a single
    # run because streaming and walking each file is heavy.
    effective_limit = _TESTING_LIMIT if testing else limit
    content_ids_to_process = list(itertools.islice(content_ids_to_process, effective_limit))

    number_to_process = len(content_ids_to_process)
    logger.info("Processing %d newly valid content IDs.", number_to_process)
    batch_start_time = time.monotonic()

    s3_filesystem = s3fs.S3FileSystem(anon=True)
    for index, content_id in enumerate(content_ids_to_process, start=1):
        progress = f"[{index}/{number_to_process}] {content_id}"
        item_start_time = time.monotonic()
        try:
            number_of_groups, description = _count_groups(s3_filesystem=s3_filesystem, content_id=content_id)
        except Exception as exception:
            # These files were already opened successfully upstream, so a failure here is
            # almost always transient (network). Skip it and leave it for a later run to retry
            # rather than recording a wrong count.
            logger.warning("%s: skipping (%s: %s)", progress, type(exception).__name__, exception)
            continue
        valid_nwb_file_to_number_of_groups[content_id] = number_of_groups
        logger.info(
            "%s: %d groups (%s; %.1f s; peak memory %.0f MiB)",
            progress,
            number_of_groups,
            description,
            time.monotonic() - item_start_time,
            _peak_memory_mib(),
        )

    logger.info(
        "Processed %d content IDs in %.1f min (peak memory %.0f MiB).",
        number_to_process,
        (time.monotonic() - batch_start_time) / 60,
        _peak_memory_mib(),
    )
    _write_cache(file_path=cache_file_path, records=valid_nwb_file_to_number_of_groups)


if __name__ == "__main__":
    default_base_directory = pathlib.Path(__file__).parent.parent

    parser = argparse.ArgumentParser(description="Update the valid-nwb-file-to-number-of-groups DANDI cache.")
    parser.add_argument(
        "--base-directory",
        type=pathlib.Path,
        default=default_base_directory,
        help=(
            "The directory containing the `sourcedata` and `derivatives` directories. "
            "Set to the mounted dataset path when run inside the pipeline container; "
            "defaults to the repository root."
        ),
    )
    parser.add_argument(
        "--testing",
        action="store_true",
        help=(
            f"Run in testing mode: process only the first {_TESTING_LIMIT} items and write "
            f"`derivatives/{_TESTING_FILE_NAME}` instead of the real cache, leaving it "
            "untouched. Omit for a complete update."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap on the number of newly valid content IDs to process in this run.",
    )
    args = parser.parse_args()

    _run(base_directory=args.base_directory, testing=args.testing, limit=args.limit)
