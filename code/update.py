import argparse
import itertools
import json
import pathlib
import resource
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


def _peak_memory_mib() -> float:
    """Peak resident set size of this process so far, in MiB (Linux reports `ru_maxrss` in KiB)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def _count_hdf5_subgroups(group: h5py.Group, visited_addresses: set[int]) -> int:
    """Count the groups reachable from `group` through hard links, `group` itself excluded.

    This counts exactly what `h5py.Group.visititems` would (hard links only, and an object
    that is hard-linked from more than one place counts once) without its cost: `visititems`
    runs `H5Ovisit`, which retrieves the full object info of every object it passes, and for a
    chunked dataset that info includes the on-disk size of its chunk index, so HDF5 walks the
    dataset's entire chunk B-tree. Streamed over HTTP that is one round trip per B-tree node,
    and a single dataset with a few hundred thousand chunks in a multi-GB file took over an
    hour. Here only groups are opened and descended into; a dataset costs one object-header
    read to learn that it is not a group.
    """
    number_of_groups = 0
    for name in group:
        # `visititems` follows hard links only; soft and external links are skipped.
        if not isinstance(group.get(name, getlink=True), h5py.HardLink):
            continue
        child = group[name]
        if not isinstance(child, h5py.Group):
            continue
        child_info = h5py.h5o.get_info(child.id)
        # An object hard-linked from more than one place (`rc` is its hard-link count) is
        # counted once, as `H5Ovisit` does; this also terminates hard-link cycles.
        if child_info.rc > 1:
            if child_info.addr in visited_addresses:
                continue
            visited_addresses.add(child_info.addr)
        number_of_groups += 1 + _count_hdf5_subgroups(group=child, visited_addresses=visited_addresses)
    return number_of_groups


def _count_hdf5_groups(content_id: str) -> tuple[int, str]:
    """Stream an HDF5 asset and count its groups, the root group included.

    Returns the count and a short description of the asset (its layout and size) for the log.
    """
    blob_url = _BLOB_URL_TEMPLATE.format(prefix=content_id[:3], infix=content_id[3:6], content_id=content_id)
    rem_file = remfile.File(url=blob_url)
    with h5py.File(name=rem_file, mode="r") as h5py_file:
        # The root `/` is itself a group. Seed the visited set with it so that a hard link
        # back to the root from below is not counted again.
        visited_addresses = {h5py.h5o.get_info(h5py_file.id).addr}
        number_of_groups = 1 + _count_hdf5_subgroups(group=h5py_file, visited_addresses=visited_addresses)
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

    # Every line below is flushed: stdout is a pipe inside the pipeline container, so without
    # it Python block-buffers and a run that is killed mid-batch leaves no trace of how far it
    # got, which file it was walking, or how much memory it had reached.
    number_to_process = len(content_ids_to_process)
    print(f"Processing {number_to_process} newly valid content IDs.", flush=True)
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
            print(f"{progress}: skipping ({type(exception).__name__}: {exception})", flush=True)
            continue
        valid_nwb_file_to_number_of_groups[content_id] = number_of_groups
        print(
            f"{progress}: {number_of_groups} groups ({description}; "
            f"{time.monotonic() - item_start_time:.1f} s; peak memory {_peak_memory_mib():.0f} MiB)",
            flush=True,
        )

    print(
        f"Processed {number_to_process} content IDs in {(time.monotonic() - batch_start_time) / 60:.1f} min "
        f"(peak memory {_peak_memory_mib():.0f} MiB).",
        flush=True,
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
