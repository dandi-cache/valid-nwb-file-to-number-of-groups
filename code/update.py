"""Count the internal groups of every valid NWB file (HDF5 or Zarr).

The archive is content-addressed, so nothing here consults the DANDI API: the blob key is probed
and the asset is read as Zarr when no such blob exists. Nothing is downloaded either, in either
layout.

Everything shared with the other caches -- the argument parsing, the logging, the batch cap, the
error logs, the output paths, testing mode, and the S3 layout probe with the HDF5 and Zarr walks
-- comes from `dandi_cache_utils`, which the runtime image carries.
"""

import dandi_cache_utils as dandi_cache


def count_groups(content_id, item) -> int:
    """Count the groups in one asset, root group included, resolved straight from its content ID."""
    item.stage = "reading the NWB file"
    return dandi_cache.nwb.walk_structure(content_id).number_of_groups


def main() -> None:
    dataset, arguments = dandi_cache.open_dataset()

    # Only the assets the upstream cache marked valid are counted.
    validity = dataset.read_input()
    valid_content_ids = [content_id for content_id, is_valid in validity.items() if is_valid is True]

    dandi_cache.run_incremental_update(
        dataset,
        candidates=valid_content_ids,
        process=count_groups,
        limit=dandi_cache.effective_limit(testing=dataset.testing, limit=arguments.limit),
        # These files were already opened successfully upstream, so a failure here is almost always
        # transient. Leave the item for a later run rather than recording a wrong count.
        on_failure=dandi_cache.SKIP,
        stages={"reading the NWB file": "file_read_errors.txt"},
        describe=lambda number_of_groups: f"{number_of_groups} groups",
        checkpoint_every=50,
    )


if __name__ == "__main__":
    main()
