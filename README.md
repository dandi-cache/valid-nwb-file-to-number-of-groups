# DANDI Cache: `valid-nwb-file-to-number-of-groups`

A mapping from the content ID of every valid NWB file on the DANDI archive to the total number of groups inside that file.

The set of valid NWB files is taken from the [`content-id-to-valid-nwb-file`](https://github.com/dandi-cache/content-id-to-valid-nwb-file) cache, restricted to the entries it marked `true`.
Each such file is streamed directly from the public DANDI S3 bucket and read with [h5py](https://www.h5py.org/) (HDF5 assets) or [zarr](https://zarr.readthedocs.io/) (Zarr assets), and its groups are counted.
The count includes the root group, so it is the total number of groups in the file's hierarchy.

Each line of the derivatives is a JSON object of the form:

```json
{"<content_id>": <number_of_groups>}
```

Updated frequently.

Primarily for use by developers.



## One-time use

If you only plan to use this cache infrequently or from disparate locations, you can directly download the latest version of the cache from the `dist` branch.
`dist` publishes each output as one compressed [JSON Lines](https://jsonlines.org/) file, so the cache itself is `derivatives/valid_nwb_file_to_number_of_groups.jsonl.gz`.
Each key appears once in it, on its own line.

### Python API (recommended)

```python
import gzip
import json
import urllib.request

url = "https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-number-of-groups/refs/heads/dist/derivatives/valid_nwb_file_to_number_of_groups.jsonl.gz"
with urllib.request.urlopen(url) as response:
    lines = gzip.decompress(data=response.read()).decode("utf-8").splitlines()
valid_nwb_file_to_number_of_groups = {key: value for line in lines if line for key, value in json.loads(line).items()}
```

### Save to file

```bash
curl https://raw.githubusercontent.com/dandi-cache/valid-nwb-file-to-number-of-groups/refs/heads/dist/derivatives/valid_nwb_file_to_number_of_groups.jsonl.gz -o valid_nwb_file_to_number_of_groups.jsonl.gz
```



## Repeated use

If you plan on using this cache regularly, clone the `derivatives` branch of this repository:

```bash
git clone --branch derivatives https://github.com/dandi-cache/valid-nwb-file-to-number-of-groups.git
```

Or, if you prefer [DataLad](https://www.datalad.org/):

```bash
datalad clone https://github.com/dandi-cache/valid-nwb-file-to-number-of-groups.git --branch derivatives
```

The `derivatives` branch also keeps the log of every update under `logs/`, next to the results it produced.

Then set up a CRON on your system to pull the latest version of the cache at your desired frequency.

For example, through `crontab -e`, add:

```bash
0 0 * * * git -C /path/to/valid-nwb-file-to-number-of-groups pull
```

This will minimize data overhead by only loading the most recent changes.

Each output is a store on the `derivatives` branch, where it is the whole log, uncompacted, and plain text:

```python
import json, pathlib

chunks = sorted(pathlib.Path("valid-nwb-file-to-number-of-groups/derivatives/valid_nwb_file_to_number_of_groups.zarr/stream/c").iterdir(), key=lambda path: int(path.name))
valid_nwb_file_to_number_of_groups = {key: value for chunk in chunks for line in chunk.read_text().splitlines() if line for key, value in json.loads(line).items()}
```
