# Google buildings geoparquet example

Quick example of converting data from https://sites.research.google/open-buildings/#download to geoparquet.

<img width="215" alt="image" src="https://github.com/opengeospatial/geoparquet/assets/1312546/a634fc9c-7b2f-4ce9-a4a0-a3d4932e185f">


## Running

Download a few files into the `data/in` directory.

```python
python3 -m pip install -r requirements.txt
python3 main.py
```

## Issues

The large (6+ GB) csv.gz files are a problem. You'll need however much RAM
necessary to hold that uncompressed data in pandas.

To avoid that, you could decompress the `.csv.gz` and set `blocksize=...` in the call to `read_csv`,
where blocksize is some size in MB. You'd also need to adjust the regular expression to remove the `.gz`.

Also, dask is writing one file per s2_cell per partition. `s2_cell` should be unique within partitions, so we should have just one per partition. Maybe because it's a categorical dtype?