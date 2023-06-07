import pathlib
import re
import geopandas
import pandas as pd
import dask_geopandas
import dask.dataframe as dd


xpr = re.compile(r".*data/in/(?P<s2_cell>\w{3})_buildings.csv.gz")


def main():
    s2_cells = [
        xpr.findall(str(p))[0]
        for p in pathlib.Path("data/in").glob("*.csv.gz")
    ]
    s2_cells = pd.CategoricalDtype(s2_cells, ordered=False)

    # The files being gzip-compressed is a problem.
    # Some are ~6 GB *compressed*, so much larger once uncompressed.
    # We might need to decompress them ahead of time :/
  
    df = (
        dd.read_csv("data/in/*.csv.gz", include_path_column=True)
        .assign(
            s2_cell=lambda df: df.path.str.extract(xpr)["s2_cell"].astype(s2_cells)
        )
        .drop(columns=["path"])
    )
    geometry = df.geometry.map_partitions(
        geopandas.GeoSeries.from_wkt, meta=geopandas.GeoSeries([])
    )
    gdf = dask_geopandas.from_dask_dataframe(df, geometry=geometry).set_crs("EPSG:4326")
    gdf.to_parquet("data/out.parquet", partition_on=["s2_cell"])


if __name__ == "__main__":
    main()