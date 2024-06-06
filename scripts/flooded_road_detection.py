# %%
import geopandas as gpd
import snail.intersection
import snail.io
from pathlib import Path
import geopandas as gpd  # type: ignore

from utils import load_config
import warnings

warnings.simplefilter("ignore")

base_path = Path(load_config()["paths"]["base_path"])
base_path /= "processed_data"
casestudy_path = Path(load_config()["paths"]["casestudy_path"])

# %%
floodPath = (
    casestudy_path
    / "inputs"
    / "incoming_data"
    / "JBA_flood_event"
    / "UK_RDS_LloydsRDS_Thames_Flood_FLRF_RD_5m_4326.tif"
)

outPath = casestudy_path / "outputs" / "JBA_exposure. gpkg"
roads = gpd.read_file(
    casestudy_path / "inputs" / "processed_data" / "selectef_flooded_roads.gpkg",
    layer="JBA",
    engine="pyogrio",
)
# project roads to 4326
roads_prj = roads.to_crs("epsg:4326")
# %%
flood_data = snail.io.read_raster_band_data(floodPath)  # [259542, 120163]
# run the intersection analysis
grid, bands = snail.io.read_raster_metadata(floodPath)
prepared = snail.intersection.prepare_linestrings(roads_prj)
flood_intersections = snail.intersection.split_linestrings(prepared, grid)
flood_intersections = snail.intersection.apply_indices(flood_intersections, grid)

# %%
flood_intersections["level_of_flood"] = snail.intersection.get_raster_values_for_splits(
    flood_intersections, flood_data
)
# attach the maximum flood depth of each road link
roads_flooddepth = flood_intersections.groupby(by=["id"], as_index=False).agg(
    {"level_of_flood": "max"}
)

roads_flooddepth.to_csv(
    casestudy_path / "outputs" / "JBA_flooded_roads.csv", index=False
)
