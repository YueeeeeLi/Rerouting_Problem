# %%
import geopandas as gpd
import snail.intersection
import snail.io

floodPath = r"C:\Oxford\Research\CONFERENCE\analysis\SW09Q75B_London.tif"
roadPath = r"C:\Oxford\Research\CONFERENCE\analysis\roads_london.gpkg"
outPath = r"C:\Oxford\Research\CONFERENCE\analysis\exporture.gpkg"
roads = gpd.read_file(roadPath, engine="pyogrio")
roads2 = roads[
    [
        "id",
        "from_id",
        "to_id",
        "combined_label",
        "acc_flow",
        "acc_capacity",
        "ave_flow_rate",
        "geometry",
        "length",
    ]
]
# %%
flood_data = snail.io.read_raster_band_data(floodPath)  # [259542, 120163]
# run the intersection analysis
grid, bands = snail.io.read_raster_metadata(floodPath)
prepared = snail.intersection.prepare_linestrings(roads2)
flood_intersections = snail.intersection.split_linestrings(prepared, grid)
flood_intersections = snail.intersection.apply_indices(flood_intersections, grid)
flood_intersections["level_of_flood"] = snail.intersection.get_raster_values_for_splits(
    flood_intersections, flood_data
)
# attach the maximum flood depth of each road link
roads_flooddepth = flood_intersections.groupby(by=["id"], as_index=False).agg(
    {"level_of_flood": "max"}
)

roads_flooddepth.to_csv(
    r"C:\Oxford\Research\git_prjs\outputs\flooded_roads.csv", index=False
)
