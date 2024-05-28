# %%
from pathlib import Path
import pandas as pd
import geopandas as gpd  # type: ignore

from utils import load_config
import warnings
from tqdm import tqdm
import ast

tqdm.pandas()

warnings.simplefilter("ignore")

base_path = Path(load_config()["paths"]["base_path"])
base_path /= "processed_data"
casestudy_path = Path(load_config()["paths"]["casestudy_path"])

# %%
# attach Boroughs info to local roads
roads = gpd.read_file(casestudy_path / "inputs" / "roads_london.gpkg")
boroughs = gpd.read_file(
    casestudy_path
    / "inputs"
    / "statistical-gis-boundaries-london"
    / "ESRI"
    / "London_Borough_Excluding_MHW.shp"
)
intersection_gdf = gpd.overlay(roads, boroughs, how="intersection")
roads2 = intersection_gdf.groupby(by=["id"], as_index=False).agg({"NAME": list})
roads2["selected"] = roads2["NAME"].apply(
    lambda x: 1 if "Barnet" in x or "Brent" in x else 0
)
selected_flooded_roads_dict = roads2.set_index("id")["selected"].to_dict()

# %%
# calculate the remaining flow between each OD pair after flooding
# original OD matrix
# flooded roads
floodedRoads = pd.read_csv(casestudy_path / "outputs" / "flooded_roads.csv")
floodedRoads["selected"] = floodedRoads.id.map(selected_flooded_roads_dict)
exposure_1m = floodedRoads[
    (floodedRoads.level_of_flood == 4) & (floodedRoads.selected == 1)
]
exposure_1m.reset_index(drop=True, inplace=True)  # 78 selection
flooded_road_set = set(exposure_1m.id.tolist())

# %%
# (optional) original OD matrix
od_gb_2011 = pd.read_csv(
    base_path / "census_datasets" / "od_matrix" / "od_gb_2011_disaggregate_oa.csv"
)
od_gb_2011.rename(
    columns={
        "origins": "Area of usual residence",
        "destinations": "Area of workplace",
        "counts": "car",
    },
    inplace=True,
)
# case study: trips related to ppl living/working in London
oa_selected = gpd.read_file(casestudy_path / "inputs" / "oa_selected.csv")
oaList = oa_selected.OA21CD.unique().tolist()
od_df = od_gb_2011[
    (
        od_gb_2011["Area of usual residence"].isin(oaList)
        | (od_gb_2011["Area of workplace"].isin(oaList))
    )
]

od_df.reset_index(drop=True, inplace=True)


# %%
# convert str to tuple for column "path"
def str_to_tuple(s):
    try:
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        return s


odpf_df = pd.read_csv(casestudy_path / "outputs" / "base_odpf.csv")
odpf_df["path"] = odpf_df["path"].apply(str_to_tuple)
odpf_df["flooded"] = odpf_df.path.progress_apply(
    lambda x: 1 if any(p in flooded_road_set for p in x) else 0
)

# simulated OD pairs
od_df_simulation = odpf_df.groupby(by=["origin", "destination"], as_index=False).agg(
    {"flow": sum}
)
od_df_simulation.rename(
    columns={
        "origin": "Area of usual residence",
        "destination": "Area of workplace",
        "flow": "car",
    },
    inplace=True,
)

# generate remaining OD matrix
odpf_df2 = odpf_df[odpf_df.flooded == 1]
odpf_df2.reset_index(drop=True, inplace=True)
od_remain = odpf_df2.groupby(by=["origin", "destination"], as_index=False).agg(
    {"flow": sum}
)
od_remain.rename(
    columns={
        "origin": "Area of usual residence",
        "destination": "Area of workplace",
        "flow": "car",
    },
    inplace=True,
)

# generate allocated OD matrix
odpf_df3 = odpf_df[odpf_df.flooded == 0]
odpf_df3.reset_index(drop=True, inplace=True)
od_allocated = odpf_df3.groupby(by=["origin", "destination"], as_index=False).agg(
    {"flow": sum}
)
od_allocated.rename(
    columns={
        "origin": "Area of usual residence",
        "destination": "Area of workplace",
        "flow": "car",
    },
    inplace=True,
)


# %%
# update the road structure after flooding: road links
# original roads (458826)
# flooded roads (455919)
def map_flooded(row_id):
    return selected_flooded_roads_dict.get(row_id, 0)


road_links = gpd.read_parquet(casestudy_path / "inputs" / "road_link_file.geoparquet")
road_links["flooded"] = road_links["id"].map(map_flooded)
valid_road_links = road_links[road_links.flooded == 0]
valid_road_links.reset_index(drop=True, inplace=True)
