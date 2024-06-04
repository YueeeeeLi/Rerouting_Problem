# %%
from pathlib import Path
import pandas as pd
import geopandas as gpd  # type: ignore

from utils import load_config
import functions as func

import json
import warnings

warnings.simplefilter("ignore")

base_path = Path(load_config()["paths"]["base_path"])
base_path /= "processed_data"
casestudy_path = Path(load_config()["paths"]["casestudy_path"])

# %%
"""
list of inputs:
 - Parameter dicts.
 - OS open roads.
 - ETISPLUS_urban_roads: to create urban mask.
 - Population-weighed centroids of admin units.
 - O-D matrix (*travel to work by car).
"""

# model parameters
with open(base_path / "parameters" / "flow_breakpoint_dict.json", "r") as f:
    flow_breakpoint_dict = json.load(f)

with open(base_path / "parameters" / "flow_cap_dict.json", "r") as f:
    flow_capacity_dict = json.load(f)

with open(base_path / "parameters" / "free_flow_speed_dict.json", "r") as f:
    free_flow_speed_dict = json.load(f)

with open(base_path / "parameters" / "min_speed_cap.json", "r") as f:
    min_speed_cap = json.load(f)

with open(base_path / "parameters" / "urban_speed_cap.json", "r") as f:
    urban_speed_cap = json.load(f)

with open(base_path / "parameters" / "flooded_road_links.json", "r") as f:
    flooded_road_list = json.load(f)

# %%
# OS open roads
osoprd_link = gpd.read_parquet(
    base_path / "networks" / "road" / "osoprd_road_links.geoparquet"
)
osoprd_node = gpd.read_parquet(
    base_path / "networks" / "road" / "osoprd_road_nodes.geoparquet"
)

# ETISPLUS roads
etisplus_road_links = gpd.read_parquet(
    base_path / "networks" / "road" / "etisplus_road_links.geoparquet"
)
etisplus_urban_roads = etisplus_road_links[["Urban", "geometry"]]
etisplus_urban_roads = etisplus_urban_roads[etisplus_urban_roads["Urban"] == 1]

# population-weighted centroids (combined spatial units)
zone_centroids = gpd.read_parquet(
    base_path / "census_datasets" / "admin_pwc" / "zone_pwc.geoparquet"
)

# %%
# select major roads
road_link_file, road_node_file = func.select_partial_roads(
    road_links=osoprd_link,
    road_nodes=osoprd_node,
    col_name="road_classification",
    list_of_values=["A Road", "B Road", "Motorway"],
)

# classify the selected major road links into urban/suburban
urban_mask = func.create_urban_mask(etisplus_urban_roads)
road_link_file = func.label_urban_roads(road_link_file, urban_mask)

# drop disrupted roads if necessary (optional)
user_input = input("Please choose a scenario (base/flooded): ")
if user_input != "base" and user_input != "flooded":
    print("Error: please check the scenario input!")
if user_input == "flooded":
    road_link_file = road_link_file[~road_link_file.e_id.isin(flooded_road_list)]
    road_link_file.reset_index(drop=True, inplace=True)

# attach toll charges to the selected major roads
tolls = pd.read_csv(base_path / "networks" / "road" / "tolls.csv")
road_link_file["average_toll_cost"] = 0
tolls_mapping = (
    tolls.iloc[1:, :].set_index("e_id")["Average_cost (£/passage)"].to_dict()
)
road_link_file["average_toll_cost"] = road_link_file["e_id"].apply(
    lambda x: tolls_mapping.get(x, 0)
)
road_link_file.loc[
    road_link_file.road_classification_number == "M6", "average_toll_cost"
] = 8.0  # £/car

# %%
# O-D matrix (2011)
od_df = pd.read_csv(base_path / "census_datasets" / "od_matrix" / "od_gb_2011.csv")
print(f"total flows: {od_df.car.sum()}")  # 14_203_635 trips/day

# %%
#!!!  O-D matrix (selected-OA, 2011)
od_gb_oa = pd.read_csv(
    base_path / "census_datasets" / "od_matrix" / "od_gb_2011_disaggregate_oa.csv"
)
od_gb_oa.rename(
    columns={
        "origins": "Area of usual residence",
        "destinations": "Area of workplace",
        "counts": "car",
    },
    inplace=True,
)

#!!! for case study
oa_selected = pd.read_csv(casestudy_path / "inputs" / "oa_selected.csv")
oa_set = set(oa_selected.OA21CD.unique())
od_df = od_gb_oa[
    od_gb_oa["Area of usual residence"].isin(oa_set)
    | od_gb_oa["Area of workplace"].isin(oa_set)
]
od_df.reset_index(drop=True, inplace=True)

#!!! for debugging only
od_df = od_df.head(100000)

# %%
# find the nearest road node for each zone
zone_to_node = func.find_nearest_node(zone_centroids, road_node_file)
# attach od info of each zone to their nearest road network nodes
list_of_origin_nodes, dict_of_destination_nodes, dict_of_origin_supplies = (
    func.od_interpret(
        od_df,
        zone_to_node,
        col_origin="Area of usual residence",
        col_destination="Area of workplace",
        col_count="car",
    )
)
# extract identical origin nodes
list_of_origin_nodes = list(set(list_of_origin_nodes))
list_of_origin_nodes.sort()

# %%
# network creation (igragh)
node_name_to_index = {name: index for index, name in enumerate(road_node_file.nd_id)}
node_index_to_name = {value: key for key, value in node_name_to_index.items()}
test_net_ig, edge_cost_dict, edge_timeC_dict, edge_operateC_dict = (
    func.create_igraph_network(
        node_name_to_index, road_link_file, road_node_file, free_flow_speed_dict
    )
)  # this returns a network and edge weights dict(edge_name, edge_weight)
edge_index_to_name = {idx: name for idx, name in enumerate(test_net_ig.es["edge_name"])}

# network initialisation
road_link_file = func.initialise_igraph_network(
    road_link_file,
    flow_capacity_dict,
    free_flow_speed_dict,
    col_road_classification="road_classification",
)

# %%
# flow simulation
speed_dict, acc_flow_dict, acc_capacity_dict = func.network_flow_model(
    test_net_ig,  # network
    edge_cost_dict,  #!!!
    edge_timeC_dict,  # !!!
    edge_operateC_dict,  # !!!
    road_link_file,  # road
    node_name_to_index,  # road
    edge_index_to_name,  # road
    list_of_origin_nodes,  # od
    dict_of_origin_supplies,  # od
    dict_of_destination_nodes,  # od
    free_flow_speed_dict,  # speed
    flow_breakpoint_dict,  # speed
    min_speed_cap,  # speed
    urban_speed_cap,  # speed
    col_eid="e_id",
)

# %%
# append estimation of: speeds, flows, and remaining capacities
road_link_file.ave_flow_rate = road_link_file.e_id.map(speed_dict)
road_link_file.acc_flow = road_link_file.e_id.map(acc_flow_dict)
road_link_file.acc_capacity = road_link_file.e_id.map(acc_capacity_dict)

# change field types
road_link_file.acc_flow = road_link_file.acc_flow.astype(int)
road_link_file.acc_capacity = road_link_file.acc_capacity.astype(int)

# %%
# export files
road_link_file.to_file(
    casestudy_path / "outputs" / "base_edge_flows_thames.gpkg",
    driver="GPKG",
    engine="pyogrio",
)
