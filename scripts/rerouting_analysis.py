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

# %%
# remaining OD matrix
od_df = pd.read_csv(casestudy_path / "outputs" / "od_remain.csv")
# remaining roads network
road_link_file = gpd.read_parquet(
    casestudy_path / "outputs" / "road_links_remain.geoparquet"
)
road_node_file = gpd.read_parquet(
    casestudy_path / "outputs" / "road_node_file.geoparquet"
)

# %%
# find the nearest road node for each zone
# attach od info of each zone to their nearest road network nodes
temp_od_df = od_df.groupby(by=["Area of usual residence"]).agg(
    {"Area of workplace": list, "car": list}
)
list_of_origin_nodes = temp_od_df.index.tolist()
list_of_origin_nodes.sort()
dict_of_destination_nodes = temp_od_df.set_index(temp_od_df.index)[
    "Area of workplace"
].to_dict()
dict_of_origin_supplies = temp_od_df.set_index(temp_od_df.index)["car"].to_dict()

# %%
# network creation (igragh)
node_name_to_index = {name: index for index, name in enumerate(road_node_file.nd_id)}
node_index_to_name = {value: key for key, value in node_name_to_index.items()}
# !!! edge_voc_dict
test_net_ig, edge_cost_dict = func.create_igraph_network(
    node_name_to_index, road_link_file, road_node_file, free_flow_speed_dict
)
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
speed_dict, acc_flow_dict, acc_capacity_dict, odpf_df = func.network_flow_model(
    test_net_ig,  # network
    edge_cost_dict,  # !!! edge_voc_dict
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
    casestudy_path / "outputs" / "reroute_remaining_flow.gpkg",
    driver="GPKG",
)
