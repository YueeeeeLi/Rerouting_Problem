# %%
from typing import Union, Tuple
from collections import defaultdict
from functools import partial
import numpy as np
import pandas as pd
import geopandas as gpd  # type: ignore

import igraph  # type: ignore

import constants as cons
from utils import get_flow_on_edges

from tqdm.auto import tqdm
import warnings

warnings.simplefilter("ignore")


# %%
# functions
# extract major roads
def select_partial_roads(
    road_links: gpd.GeoDataFrame,
    road_nodes: gpd.GeoDataFrame,
    col_name: str,
    list_of_values: list,
) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:

    selected_links = []
    # road links selection
    for ci in list_of_values:
        selected_links.append(road_links[road_links[col_name] == ci])

    selected_links = pd.concat(selected_links, ignore_index=True)
    selected_links = gpd.GeoDataFrame(selected_links, geometry="geometry")

    selected_links["e_id"] = selected_links.id
    selected_links["from_id"] = selected_links.start_node
    selected_links["to_id"] = selected_links.end_node

    # road nodes selection
    sel_node_idx = list(
        set(selected_links.start_node.tolist() + selected_links.end_node.tolist())
    )

    selected_nodes = road_nodes[road_nodes.id.isin(sel_node_idx)]
    selected_nodes.reset_index(drop=True, inplace=True)
    selected_nodes["nd_id"] = selected_nodes.id
    selected_nodes["lat"] = selected_nodes.geometry.y
    selected_nodes["lon"] = selected_nodes.geometry.x

    return selected_links, selected_nodes


# urban road classification
def create_urban_mask(etisplus_urban_roads: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    etisplus_urban_roads = etisplus_urban_roads[
        etisplus_urban_roads["Urban"] == 1
    ].reset_index(drop=True)
    buf_geom = etisplus_urban_roads.geometry.buffer(
        500
    )  # create a buffer of 500 meters
    uni_geom = buf_geom.unary_union  # feature union within the same layer
    temp = gpd.GeoDataFrame(geometry=[uni_geom])
    new_geom = (
        temp.explode()
    )  # explode multi-polygons into separate individual polygons
    cvx_geom = (
        new_geom.convex_hull
    )  # generate convex polygon for each individual polygon

    urban_mask = gpd.GeoDataFrame(
        geometry=cvx_geom[0], crs=etisplus_urban_roads.crs
    ).to_crs("27700")
    return urban_mask


def label_urban_roads(
    road_links: gpd.GeoDataFrame, urban_mask: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    temp_file = road_links.sjoin(urban_mask, how="left")
    temp_file["urban"] = temp_file["index_right"].apply(
        lambda x: 0 if pd.isna(x) else 1
    )
    max_values = temp_file.groupby("e_id")["urban"].max()
    road_links = road_links.merge(max_values, on="e_id", how="left")
    return road_links


def voc_func(speed: float) -> float:  # speed: mile/hour
    # d = distance * conv_mile_to_km  # km
    s = speed * cons.CONV_MILE_TO_KM  # km/hour
    lpkm = 0.178 - 0.00299 * s + 0.0000205 * (s**2)  # fuel cost (liter/km)
    voc = 140 * lpkm * cons.PENCE_TO_POUND  # average petrol cost: 140 pence/liter
    return voc  # pound/km


#!!! revise this cost function: add toll costs (£)
def cost_func(
    time: float,
    distance: float,
    voc: float,
    toll: float,
) -> float:  # time: hour, distance: mile/hour, voc: pound/km
    ave_occ = 1.6
    vot = 20  # value of time: pounds/hour
    d = distance * cons.CONV_MILE_TO_KM  # km
    cost = time * ave_occ * vot + d * voc + toll  # pound
    return cost  # total cost: pounds


# speed functions
def initial_speed_func(
    road_type: str, form_of_road: str, free_flow_speed_dict: dict
) -> Union[float, None]:
    if road_type == "M":
        return free_flow_speed_dict["M"]
    elif road_type == "A":
        if form_of_road == "Single Carriageway":
            return free_flow_speed_dict["A_single"]
        else:
            return free_flow_speed_dict["A_dual"]
    elif road_type == "B":
        return free_flow_speed_dict["B"]
    else:
        print("Error: initial speed!")
        return None


def speed_flow_func(
    road_type: str,
    isurban: int,
    vp: float,
    free_flow_speed_dict: dict,
    flow_breakpoint_dict: dict,
    min_speed_cap: dict,
    urban_speed_cap: dict,
) -> Union[float, None]:
    vp = vp / 24
    if road_type == "M":
        initial_speed = free_flow_speed_dict["M"]
        if vp > flow_breakpoint_dict["M"]:  # speed starts to decrease
            vt = max(
                (initial_speed - 0.033 * (vp - flow_breakpoint_dict["M"])),
                min_speed_cap["M"],
            )
            if isurban:
                return min(urban_speed_cap["M"], vt)
            else:
                return vt
        else:
            if isurban:
                return min(urban_speed_cap["M"], initial_speed)
            else:
                return initial_speed
    elif road_type == "A_single":  # A_single/A_dual
        initial_speed = free_flow_speed_dict["A_single"]
        if vp > flow_breakpoint_dict["A_single"]:
            vt = max(
                (initial_speed - 0.05 * (vp - flow_breakpoint_dict["A_single"])),
                min_speed_cap["A_single"],
            )
            if isurban:
                return min(urban_speed_cap["A_single"], vt)
            else:
                return vt
        else:
            if isurban:
                return min(urban_speed_cap["A_single"], initial_speed)
            else:
                return initial_speed
    elif road_type == "A_dual":
        initial_speed = free_flow_speed_dict["A_dual"]
        if vp > flow_breakpoint_dict["A_dual"]:
            vt = max(
                (initial_speed - 0.033 * (vp - flow_breakpoint_dict["A_dual"])),
                min_speed_cap["A_dual"],
            )
            if isurban:
                return min(urban_speed_cap["A_dual"], vt)
            else:
                return vt
        else:
            if isurban:
                return min(urban_speed_cap["A_dual"], initial_speed)
            else:
                return initial_speed
    elif road_type == "B":
        initial_speed = free_flow_speed_dict["B"]
        if isurban:
            return min(urban_speed_cap["B"], initial_speed)
        else:
            return initial_speed
    else:
        print("Please select the road type from [M, A, B]!")
        return None


def filter_less_than_one(arr: np.ndarray) -> np.ndarray:
    return np.where(arr >= 1, arr, 0)


# find nearest network node for each admin centroid
def find_nearest_node(zones: gpd.GeoDataFrame, road_nodes: gpd.GeoDataFrame) -> dict:
    nearest_node_dict = {}  # node_idx: zone_idx
    for zidx, z in zones.iterrows():
        closest_road_node = road_nodes.sindex.nearest(z.geometry, return_all=False)[1][
            0
        ]
        # for the first [x]:
        #   [0] represents the index of geometry;
        #   [1] represents the index of gdf
        # the second [x] represents the No. of closest item in the returned list,
        #   which only return one nearest node in this case
        nearest_node_dict[zidx] = closest_road_node

    zone_to_node = {}
    for zidx in range(zones.shape[0]):
        z = zones.loc[zidx, "code"]
        nidx = nearest_node_dict[zidx]
        n = road_nodes.loc[nidx, "nd_id"]
        zone_to_node[z] = n

    return zone_to_node


# interpret od matrix
# {list of origins,
# list of destinations attached to each origin,
# list of supplies from each origin}
def od_interpret(
    od_matrix: pd.DataFrame,
    zone_to_node: dict,
    col_origin: str,
    col_destination: str,
    col_count: str,
) -> Tuple[list, dict, dict]:

    list_of_origins = []
    destination_dict: dict[str, list[str]] = defaultdict(list)
    supply_dict: dict[str, list[float]] = defaultdict(list)

    for idx in tqdm(range(od_matrix.shape[0]), desc="Processing"):
        from_zone = od_matrix.loc[idx, col_origin]
        to_zone = od_matrix.loc[idx, col_destination]
        count: float = od_matrix.loc[idx, col_count]  # type: ignore
        try:
            from_node = zone_to_node[from_zone]
        except KeyError:
            print(f"No accessible network node to attached to home/origin {from_zone}!")
        try:
            to_node = zone_to_node[to_zone]
        except KeyError:
            print(
                f"No accessible network node attached to workplace/destination {to_zone}!"
            )

        list_of_origins.append(from_node)  # origin
        destination_dict[from_node].append(to_node)  # origin -> destinations
        supply_dict[from_node].append(count)  # origin -> supply

    return list_of_origins, destination_dict, supply_dict


# network creation
def create_igraph_network(
    name_to_index: dict,
    road_links: gpd.GeoDataFrame,
    road_nodes: gpd.GeoDataFrame,
    initialSpeeds: dict,
) -> Tuple[igraph.Graph, dict]:
    nodeList = [
        (
            name_to_index[node.id],
            {
                "lon": node.geometry.x,
                "lat": node.geometry.y,
            },
        )
        for _, node in road_nodes.iterrows()
    ]  # (id, {x, y})

    edgeNameList = []
    edgeList = []
    edgeLengthList = []
    edgeTypeList = []
    edgeFormList = []
    edgeTollList = []
    for _, link in road_links.iterrows():
        edge_name = link.e_id
        edge_from = link.from_id
        edge_to = link.to_id
        edge_length = link.geometry.length * cons.CONV_METER_TO_MILE  # miles
        edge_type = link.road_classification[0]
        edge_form = link.form_of_way
        edge_toll = link.average_toll_cost
        edgeNameList.append(edge_name)
        edgeList.append((name_to_index[edge_from], name_to_index[edge_to]))
        edgeLengthList.append(edge_length)
        edgeTypeList.append(edge_type)
        edgeFormList.append(edge_form)
        edgeTollList.append(edge_toll)

    edgeSpeedList = np.vectorize(initial_speed_func)(
        edgeTypeList,
        edgeFormList,
        initialSpeeds,
    )  # miles/hour

    # travel time
    timeList = np.array(edgeLengthList) / np.array(edgeSpeedList)  # hour

    # total travel cost (time-equivalent)
    vocList = np.vectorize(voc_func)(edgeSpeedList)  # £/km
    costList = np.vectorize(cost_func)(
        timeList, edgeLengthList, vocList, edgeTollList
    )  # hour
    # weightList = (costList * 3600).tolist()  # seconds
    weightList = costList.tolist()  # pounds

    test_net = igraph.Graph(directed=False)
    test_net.add_vertices(nodeList)
    test_net.vs["nd_id"] = road_nodes.id.tolist()
    test_net.add_edges(edgeList)
    test_net.es["edge_name"] = edgeNameList
    test_net.es["weight"] = weightList

    # estimate edge operating costs (£)
    edge_cost_dict = dict(zip(edgeNameList, weightList))

    return test_net, edge_cost_dict


# network initialization
def initialise_igraph_network(
    road_links: gpd.GeoDataFrame,
    initial_capacity_dict: dict,
    initial_speed_dict: dict,
    col_road_classification=str,
) -> gpd.GeoDataFrame:
    # road_types: M, A, B
    road_links["road_type_label"] = road_links[col_road_classification].str[0]
    # road_forms: M, A_dual, A_single, B
    road_links["combined_label"] = road_links["road_type_label"]
    road_links.loc[road_links.road_type_label == "A", "combined_label"] = "A_dual"
    road_links.loc[
        (
            (road_links.road_type_label == "A")
            & (road_links.form_of_way.str.contains("Single"))
        ),
        "combined_label",
    ] = "A_single"  # only single carriageways of A roads
    # accumulated edge flows (cars/day)
    road_links["acc_flow"] = 0.0
    # remaining edge capacities (cars/day)
    road_links["acc_capacity"] = road_links["combined_label"].map(initial_capacity_dict)
    # average edge flow rates (miles/hour)
    road_links["ave_flow_rate"] = road_links["combined_label"].map(initial_speed_dict)

    return road_links


def update_od_matrix(
    temp_flow_matrix: pd.DataFrame,
    supply_dict: dict,
    destination_dict: dict,
) -> Tuple[list, dict, dict, float]:
    # drop the origin-destination pairs ("path = []")
    temp_df = temp_flow_matrix[temp_flow_matrix["path"].apply(lambda x: len(x) == 0)]
    non_allocated_flow = temp_df.flow.sum()
    print(f"Non_allocated_flow: {non_allocated_flow}")

    for _, row in temp_df.iterrows():
        origin_temp = row["origin"]
        destination_temp = row["destination"]
        idx_temp = destination_dict[origin_temp].index(destination_temp)
        destination_dict[origin_temp].remove(destination_temp)
        del supply_dict[origin_temp][idx_temp]

    # drop origins with zero supply
    new_supply_dict = {}
    new_destination_dict = {}
    new_list_of_origins = []
    for origin, list_of_counts in supply_dict.items():
        tt_supply = sum(list_of_counts)
        if tt_supply > 0:
            new_list_of_origins.append(origin)
            new_counts = [od_flow for od_flow in list_of_counts if od_flow != 0]
            new_supply_dict[origin] = new_counts
            new_destination_dict[origin] = [
                dest
                for idx, dest in enumerate(destination_dict[origin])
                if list_of_counts[idx] != 0
            ]

    return (
        new_list_of_origins,
        new_supply_dict,
        new_destination_dict,
        non_allocated_flow,
    )


def update_network_structure(
    network: igraph.Graph,
    length_dict: dict,
    speed_dict: dict,
    toll_dict: dict,
    temp_edge_flow: pd.DataFrame,
) -> Tuple[igraph.Graph, dict, dict]:
    zero_capacity_edges = set(
        temp_edge_flow.loc[temp_edge_flow["remaining_capacity"] < 1, "e_id"].tolist()
    )
    net_edges = network.es["edge_name"]
    idx_to_remove = [
        idx for idx, element in enumerate(net_edges) if element in zero_capacity_edges
    ]

    # drop links that have reached their full capacities
    network.delete_edges(idx_to_remove)
    number_of_edges = len(list(network.es))
    print(f"The remaining number of edges in the network: {number_of_edges}")

    # update edge weights (time: seconds)
    remaining_edges = network.es["edge_name"]
    lengthList = list(
        map(length_dict.get, filter(length_dict.__contains__, remaining_edges))
    )
    speedList = list(
        map(speed_dict.get, filter(speed_dict.__contains__, remaining_edges))
    )
    timeList = np.where(
        np.array(speedList) != 0, np.array(lengthList) / np.array(speedList), np.nan
    )  # hours (omg: here has some problems!!!)
    tollList = list(map(toll_dict.get, filter(toll_dict.__contains__, remaining_edges)))

    if np.isnan(timeList).any():
        print("ERROR: Network contains congested edges.")
        exit()
    else:
        vocList = np.vectorize(voc_func)(speedList)
        costList = np.vectorize(cost_func)(
            timeList, lengthList, vocList, tollList
        )  # hours
        weightList = costList.tolist()  # pounds
        network.es["weight"] = weightList

        # update idx_to_name dict
        edge_index_to_name = {
            idx: name for idx, name in enumerate(network.es["edge_name"])
        }
        # edge operating costs (£)
        edge_cost_dict = dict(zip(network.es["edge_name"], weightList))
    return network, edge_index_to_name, edge_cost_dict


def map_tuple(tup: Tuple, mapping: dict) -> Tuple:
    return tuple(mapping.get(item, item) for item in tup)


def cauculate_total_weight(list_of_paths: list, network: igraph.Graph) -> float:
    total_weight = 0
    for path in list_of_paths:
        edge = network.es.find(edge_name=path)
        total_weight += edge["weight"]
    return total_weight


def network_flow_model(
    network: igraph.Graph,
    edge_cost_dict: dict,
    road_links: gpd.GeoDataFrame,
    node_name_to_index: dict,
    edge_index_to_name: dict,
    list_of_origins: list,
    supply_dict: dict,
    destination_dict: dict,
    free_flow_speed_dict: dict,
    flow_breakpoint_dict: dict,
    min_speed_cap: dict,
    urban_speed_cap: dict,
    col_eid: str,
) -> Tuple[dict, dict, dict]:

    # record total cost of travelling: weight * flow
    total_cost = 0
    partial_speed_flow_func = partial(
        speed_flow_func,
        free_flow_speed_dict=free_flow_speed_dict,
        flow_breakpoint_dict=flow_breakpoint_dict,
        min_speed_cap=min_speed_cap,
        urban_speed_cap=urban_speed_cap,
    )

    total_remain = sum(sum(values) for values in supply_dict.values())
    print(f"The initial total supply is {total_remain}")
    number_of_edges = len(list(network.es))
    print(f"The initial number of edges in the network: {number_of_edges}")
    print(f"The initial number of origins: {len(list_of_origins)}")
    number_of_destinations = sum(len(value) for value in destination_dict.values())
    print(f"The initial number of destinations: {number_of_destinations}")

    # road link properties
    edge_cbtype_dict = road_links.set_index(col_eid)["combined_label"].to_dict()
    edge_isUrban_dict = road_links.set_index(col_eid)["urban"].to_dict()
    edge_length_dict = (
        road_links.set_index(col_eid)["geometry"].length * cons.CONV_METER_TO_MILE
    ).to_dict()
    edge_toll_dict = road_links.set_index(col_eid)[
        "average_toll_cost"
    ].to_dict()  # tolls

    acc_flow_dict = road_links.set_index(col_eid)["acc_flow"].to_dict()
    acc_capacity_dict = road_links.set_index(col_eid)["acc_capacity"].to_dict()
    acc_speed_dict = road_links.set_index(col_eid)["ave_flow_rate"].to_dict()

    # starts
    iter_flag = 1
    total_non_allocated_flow = 0
    while total_remain > 0:
        print(f"No.{iter_flag} iteration starts:")
        list_of_spath = []
        # find the shortest path for each origin-destination pair
        for i in tqdm(range(len(list_of_origins)), desc="Processing"):
            name_of_origin_node = list_of_origins[i]
            idx_of_origin_node = node_name_to_index[name_of_origin_node]
            list_of_name_destination_node = destination_dict[
                name_of_origin_node
            ]  # a list of destination nodes
            list_of_idx_destination_node = [
                node_name_to_index[i] for i in list_of_name_destination_node
            ]
            flows = supply_dict[name_of_origin_node]
            paths = network.get_shortest_paths(
                v=idx_of_origin_node,
                to=list_of_idx_destination_node,  #!!! debug here
                weights="weight",
                mode="out",
                output="epath",
            )
            # [origin, destination list, path list, flow list]
            list_of_spath.append(
                [name_of_origin_node, list_of_name_destination_node, paths, flows]
            )

        # calculate od flow matrix
        # [origin, destination, path, flow]
        temp_flow_matrix = pd.DataFrame(
            list_of_spath,
            columns=[
                "origin",
                "destination",
                "path",
                "flow",
            ],
        ).explode(["destination", "path", "flow"])

        # calculate edge flows
        # [edge_name, flow]
        temp_edge_flow = get_flow_on_edges(temp_flow_matrix, col_eid, "path", "flow")
        temp_edge_flow[col_eid] = temp_edge_flow[col_eid].map(
            edge_index_to_name
        )  # edge name

        # road form -> combined type
        temp_edge_flow["combined_label"] = temp_edge_flow[col_eid].map(edge_cbtype_dict)
        temp_edge_flow["isUrban"] = temp_edge_flow[col_eid].map(
            edge_isUrban_dict
        )  # urban/suburban
        temp_edge_flow["temp_acc_flow"] = temp_edge_flow[col_eid].map(
            acc_flow_dict
        )  # flow
        temp_edge_flow["temp_acc_capacity"] = temp_edge_flow[col_eid].map(
            acc_capacity_dict
        )  # capacity
        temp_edge_flow["est_overflow"] = (
            temp_edge_flow["flow"] - temp_edge_flow["temp_acc_capacity"]
        )  # estimated overflow: positive -> has overflow

        max_overflow = temp_edge_flow["est_overflow"].max()
        print(f"The maximum amount of overflow of edges: {max_overflow}")

        # break
        if max_overflow <= 0:
            temp_edge_flow["total_flow"] = (
                temp_edge_flow["flow"] + temp_edge_flow["temp_acc_flow"]
            )
            temp_edge_flow["speed"] = np.vectorize(partial_speed_flow_func)(
                temp_edge_flow["combined_label"],
                temp_edge_flow["isUrban"],
                temp_edge_flow["total_flow"],
            )
            temp_edge_flow["remaining_capacity"] = (
                temp_edge_flow["temp_acc_capacity"] - temp_edge_flow["flow"]
            )
            # update dicts
            # accumulated edge flows
            temp_dict = temp_edge_flow.set_index(col_eid)["total_flow"].to_dict()
            acc_flow_dict.update(
                {key: temp_dict[key] for key in acc_flow_dict.keys() & temp_dict.keys()}
            )
            # average flow rate
            temp_dict = temp_edge_flow.set_index(col_eid)["speed"].to_dict()
            acc_speed_dict.update(
                {
                    key: temp_dict[key]
                    for key in acc_speed_dict.keys() & temp_dict.keys()
                }
            )
            # accumulated remaining capacities
            temp_dict = temp_edge_flow.set_index(col_eid)[
                "remaining_capacity"
            ].to_dict()
            acc_capacity_dict.update(
                {
                    key: temp_dict[key]
                    for key in acc_capacity_dict.keys() & temp_dict.keys()
                }
            )

            #!!! update total cost of travelling
            temp_cost = (
                temp_edge_flow["e_id"].map(edge_cost_dict) * temp_edge_flow["flow"]
            )

            total_cost += temp_cost.sum()
            print("Iteration stops: there is no edge overflow.")
            break

        # calculate the ratio of flow adjustment (0 < r < 1)
        temp_edge_flow["r"] = np.where(
            temp_edge_flow["flow"] != 0,
            temp_edge_flow["temp_acc_capacity"] / temp_edge_flow["flow"],
            np.nan,
        )
        r = temp_edge_flow.r.min()
        if r < 0:
            print("Error: negative r!")
            break
        if r == 0:  # temp_acc_capacity = 0
            print("Error: (r==0) existing network has zero-capacity links!")
            break
        if r >= 1:
            print("Error: (r>=1) there is no edge overflow!")
            break
        print(f"r = {r}")  # set as NaN when flow is zero

        # update flow matrix
        temp_flow_matrix = temp_flow_matrix[
            temp_flow_matrix["path"].apply(lambda x: len(x) != 0)
        ]
        temp_flow_matrix["flow"] = temp_flow_matrix["flow"] * r

        # update edge flows
        temp_edge_flow["adjusted_flow"] = temp_edge_flow["flow"] * r
        temp_edge_flow["total_flow"] = (
            temp_edge_flow.temp_acc_flow + temp_edge_flow.adjusted_flow
        )
        temp_edge_flow["speed"] = np.vectorize(partial_speed_flow_func)(
            temp_edge_flow.combined_label,
            temp_edge_flow.isUrban,
            temp_edge_flow.total_flow,
        )
        temp_edge_flow["remaining_capacity"] = (
            temp_edge_flow.temp_acc_capacity - temp_edge_flow.adjusted_flow
        )
        temp_edge_flow.loc[
            temp_edge_flow.remaining_capacity < 0, "remaining_capacity"
        ] = 0.0  # capacity is non-negative

        #!!! update total cost of travelling
        temp_cost = temp_edge_flow["e_id"].map(edge_cost_dict) * temp_edge_flow["flow"]
        total_cost += temp_cost.sum()

        # update dicts
        # accumulated flows
        temp_dict = temp_edge_flow.set_index(col_eid)["total_flow"].to_dict()
        acc_flow_dict.update(
            {key: temp_dict[key] for key in acc_flow_dict.keys() & temp_dict.keys()}
        )
        # average flow rate
        temp_dict = temp_edge_flow.set_index(col_eid)["speed"].to_dict()
        acc_speed_dict.update(
            {key: temp_dict[key] for key in acc_speed_dict.keys() & temp_dict.keys()}
        )
        # accumulated remaining capacities
        temp_dict = temp_edge_flow.set_index(col_eid)["remaining_capacity"].to_dict()
        acc_capacity_dict.update(
            {key: temp_dict[key] for key in acc_capacity_dict.keys() & temp_dict.keys()}
        )

        # if remaining supply < 1 -> 0
        supply_dict = {
            k: filter_less_than_one(np.array(v) * (1 - r)).tolist()
            for k, v in supply_dict.items()
        }
        total_remain = sum(sum(values) for values in supply_dict.values())
        print(f"The total remaining supply is: {total_remain}")

        # update od matrix
        list_of_origins, supply_dict, destination_dict, non_allocated_flow = (
            update_od_matrix(temp_flow_matrix, supply_dict, destination_dict)
        )

        total_non_allocated_flow += non_allocated_flow  # record the overall flow loss
        number_of_destinations = sum(len(value) for value in destination_dict.values())
        print(f"The remaining number of origins: {len(list_of_origins)}")
        print(f"The remaining number of destinations: {number_of_destinations}")

        # update network structure (nodes and edges)
        #!!! update edge-related costs
        network, edge_index_to_name, edge_cost_dict = update_network_structure(
            network, edge_length_dict, acc_speed_dict, edge_toll_dict, temp_edge_flow
        )

        iter_flag += 1

    print("The flow simulation is completed!")
    print(f"total travel cost is (£): {total_cost}")
    print(f"The total non-allocated flow is {total_non_allocated_flow}")
    return acc_speed_dict, acc_flow_dict, acc_capacity_dict
