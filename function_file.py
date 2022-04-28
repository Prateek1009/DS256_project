from collections import defaultdict

import pandas as pd


def initialize_onemany(MAX_TRANSFER, DESTINATION_LIST):
    '''
    initialize values for one-to-many TBTR.
    Args:
        MAX_TRANSFER (int): maximum transfer limit.
        DESTINATION_LIST (list): list of stop ids of destination stop.
    Returns:
        J (dict): dict to store arrival timestamps. Keys: number of transfer, Values: arrival time.
        inf_time (pandas.datetime): Variable indicating infinite time.
    '''
    #    inf_time = pd.to_datetime("today").round(freq='H') + pd.to_timedelta("365 day")
    inf_time = pd.to_datetime("2023-01-26 20:00:00")
    J = {desti: {x: [inf_time, 0] for x in range(MAX_TRANSFER+1)} for desti in DESTINATION_LIST}
    return J, inf_time


def initialize_from_desti_onemany(routes_by_stop_dict, stops_dict, DESTINATION_LIST, footpath_dict, idx_by_route_stop_dict):
    '''
    initialize routes/footpath to leading to destination stop in case of one-to-many rTBTR
    Args:
        routes_by_stop_dict (dict): preprocessed dict. Format {stop_id: [id of routes passing through stop]}.
        stops_dict (dict): preprocessed dict. Format {route_id: [ids of stops in the route]}.
        DESTINATION_LIST (list): list of stop ids of destination stop.
        footpath_dict (dict): preprocessed dict. Format {from_stop_id: [(to_stop_id, footpath_time)]}.
        idx_by_route_stop_dict (dict): preprocessed dict. Format {(route id, stop id): stop index in route}.
    Returns:
        L (nested dict): A dict to track routes/leading to destination stops. Key: route_id, value: {destination_stop_id: [(from_stop_idx, travel time, stop id)]}
    '''
    L_dict_final = {}
    for destination in DESTINATION_LIST:
        L_dict = defaultdict(lambda: [])
        try:
            transfer_to_desti = footpath_dict[destination]
            for from_stop, foot_time in transfer_to_desti:
                try:
                    walkalble_desti_route = routes_by_stop_dict[from_stop]
                    for route in walkalble_desti_route:
                        L_dict[route].append((idx_by_route_stop_dict[(route, from_stop)], foot_time, from_stop))
                except KeyError:
                    pass
        except KeyError:
            pass
        delta_tau = pd.to_timedelta(0, unit="seconds")
        for route in routes_by_stop_dict[destination]:
            L_dict[route].append((idx_by_route_stop_dict[(route, destination)], delta_tau, destination))
        L_dict_final[destination] = dict(L_dict)
    return L_dict_final


def update_label(label, no_of_transfer, predecessor_label, J, MAX_TRANSFER):
    '''
    Updates and returns destination pareto set.
    Args:
        label (pandas.datetime): optimal arrival time .
        no_of_transfer (int): number of transfer.
        predecessor_label (tuple): predecessor_label for backtracking (To be developed)
        J (dict): dict to store arrival timestamps. Keys: number of transfer, Values: arrival time
        MAX_TRANSFER (int): maximum transfer limit.
    Returns:
        J (dict): dict to store arrival timestamps. Keys: number of transfer, Values: arrival time
    '''
    J[no_of_transfer][1] = predecessor_label
    for x in range(no_of_transfer, MAX_TRANSFER+1):
        if J[x][0] > label:
            J[x][0] = label
    return J


def post_process_range(J, Q, rounds_desti_reached, PRINT_ITINERARY, DESTINATION, SOURCE, footpath_dict, stops_dict, stoptimes_dict, d_time, MAX_TRANSFER, trip_transfer_dict):
    '''
    Contains all the post-processing features for rTBTR.
    Currently supported functionality:
        Collect list of trips needed to cover pareto-optimal journeys.
    Args:
        J (dict): dict to store arrival timestamps. Keys: number of transfer, Values: arrival time
        Q (list): list of trips segments.
        rounds_desti_reached (list): Rounds in which DESTINATION is reached.
    Returns:
        necessory_trips (set): trips needed to cover pareto-optimal journeys.
    '''
    rounds_desti_reached = list(set(rounds_desti_reached))
    if PRINT_ITINERARY == 1:
        _print_tbtr_journey_otm(J, Q, DESTINATION, SOURCE, footpath_dict, stops_dict, stoptimes_dict, d_time, MAX_TRANSFER, trip_transfer_dict, rounds_desti_reached)
    necessory_trips = []
    for transfer_needed in reversed(rounds_desti_reached):
        no_of_transfer = transfer_needed
        current_trip = J[transfer_needed][1][0]
        journey = []
        while current_trip != 0:
            journey.append(current_trip)
            current_trip = [x for x in Q[no_of_transfer] if x[1] == current_trip][-1][-1][0]
            no_of_transfer = no_of_transfer - 1
        necessory_trips.extend(journey)
    return set(necessory_trips)


def initialize_from_source_range(dep_details, MAX_TRANSFER, stoptimes_dict, R_t):
    '''
    Initialize trips segments from source in rTBTR
    Args:
        dep_details (list): list of format [trip id, departure time, source index]
        MAX_TRANSFER (int): maximum transfer limit.
        stoptimes_dict (dict): preprocessed dict. Format {route_id: [[trip_1], [trip_2]]}.
        R_t (nested dict): Nested_Dict with primary keys as trip id and secondary keys as number of transfers. Format {trip_id: {[round]: first reached stop}}
    Returns:
        Q (list): list of trips segments
    '''
    Q = [[] for x in range(MAX_TRANSFER + 2)]
    route, trip_idx = [int(x) for x in dep_details[0].split("_")]
    stop_index = dep_details[2]
    # _enqueue_range1(f'{route}_{trip_idx}', stop_index, n, (0, 0), R_t, Q, stoptimes_dict, MAX_TRANSFER)
    connection_list = [(f'{route}_{trip_idx}', stop_index)]
    enqueue_range(connection_list, 1, (0, 0), R_t, Q, stoptimes_dict, MAX_TRANSFER)
    return Q


def enqueue_range(connection_list, nextround, predecessor_label, R_t, Q, stoptimes_dict, MAX_TRANSFER):
    '''
    adds trips-segments to next round round and update R_t. Used in range queries
    Args:
        connection_list (list): list of connections to be added. Format: [(to_trip_id, to_trip_id_stop_index)].
        nextround (int): next round/transfer number to which trip-segments are added
        predecessor_label (tuple): predecessor_label for backtracking journey ( To be developed ).
        R_t (nested dict): Nested_Dict with primary keys as trip id and secondary keys as number of transfers. Format {trip_id: {[round]: first reached stop}}
        Q (list): list of trips segments
        stoptimes_dict (dict): preprocessed dict. Format {route_id: [[trip_1], [trip_2]]}.
        MAX_TRANSFER (int): maximum transfer limit.
    Returns: None
    '''
    for to_trip_id, to_trip_id_stop in connection_list:
        if to_trip_id_stop < R_t[nextround][to_trip_id]:
            route, tid = [int(x) for x in to_trip_id.split("_")]
            Q[nextround].append((to_trip_id_stop, to_trip_id, R_t[nextround][to_trip_id], route, tid, predecessor_label))
            for x in range(tid, len(stoptimes_dict[route]) + 1):
                for r in range(nextround, MAX_TRANSFER + 1):
                    new_tid = f"{route}_{x}"
                    if R_t[r][new_tid] > to_trip_id_stop:
                        R_t[r][new_tid] = to_trip_id_stop


def post_process_range_onemany(J, Q, rounds_desti_reached, PRINT_ITINERARY, desti, SOURCE, footpath_dict,
                               stops_dict, stoptimes_dict, d_time, MAX_TRANSFER, trip_transfer_dict):
    '''
    Contains all the post-processing features for One-To-Many rTBTR.
    Currently supported functionality:
        Collect list of trips needed to cover pareto-optimal journeys.
    Args:
        J (dict): dict to store arrival timestamps. Keys: number of transfer, Values: arrival time
        Q (list): list of trips segments.
        rounds_desti_reached (list): Rounds in which DESTINATION is reached.
        desti (int): destination stop id.
    Returns:
        TBTR_out (set): Trips needed to cover pareto-optimal journeys.
    '''
    rounds_desti_reached = list(set(rounds_desti_reached))
    TP = _print_tbtr_journey_otm(J, Q, desti, SOURCE, footpath_dict, stops_dict, stoptimes_dict, d_time, MAX_TRANSFER, trip_transfer_dict, rounds_desti_reached, PRINT_ITINERARY)
    return TP

def _print_tbtr_journey_otm(J, Q, DESTINATION, SOURCE, footpath_dict, stops_dict, stoptimes_dict, D_TIME, MAX_TRANSFER, trip_transfer_dict, rounds_desti_reached, PRINT_ITINERARY):
    TP_list = []
    for x in reversed(rounds_desti_reached):
        round_no = x
        journey = []
        trip_segement_counter = J[DESTINATION][x][1][2]
        while round_no>0:
            pred = Q[round_no][trip_segement_counter]
            journey.append((pred[5][0], pred[1], pred[0]))
            trip_segement_counter = pred[5][1]
            round_no = round_no - 1
        from_stop_list = []
        for id, t_transfer in enumerate(journey[:-1]):
            from_Stop_onwards = journey[id+1][2]
            for from_stop, trasnsfer_list in trip_transfer_dict[t_transfer[0]].items():
                if from_stop<from_Stop_onwards:
                    continue
                else:
                    if (t_transfer[1], t_transfer[2]) in trasnsfer_list:
                        from_stop_list.append(from_stop)
        journey_final = [(journey[counter][0], x, journey[counter][1], journey[counter][2]) for counter, x in enumerate(from_stop_list)]
        # from source
        from_trip, from_stop_idxx= journey[-1][1], journey[-1][2]
        fromstopid = stops_dict[int(from_trip.split("_")[0])][from_stop_idxx]
        if fromstopid==SOURCE:
            journey_final.append(("trip",0, from_trip, from_stop_idxx))
        else:
            for to_stop, to_time in footpath_dict[fromstopid]:
                if to_stop== SOURCE:
                    journey_final.append(("walk", SOURCE, fromstopid, to_time+D_TIME))
                    break
        #Add final lag. Destination can either be along the route or at a walking distance from it.
        if J[DESTINATION][x][1][1] != (0, 0):    #Add here if the destination is at walking distance from final route
            try:
                final_route, boarded_from = int(journey_final[0][2].split("_")[0]), journey_final[0][3]
                found = 0
                for walking_from_stop_idx, stop_id in enumerate(stops_dict[final_route]):
                    if walking_from_stop_idx<boarded_from:continue
                    try:
                        for to_stop, to_stop_time in footpath_dict[stop_id]:
                            if to_stop==DESTINATION:
                                found = 1
                                journey_final.insert(0, ("walk", journey_final[0][2], boarded_from, walking_from_stop_idx, to_stop_time)) #walking_pointer, from_trip, from_stop, to_stop
                                break
                    except KeyError:continue
                    if found==1:break
            except AttributeError:
                if len(journey_final)==1:
                    final_route =int(J[DESTINATION][x][1][0].split("_")[0])
                    boarded_from = stops_dict[final_route].index(journey_final[0][2])
                    found = 0
                    for walking_from_stop_idx, stop_id in enumerate(stops_dict[final_route]):
                        if walking_from_stop_idx<boarded_from:continue
                        try:
                            for to_stop, to_stop_time in footpath_dict[stop_id]:
                                if to_stop==DESTINATION:
                                    found = 1
                                    journey_final.insert(0, ("walk", J[DESTINATION][x][1][0], boarded_from, walking_from_stop_idx, to_stop_time)) #walking_pointer, from_trip, from_stop, to_stop
                                    break
                        except KeyError:continue
                        if found==1:break
                else:raise NameError
        else:   #Destination is along the route.
            try:
                final_route, boarded_from = int(journey_final[0][2].split("_")[0]), journey_final[0][3]
                desti_index = stops_dict[final_route].index(DESTINATION)
                journey_final.insert(0, ("trip", journey_final[0][2], boarded_from, desti_index)) #walking_pointer, from_trip, from_stop, to_stop
            except AttributeError:
                if len(journey_final)==1:
                    final_route =int(J[DESTINATION][x][1][0].split("_")[0])
                    boarded_from = stops_dict[final_route].index(journey_final[0][2])
                    desti_index = stops_dict[final_route].index(DESTINATION)
                    journey_final.insert(0, ("trip", J[DESTINATION][x][1][0], boarded_from, desti_index)) #walking_pointer, from_trip, from_stop, to_stop
        if journey_final==[]:
            tid = [int(x) for x in journey[0][1].split("_")]
            tostop_det = stops_dict[tid[0]].index(DESTINATION)
            journey_final.append((journey[0][1], stoptimes_dict[tid[0]][tid[1]][journey[0][2]], stoptimes_dict[tid[0]][tid[1]][tostop_det]))
        journey_final.reverse()
        journey_final_copy = journey_final.copy()
        journey_final.clear()
        for c, leg in enumerate(journey_final_copy):
            if c==0:
                if leg[0]=="trip":
                    [trip_route, numb], fromstopidx = [int(x) for x in leg[2].split("_")], leg[3]
                    try:
                        journey_final.append([leg[2], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][journey_final_copy[c+1][1]]])
                    except TypeError:
                        try:
                            journey_final.append([leg[2], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][stops_dict[trip_route].index(DESTINATION)]])
                            break
                        except ValueError:
                            journey_final.append([leg[2], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][journey_final_copy[c+1][3]]])
                elif leg[0]=="walk":
                    journey_final.append(("walk", leg[1], leg[2], [time for stop, time in footpath_dict[leg[1]] if stop==leg[2]][0]))
            elif c==len(journey_final_copy)-1:
                if leg[0]=="trip":
                    [trip_route, numb], fromstopidx, tostopidx = [int(x) for x in leg[1].split("_")], leg[2], leg[3]
                    journey_final.append([leg[1], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][tostopidx]])
                elif leg[0]=="walk":
                    from_trip = [int(x) for x in leg[1].split("_")]
                    journey_final.append((leg[1], stoptimes_dict[from_trip[0]][from_trip[1]][leg[2]], stoptimes_dict[from_trip[0]][from_trip[1]][leg[3]]))
                    foot_connect = stoptimes_dict[from_trip[0]][from_trip[1]][leg[3]]
                    last_foot_tme = [time for stop, time in footpath_dict[foot_connect[0]] if stop==DESTINATION][0]
                    journey_final.append(("walk", foot_connect[0], DESTINATION, last_foot_tme, stoptimes_dict[from_trip[0]][from_trip[1]][leg[3]][1] + last_foot_tme))
            else:
                if c==1:
                    if journey_final_copy[c-1][0]=="walk":
                        [trip_route, numb], tostopidx = [int(x) for x in leg[0].split("_")], leg[1]
                        fromstopidx = stops_dict[trip_route].index(journey_final_copy[c-1][2])
                        journey_final.append([leg[0], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][tostopidx]])
                    elif journey_final_copy[c-1][0]=="trip":
                        [trip_route, numb], tostopidx = [int(x) for x in leg[0].split("_")], leg[1]
                        fromstopidx = stops_dict[trip_route].index(SOURCE)
                        if [leg[0], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][tostopidx]] not in journey_final:
                            journey_final.append([leg[0], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][tostopidx]])
                from_stop = stops_dict[int(journey_final_copy[c][0].split("_")[0])][int(journey_final_copy[c][1])]
                to_stop = stops_dict[int(journey_final_copy[c][2].split("_")[0])][int(journey_final_copy[c][3])]
                if from_stop!=to_stop:
                    time_needed = [x[1] for x in footpath_dict[from_stop] if x[0]==to_stop][0]
                    journey_final.append(("walk", from_stop, to_stop, time_needed))
                    if c+1!=len(journey_final_copy)-1:
                        [trip_route, numb], fromstopidx = [int(x) for x in leg[2].split("_")], leg[3]
                        journey_final.append([leg[2], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][journey_final_copy[c+1][1]]])
                elif from_stop==to_stop:
                    if c+1!=len(journey_final_copy)-1:
                        [trip_route, numb], fromstopidx = [int(x) for x in leg[2].split("_")], leg[3]
                        journey_final.append([leg[2], stoptimes_dict[trip_route][numb][fromstopidx], stoptimes_dict[trip_route][numb][journey_final_copy[c+1][1]]])
        TP = []
        for leg in journey_final:
            if leg[0]=="walk":
                if PRINT_ITINERARY==1:print(f"from {leg[1]} walk till  {leg[2]} for {leg[3].total_seconds()} seconds")
                TP.extend([leg[1], leg[2]])
            else:
                if PRINT_ITINERARY==1: print(f"from {leg[1][0]} board at {leg[1][1].time()} and get down on {leg[2][0]} at {leg[2][1].time()} along {leg[0]}")
                TP.extend([leg[1][0], leg[2][0]])
        TP_list.append(list(dict.fromkeys(TP)))
        if PRINT_ITINERARY==1: print("####################################")
    return TP_list

def direct_connection_tabe(stop_times_file):
    from collections import defaultdict
    from tqdm import tqdm
    t_geoups = stop_times_file.groupby("trip_id")
    connection_dict = defaultdict(list)
    for tid, trip in tqdm(t_geoups):
        trip = trip.sort_values(by="stop_sequence")[["stop_id", "arrival_time"]].values.tolist()
        for x in range(len(trip)-1):
            connection_dict[(trip[x][0], trip[x+1][0])].append((trip[x][1], trip[x+1][1]))
    for stoppair, connections in connection_dict.items():
        connection_dict[stoppair] = sorted(connections, key = lambda x: x[0])
    connection_dict = dict(connection_dict)
    return connection_dict

def onetomany_rtbtr(SOURCE, DESTINATION_LIST, d_time_groups, MAX_TRANSFER, WALKING_FROM_SOURCE, PRINT_ITINERARY, OPTIMIZED,
                    routes_by_stop_dict, stops_dict, stoptimes_dict, footpath_dict, idx_by_route_stop_dict, trip_transfer_dict, trip_set):
    """
    One to many rTBTR implementation
    Args:
        SOURCE (int): stop id of source stop.
        DESTINATION_LIST (list): list of stop ids of destination stop.
        d_time_groups (pandas.group): all possible departures times from all stops.
        MAX_TRANSFER (int): maximum transfer limit.
        WALKING_FROM_SOURCE (int): 1 or 0. 1 means walking from SOURCE is allowed.
        PRINT_ITINERARY (int): 1 or 0. 1 means print complete path.
        OPTIMIZED (int): 1 or 0. 1 means collect trips and 0 means collect routes.
        routes_by_stop_dict (dict): preprocessed dict. Format {stop_id: [id of routes passing through stop]}.
        stops_dict (dict): preprocessed dict. Format {route_id: [ids of stops in the route]}.
        stoptimes_dict (dict): preprocessed dict. Format {route_id: [[trip_1], [trip_2]]}.
        footpath_dict (dict): preprocessed dict. Format {from_stop_id: [(to_stop_id, footpath_time)]}.
        idx_by_route_stop_dict (dict): preprocessed dict. Format {(route id, stop id): stop index in route}.
        trip_transfer_dict (nested dict): keys: id of trip we are transferring from, value: {stop number: list of tuples
        of form (id of trip we are transferring to, stop number)}
        trip_set (set): set of trip ids from which trip-transfers are available.
    Returns:
        if OPTIMIZED==1:
            out (list):  list of trips required to cover all optimal journeys Format: [trip_id]
        elif OPTIMIZED==0:
            out (list):  list of routes required to cover all optimal journeys. Format: [route_id]
    """
    DESTINATION_LIST.remove(SOURCE)
    d_time_list = d_time_groups.get_group(SOURCE)[["trip_id", 'arrival_time', 'stop_sequence']].values.tolist()
    if WALKING_FROM_SOURCE == 1:
        try:
            source_footpaths = footpath_dict[SOURCE]
            for connection in source_footpaths:
                d_time_list.extend(d_time_groups.get_group(connection[0])[["trip_id", 'arrival_time', 'stop_sequence']].values.tolist())
        except KeyError:
            pass
    d_time_list.sort(key=lambda x: x[1], reverse=True)

    TP_list = []
    J, inf_time = initialize_onemany(MAX_TRANSFER, DESTINATION_LIST)
    L = initialize_from_desti_onemany(routes_by_stop_dict, stops_dict, DESTINATION_LIST, footpath_dict, idx_by_route_stop_dict)
    R_t = {x: defaultdict(lambda: 1000) for x in range(0, MAX_TRANSFER + 2)}  # assuming maximum route length is 1000

    for dep_details in d_time_list:
        rounds_desti_reached = {x: [] for x in DESTINATION_LIST}
        n = 1
        Q = initialize_from_source_range(dep_details, MAX_TRANSFER, stoptimes_dict, R_t)
        dest_list_prime = DESTINATION_LIST.copy()
        while n <= MAX_TRANSFER:
            stop_mark_dict = {stop: 0 for stop in dest_list_prime}
            scope = []
            for counter, trip_segment in enumerate(Q[n]):
                from_stop, tid, to_stop, trip_route, tid_idx = trip_segment[0: 5]
                trip = stoptimes_dict[trip_route][tid_idx][from_stop:to_stop]
                connection_list = []
                for desti in dest_list_prime:
                    try:
                        L[desti][trip_route]
                        stop_list, _ = zip(*trip)
                        for last_leg in L[desti][trip_route]:
                            idx = [x[0] for x in enumerate(stop_list) if x[1] == last_leg[2]]
                            if idx and from_stop < last_leg[0] and trip[idx[0]][1] + last_leg[1] < J[desti][n][0]:
                                if last_leg[1] == pd.to_timedelta(0, unit="seconds"):
                                    walking = (0, 0)
                                else:
                                    walking = (1, stops_dict[trip_route][last_leg[0]])
                                J[desti] = update_label(trip[idx[0]][1] + last_leg[1], n, (tid, walking, counter), J[desti], MAX_TRANSFER)
                                rounds_desti_reached[desti].append(n)
                    except KeyError:
                        pass
                    try:
                        if tid in trip_set and trip[1][1] < J[desti][n][0]:
                            if stop_mark_dict[desti]==0:
                                scope.append(desti)
                                stop_mark_dict[desti]=1
                            connection_list.extend([connection for from_stop_idx, transfer_stop_id in enumerate(trip[1:], from_stop + 1)
                                                    for connection in trip_transfer_dict[tid][from_stop_idx]])
                    except IndexError:
                        pass
                connection_list = list(set(connection_list))
                enqueue_range(connection_list, n + 1, (tid, counter, 0), R_t, Q, stoptimes_dict, MAX_TRANSFER)
            dest_list_prime = [*scope]
            n = n + 1
        for desti in DESTINATION_LIST:
            if rounds_desti_reached[desti]:
                TP_list.extend(post_process_range_onemany(J, Q, rounds_desti_reached[desti], PRINT_ITINERARY, desti, SOURCE, footpath_dict, stops_dict, stoptimes_dict,
                                                          dep_details[1], MAX_TRANSFER, trip_transfer_dict))
    return TP_list

def tempfunc(item_list):
    item_list.append(2)
    return item_list
