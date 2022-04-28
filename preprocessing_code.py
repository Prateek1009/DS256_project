
from time import time
from miscellaneous_func import *
from transferpattern.transferpattern_func import *
from tqdm import tqdm
import pickle
from multiprocessing import Pool

print_logo()
print("Reading Testcase...")
FOLDER = './sweden'
FOLDER = './swiss'
cores = 100
stops_file, trips_file, stop_times_file, transfers_file, stops_dict, stoptimes_dict, footpath_dict, routes_by_stop_dict, idx_by_route_stop_dict = read_testcase(FOLDER)

with open(f'./GTFS/{FOLDER}/TBTR_trip_transfer_dict.pkl', 'rb') as file:
    trip_transfer_dict = pickle.load(file)
trip_set = set(trip_transfer_dict.keys())
print_network_details(transfers_file, trips_file, stops_file)
MAX_TRANSFER = 4
WALKING_FROM_SOURCE = 0
PRINT_ITINERARY = 0
OPTIMIZED = 1
d_time_groups = stop_times_file.groupby("stop_id")
def run_parallel(SOURCE):
    DESTINATION_LIST = list(routes_by_stop_dict.keys())
    output = onetomany_rtbtr(SOURCE, DESTINATION_LIST, d_time_groups, MAX_TRANSFER, WALKING_FROM_SOURCE, PRINT_ITINERARY,
                             OPTIMIZED, routes_by_stop_dict, stops_dict, stoptimes_dict, footpath_dict, idx_by_route_stop_dict, trip_transfer_dict,
                             trip_set)
    with open(f"./transferpattern/transfer_pattern/{FOLDER}/{SOURCE}", "wb") as fp:
        pickle.dump(output, fp)

if __name__ == "__main__":
    source_LIST = list(routes_by_stop_dict.keys())
    start = time()
    with Pool(cores) as pool:
        result = pool.map(run_parallel, source_LIST)
    print(f'    Time required: {round(time() - start)}')
