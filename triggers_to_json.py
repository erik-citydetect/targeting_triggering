import json
import pandas as pd
import os

df = pd.read_csv(os.path.expanduser('/Users/erikjohnson/Documents/Github/stockton_ca/data/clean/gps_triggers.csv'))

json_out = []
for row in df.iterrows():
    parcel_lon = row[1]['parcel_lon']
    parcel_lat = row[1]['parcel_lat']
    target_orientation = row[1]['orientation']
    tolerance = 45
    parcel_id = row[1]['parcel_id']
    l_key = [parcel_lat, parcel_lon]
    coord_dict = {'orientation': target_orientation,
                  'tolerance': tolerance,
                  'parcel_id': parcel_id}
    json_out.append([l_key, coord_dict])

out_file = open(os.path.expanduser('/Users/erikjohnson/Documents/Github/stockton_ca/data/clean/stockton_ca_triggers.json'), "w")
json.dump(json_out, out_file)
out_file.close()
