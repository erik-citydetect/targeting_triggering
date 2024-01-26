create_hiveCV_triggers <- function(){
  dt_original <- fread('~/Documents/Github/columbia_sc/data/clean/gps_triggers.csv')
  dt_original <- dt_original[, .(parcel_id = paste0(target_id, '---', parcel_id),
                                 parcel_lon = target_lon,
                                 parcel_lat = target_lat,
                                 trigger_lon,
                                 trigger_lat,
                                 orientation,
                                 epsilon=10)]
  dt_original <- dt_original[orientation<0, orientation:=orientation+360]
  dt_original <- dt_original[,orientation:=orientation-180]
  dt_original <- dt_original[orientation<0, orientation:=orientation+360]
  dt_original <- dt_original[, tolerance:=10]
  fwrite(dt_original, 'data/clean/dt_columbia_sc_triggers.csv')
  system('python3 /Users/erikjohnson/Documents/Github/columbia_sc/python/triggers_to_json.py')
}
