map_triggers <- function(){
  dt_triggers <- get_triggers()
  dt_parcels <- get_parcels()
  dt_roads <- get_roads()
  sf_roads <- st_as_sf(dt_roads)
  sf_parcels <- st_cast(st_sf(dt_parcels, sf_column_name = 'geometry'))
  # Create orientation line (with correct distance)
  st_segment = function(r){st_linestring(t(matrix(unlist(r), 2, 2)))}
  df$geom = st_sfc(sapply(1:nrow(df), 
                          function(i){st_segment(df[i,])},simplify=FALSE))
  
  dt_endpoints <- dt_triggers[, .(trigger_lon, trigger_lat, target_lon, target_lat)]
  sf_bearing_line = st_sfc(sapply(1:nrow(dt_endpoints), 
                                  function(i){st_segment(dt_endpoints[i,])},simplify=FALSE))
  sf_bearing_line <- st_sf(sf_bearing_line)
  
  dt_footprints_assigned <- get_footprints()
  sf_footprints_assigned <- st_sf(dt_footprints_assigned)
  dt_footprints_full <- readRDS('data/dt_footprints.rds')
  sf_footprints_full <- st_sf(dt_footprints_full)
  
  st_write(sf_footprints_assigned, 'data/map/sf_footprints_assigned.geojson', delete_dsn = TRUE, delete_layer = TRUE)
  st_write(sf_footprints_full, 'data/map/sf_footprints_full.geojson', delete_dsn = TRUE, delete_layer = TRUE)
  st_write(sf_parcels, 'data/map/sf_parcels.geojson', delete_dsn = TRUE)
  st_write(sf_roads, 'data/map/sf_roads.geojson')
  st_write(sf_bearing_line, 'data/map/sf_bearing_line.geojson')                    
}
