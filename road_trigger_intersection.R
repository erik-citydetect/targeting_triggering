
# Build road buffers
road_trigger_intersection <- function(){
  bham_proj <- "+proj=tmerc +lat_0=30.5 +lon_0=-85.8333333333333 +k=0.99996 +x_0=200000 +y_0=0 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs +type=crs"
  
  sf_split_roads <- sf::st_sf(dt_split_roads, crs=4326)
  sf_split_roads_proj <- sf::st_transform(sf_split_roads, crs=6355)
  sf_split_roads_buffer_left <- sf::st_buffer(sf_split_roads_proj, dist = 6, singleSide = TRUE)
  sf_split_roads_buffer_right <- sf::st_buffer(sf_split_roads_proj, dist = -6, singleSide = TRUE)
  sf_split_roads_buffer <- rbind(sf_split_roads_buffer_right, sf_split_roads_buffer_left)
  sf_split_roads_buffer <- sf::st_transform(sf_split_roads_buffer, crs=4326)
  sf_split_roads_buffer$geo_linestring <- NULL
  sf_split_roads_buffer$geo <- NULL
  st_write(sf_split_roads_buffer, 'maps/sf_split_roads_buffer.geojson', delete_dsn = TRUE)
  sf_split_roads_buffer <- sf::st_transform(sf_split_roads_buffer, crs=6355)
  
  # Build trigger buffers
  sf_trigger_targets = sf::st_as_sf(dt_trigger_targets, coords = c("trigger_lon", "trigger_lat"), 
                                    crs = 4326, agr = "constant")
  st_write(sf_trigger_targets, 'maps/sf_trigger_targets.geojson')
  sf_trigger_targets_proj <- sf::st_transform(sf_trigger_targets, crs=6355)
  
  # Join roads to targets
  sf_point_polygon_join <- sf::st_join(sf_split_roads_buffer, sf_trigger_targets_proj)
  dt_point_polygon_join <- as.data.table(sf_point_polygon_join)
  dt_point_polygon_join <- dt_point_polygon_join[, target_id:=as.character(target_id)]
  setkey(dt_point_polygon_join, target_id)
  dt_parcels_coverage <- dt_parcels_coverage[, target_id:=as.character(target_id)]
  setkey(dt_parcels_coverage, target_id)
  dt_point_polygon_join <- dt_parcels_coverage[dt_point_polygon_join]
  dt_point_polygon_join <- dt_point_polygon_join[!is.na(target_id)]
  
  dt_parcels_zoning <- readRDS('data/analysis/dt_parcels_zoning.rds')
  dt_parcels_zoning <- dt_parcels_zoning[, target_id:=as.character(target_id)]
  dt_parcels_zoning <- dt_parcels_zoning[, .(target_id, zoning_full, zoning_prefix)]
  setkey(dt_parcels_zoning, target_id)
  dt_point_polygon_join <- dt_parcels_zoning[dt_point_polygon_join]
  dt_point_polygon_join <- dt_point_polygon_join[zoning_prefix %in% 'R']
  
  dt_road_stats <- dt_point_polygon_join[,n_road_triggers:=.N,by=split_fID]
  dt_road_stats <- dt_road_stats[, n_road_triggers:=.N, by=.(split_fID)]
  dt_road_stats <- dt_road_stats[chr_labeled==0]
  dt_road_stats <- dt_road_stats[, .(n_not_labeled=.N), by=.(split_fID, n_road_triggers)]
  dt_road_stats <- dt_road_stats[, share_unlabeled:=n_not_labeled/n_road_triggers]
  dt_road_stats <- dt_road_stats[order(n_not_labeled, decreasing = TRUE)]
  #dt_road_stats <- dt_road_stats[, cumsum_n_not_labeled:=cumsum(n_not_labeled)]
  #dt_road_stats <- dt_road_stats[, n_not_labeled_total:=sum(n_not_labeled)]
  #dt_road_stats <- dt_road_stats[, pct_missing:=cumsum_n_not_labeled/n_not_labeled_total]
  #dt_road_stats <- dt_road_stats[, pct_road_split_not_labeled:=n_not_labeled]
  dt_road_stats <- dt_road_stats[share_unlabeled > 0.75]
  setkey(dt_road_stats, split_fID)
  
  dt_split_roads <- as.data.table(sf_split_roads)
  setkey(dt_split_roads, split_fID)
  
  dt_roads_missing <- dt_split_roads[dt_road_stats]
  sf_roads_missing <- st_sf(dt_roads_missing)
  sf::st_write(sf_roads_missing, 'maps/sf_roads_missing.geojson', delete_dsn = TRUE)
  dt_roads_missing <- as.data.table(sf_roads_missing)
  
  setkey(roads, id)
  dt_roads <- roads
  dt_roads$geometry <- NULL
  setkey(dt_roads_missing, id)
  dt_roads_missing <- dt_roads[dt_roads_missing]
  dt_roads_missing <- dt_roads_missing[!(str_detect(FULLNAME, '(?i)aly'))]
  sf_roads_missing <- st_sf(dt_roads_missing)
  sf::st_write(sf_roads_missing, 'maps/sf_roads_missing.geojson', delete_dsn = TRUE)
  
  
  saveRDS(dt_roads_missing, 'data/analysis/dt_roads_missing.rds')
}
