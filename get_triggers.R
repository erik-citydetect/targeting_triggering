get_triggers <- function(dt_targets, dt_parcels, dt_roads){
  # parcel snapshot locations for Tuscaloosa
  # Input parameters for GIS/camera linkages
  if(!file.exists(triggers_location)){
    
    source('R/clean_addresses.R')
    dt_targets <- get_targets()
    dt_parcels <- get_parcels()
    dt_triggers <- data.table()
    sf_roads <- get_roads()
    dt_roads <- as.data.table(sf_roads)
    dt_roads <- dt_roads[!is.na(normalized_street)]
    
    dt_targets <- dt_targets[,geo:=as_geos_geometry(st_as_sf(geometry))]
    dt_targets <- dt_targets[,target_lon:=geos_extent(geos_centroid(geometry))[1]]
    dt_targets <- dt_targets[,target_lat:=geos_extent(geos_centroid(geometry))[2]]
    dt_targets <- dt_targets[, lon_max:=geos::geos_extent(geo)[3]]
    dt_targets$target_id <- 1:nrow(dt_targets)
    dt_parcel_shapes <- dt_parcel_shapes[, lat_min:=geos::geos_extent(geo)[2]]
    dt_parcel_shapes <- dt_parcel_shapes[, lon_min:=geos::geos_extent(geo)[1]]
    
    setkey(dt_targets, normalized_street)
    setkey(dt_roads, normalized_street)
    dt_targets_roads <- dt_roads[dt_targets, allow.cartesian = TRUE]
    i_target_ids <- 1:nrow(dt_targets)
    #i_target_ids <- 1:100
    
    plan(multisession,workers=3)
    handlers(global = TRUE)
    #handlers(c("progress", 'rstudio'))
    handlers('rstudio')
    
    my_fcn <- function(i_target_ids, dt_targets_roads, dt_targets, sf_roads, dt_roads){
      pb <- progressor(along = i_target_ids)
      
      foreach(i=i_target_ids, .options.future = list(seed = TRUE)) %dofuture% {
        pb(sprintf("i=%g", i))
        
        dt_target <- dt_targets[i]
        sf_target <- st_as_sf(dt_target, coords=c('target_lon', 'target_lat'))
        sf_target <- st_set_crs(sf_target, proj_WGS84)
        
        nearest_road_dist <- 10000
        dt_target_roads <- dt_targets_roads[target_id == dt_target$target_id]
        target_road_test <- nrow(dt_target_roads[!is.na(id)])>0
        if(target_road_test==TRUE){
          sf_target_roads <- sf_roads[sf_roads$id %in% dt_target_roads$id, ]
          target_roads_dist <- as.numeric(st_distance(sf_target, sf_target_roads))
          nearest_road_ind <- which.min(target_roads_dist)
          nearest_road_id <- sf_target_roads[nearest_road_ind,]$id
          nearest_road_dist <- target_roads_dist[nearest_road_ind]
          sf_nearest_road <- sf_target_roads[sf_target_roads$id %in% nearest_road_id,]
          if(nearest_road_dist < 50){ #Neters
            #cat('Named match road too far away at distance', nearest_road_dist, '\n')
            
            sf_nearest_points <- st_sf(st_cast(st_transform(st_nearest_points(st_transform(sf_target, proj_orig),
                                                                              st_transform(st_sf(sf_nearest_road), proj_orig)),
                                                            crs = proj_WGS84), 'POINT'))
            sf_nearest_road_point <- sf_nearest_points[2,]
            nearest_road_coords <- st_coordinates(sf_nearest_road_point)
            
            orientation <- bearing(nearest_road_coords,
                                   cbind(dt_target$target_lon, dt_target$target_lat))
            orientation <- if(orientation<0){
              orientation <- orientation + 360
            } else {
              orientation <- orientation
            }
            #l_triggers <- vector('list', length=3) g
            l_triggers <- vector('list', 1)
            dt_trigger <- data.table(target_id = dt_target$target_id,
                                     parcel_id = dt_target$parcel_id,
                                     target_type = dt_target$target_type,
                                     target_lon = dt_target$target_lon,
                                     target_lat = dt_target$target_lat,
                                     trigger_lon = nearest_road_coords[1],
                                     trigger_lat = nearest_road_coords[2],
                                     orientation = orientation,
                                     nearest_road_dist = nearest_road_dist,
                                     target_road_name = sf_nearest_road$FULLNAME,
                                     target_trigger_match_type = 'road string close dist')
            
          }
        }
        if(target_road_test==FALSE || as.numeric(nearest_road_dist) > 50){
          target_roads_dist <- as.numeric(st_distance(sf_target, sf_roads))
          nearest_road_dist <- min(target_roads_dist)
          nearest_road_ind <- which.min(target_roads_dist)
          nearest_road_id <- sf_roads$id[nearest_road_ind]
          sf_nearest_road <- sf_roads[sf_roads$id == nearest_road_id,]
          #cat('Named match road too far away at distance', nearest_road_dist, '\n')
          sf_nearest_points <- st_sf(st_cast(st_transform(st_nearest_points(st_transform(sf_target, proj_orig),
                                                                            st_transform(st_sf(sf_nearest_road), proj_orig)),
                                                          crs = proj_WGS84), 'POINT'))
          sf_nearest_road_point <- sf_nearest_points[2,]
          nearest_road_coords <- st_coordinates(sf_nearest_road_point)
          orientation <- bearing(nearest_road_coords,
                                 cbind(dt_target$target_lon,
                                       dt_target$target_lat))
          orientation <- if(orientation<0){
            orientation <- orientation + 360
          } else {
            orientation <- orientation
          }
          #l_triggers <- vector('list', length=3)
          dt_trigger <- data.table(target_id = dt_target$target_id,
                                   parcel_id = dt_target$parcel_id,
                                   target_type = dt_target$target_type,
                                   target_lon = dt_target$target_lon,
                                   target_lat = dt_target$target_lat,
                                   trigger_lon = nearest_road_coords[1],
                                   trigger_lat = nearest_road_coords[2],
                                   orientation = orientation,
                                   nearest_road_dist = nearest_road_dist,
                                   target_road_name = sf_nearest_road$FULLNAME,
                                   target_trigger_match_type = 'nearest road')
        }
        dt_trigger
      }
    }
    l_triggers <- my_fcn(i_target_ids, dt_targets_roads, dt_targets, sf_roads, dt_roads)
    dt_triggers<- rbindlist(l_triggers,use.names=TRUE, fill=TRUE)
    saveRDS(dt_triggers, triggers_location)
    
    # To JSON
    dt_triggers <- dt_triggers[, .(parcel_id = paste0(target_id, '---', parcel_id),
                                   parcel_lon = target_lon,
                                   parcel_lat = target_lat,
                                   trigger_lon,
                                   trigger_lat,
                                   orientation,
                                   epsilon=10)]
    dt_triggers <- dt_triggers[orientation<0, orientation:=orientation+360]
    dt_triggers <- dt_triggers[,orientation:=orientation-180]
    dt_triggers <- dt_triggers[orientation<0, orientation:=orientation+360]
    dt_triggers <- dt_triggers[, tolerance:=10]
    fwrite(dt_triggers, 'data/clean/gps_triggers.csv')
    system('python3 python/triggers_to_json.py')
  }else{
    dt_triggers <- readRDS(triggers_location)
  }
  return(dt_triggers)
}

## For the polygon vertices
#dt_polygon_verts <- get_polygon_vertices(target, parcels, nearest_road_84)
#dt_vert_coordinates <- st_coordinates(dt_polygon_verts$geometry)
#dt_vert_coordinates <- as.data.table(dt_vert_coordinates)
#sf_targets = st_as_sf(dt_vert_coordinates, coords = c("X", "Y"), 
#                      crs = CRS(proj_wgs84))
#sf_targets <- st_transform(sf_targets, crs=CRS(proj_orig))
#for(i_vert in 1:2){
#  epsilon <- 0
#  intersection_null <- TRUE
#  iter <- 0
#  i_l_trigger <- 1+i_vert
#  sp_target <- sf_targets[i_vert,]
#  sp_target <- as_Spatial(sp_target, cast=TRUE)
#  target_roads_dist <- gDistance(sp_target, target_roads, byid=TRUE)
#  nearest_road <- target_roads[which.min(target_roads_dist),]
#  nearest_road_84 <- st_as_sf(spTransform(nearest_road, CRS(proj_wgs84)))
#  nearest_road_dist <- target_roads_dist[which.min(target_roads_dist)]
#  nearest_road_buffer <- gBuffer(nearest_road, width=0.1)
#  while(isTRUE(intersection_null) & iter < 10000){
#    iter <- iter + 1
#    target_buffer <- gBuffer(sp_target, width=nearest_road_dist+epsilon)
#    road_buff_intersection <- suppressWarnings(raster::intersect(target_buffer, nearest_road_buffer))
#    intersection_null <- is.null(road_buff_intersection)
#    epsilon <- epsilon + 1
#  }
#  road_buff_intersection_84 <- spTransform(road_buff_intersection, CRS(proj_wgs84))
#  road_centroid <- gCentroid(road_buff_intersection_84)
#  orientation <- bearing(road_centroid@coords[1,], dt_vert_coordinates[i_vert,.(X,Y)])
#  orientation <- if(orientation<0){
#    orientation <- orientation + 360
#  } else {
#    orientation <- orientation
#  }
# l_triggers[[i_l_trigger]] <- data.table(target_id = target_ids[i],
#                                          target_type = paste0('parcel_vert_', i_vert),
#                                          target_lon = dt_vert_coordinates$X[i_vert],
#                                          target_lat = dt_vert_coordinates$Y[i_vert],
#                                          trigger_lon = road_centroid@coords[1,1],
#                                          trigger_lat = road_centroid@coords[1,2],
#                                         orientation = orientation,
#                                          epsilon = epsilon,
#                                          target_road_name = target_road_name,
#                                          bad_loc = NA)
#}
#dt_trigger <- rbindlist(l_triggers, use.names = TRUE, fill=TRUE)
