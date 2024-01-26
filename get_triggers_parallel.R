get_triggers <- function(){
  # parcel snapshot locations for Tuscaloosa
  # Input parameters for GIS/camera linkages
  if(!file.exists(triggers_location)){
    library(data.table)
    library(geos)
    library(sf)
    dt_targets <- get_targets()
    dt_parcels <- get_parcels()
    
    dt_triggers <- data.table()
    
    proj_wgs84 <- copy(proj_WGS84)
    proj_orig <- readRDS(proj_orig_location)
    roads <- get_roads()
    dt_roads <- data.table(st_as_sf(roads))
    dt_roads <- dt_roads[, street_name_modified:=clean_addresses(FULLNAME)]
    geos_roads <- as_geos_geometry(dt_roads[, .(geometry)])
    
    l_roads_decomp <-
      lapply(1:nrow(dt_roads), function(i) data.table(id = dt_roads[i]$id, road_name_parts=unlist(str_split(dt_roads[i]$street_name_modified,
                                                                                                            ' '))))
    dt_roads_decomp <- rbindlist(l_roads_decomp, use.names=TRUE, fill=TRUE)
    dt_roads_decomp <- dt_roads_decomp[!is.na(road_name_parts)]
    dt_roads_decomp <- dt_roads_decomp[str_length(road_name_parts)>2]
    dt_roads_decomp <- dt_roads_decomp[!(road_name_parts %in% c('HWY', 'OLD', 'NEW'))]
    dt_roads_decomp[,N:=.N,by=road_name_parts]
    dt_roads_decomp <- dt_roads_decomp[N<100]
    dt[, list(text = paste(text, collapse="")), by = group]
    dt_roads_decomp <- dt_roads_decomp[, .(regex_road_name_parts=paste0('(',paste0(road_name_parts, collapse='|'), ')')), by=id]
    
    #l_par_triggers <- vector('list', nrow(dt_targets))
    i_bad_locs <- 0
    
    assign_triggers <- function(iterator){
      pb <- txtProgressBar(min = 1, max = iterator - 1, style = 3)
      count <- 0
      function(...) {
        count <<- count + length(list(...)) - 1
        setTxtProgressBar(pb, count)
        flush.console()
        rbind(...) # this can feed into .combine option of foreach
      }
    }
    
    myCluster <- parallel::makePSOCKcluster(getOption("mc.cores", detectCores()-3))
    doParallel::registerDoParallel(myCluster)
    
    l_par_triggers <- foreach(i = 1:nrow(dt_targets), .combine=assign_triggers(nrow(dt_targets))) %dopar% {
      dt_target <- dt_targets[i,]
      sf_target <- st_as_sf(dt_target,
                            coords = c("target_lon", "target_lat"), 
                            crs = 4326)
      geos_target <- geos::as_geos_geometry(sf_target)
      target_extent <- geos::geos_extent(geos_target)
      target_lon <- target_extent$xmin
      target_lat <- target_extent$ymin
      
      #dt_target_decomp <- dt_targets_decomp[target_id %in% dt_target$target_id]
      #setkey(dt_target_decomp, target_name_parts)
      #setkey(dt_roads_decomp, road_name_parts)
      match_road_ids <- unique(dt_roads_decomp[which(sapply(dt_roads_decomp$regex_road_name_parts, function(x) str_detect(dt_target$target_address, pattern = x)))]$id)
      dt_target_roads <- dt_roads[id %in% match_road_ids]
      sf_target_roads <- st_as_sf(dt_target_roads)
      geos_target_roads <- as_geos_geometry(dt_target_roads$geometry)
      
      # Check to see if there is an address match on the roads
      #ggmap(get_googlemap(center = c(lon = target_lon,
      #                               lat = target_lat),
      #                    zoom = 17, scale = 2,
      #                    maptype ='satellite',
      #                    color = 'color')) +
      #  geom_sf(data=sf_target_roads, color='red', size=10, inherit.aes=FALSE) +
      #  geom_point(data=target_extent, aes(x=target_lon, y=target_lat), color='yellow', size=6, shape=10)
      
      if(nrow(dt_target_roads)>0){
        target_roads_dist <- geos_distance(geos_target, geos_target_roads)
        nearest_road <- dt_target_roads[which.min(target_roads_dist),]
        geos_nearest_road <- NULL
        geos_nearest_road <- geos::as_geos_geometry(nearest_road)
        nearest_road_dist <- target_roads_dist[which.min(target_roads_dist)]
        nearest_road_buffer <- geos_buffer(geos_nearest_road,distance = 0.00001)
        #plot(nearest_road_buffer, col='red')
        #plot(nearest_road$geometry, add=TRUE)
        #nearest_road_buffer <- gBuffer(nearest_road, width=0.1)
        
        # If the nearest road is too far away
        buffer_dist <- nearest_road_dist
        if(nearest_road_dist < 0.002){ #0.0008
          #cat('Named match road too far away at distance', nearest_road_dist, '\n')
          epsilon <- 0
          intersection_null <- TRUE
          iter <- 0
          while(isTRUE(intersection_null) & iter < 10000){
            iter <- iter + 1
            target_buffer <- geos_buffer(geos_target, distance=buffer_dist+epsilon)
            #plot(target_buffer, col='yellow',add=TRUE)
            road_buff_intersection <- geos_intersection(target_buffer, nearest_road_buffer)
            intersection_null <- geos_is_empty(road_buff_intersection)
            epsilon <- epsilon + 0.000001
          }
          road_centroid <- geos_centroid(road_buff_intersection)
          road_point_lat <- geos::geos_extent(road_centroid)$ymin
          road_point_lon <- geos::geos_extent(road_centroid)$xmin
          orientation <- bearing(cbind(road_point_lon, road_point_lat),
                                 cbind(target_lon, target_lat))
          orientation <- if(orientation<0){
            orientation <- orientation + 360
          } else {
            orientation <- orientation
          }
          #l_triggers <- vector('list', length=3)
          l_triggers <- vector('list', 1)
          dt_trigger <- data.table(target_id = dt_target$target_id,
                                   parcel_id = dt_target$parcel_id,target_type = 'centered',
                                   target_lon = target_lon,
                                   target_lat = target_lat,
                                   trigger_lon = road_point_lon,
                                   trigger_lat = road_point_lat,
                                   orientation = orientation,
                                   nearest_road_dist = nearest_road_dist,
                                   epsilon = epsilon,
                                   target_road_name = nearest_road$FULLNAME,
                                   target_trigger_match_type = 'road string close dist',
                                   bad_loc = NA)
          
        }
        if((nearest_road_dist >= 0.002)| iter == 10000){# Nearest string match road is too far away
          geos_target <- NULL
          geos_target <- as_geos_geometry(sf_target)
          
          roads_dist <- geos_distance(geos_target, geos_roads)
          nearest_road <- dt_roads[which.min(roads_dist),]
          geos_nearest_road <- geos::as_geos_geometry(nearest_road)
          nearest_road_dist <- roads_dist[which.min(roads_dist)]
          nearest_road_buffer <- geos_buffer(geos_nearest_road, distance = 0.00001)
          
          epsilon <- 0
          intersection_null <- TRUE
          iter <- 0
          while(isTRUE(intersection_null) & iter < 10000){
            iter <- iter + 1
            target_buffer <- geos_buffer(geos_target, distance=nearest_road_dist+epsilon)
            #plot(target_buffer, col='yellow',add=TRUE)
            road_buff_intersection <- geos_intersection(target_buffer, nearest_road_buffer)
            intersection_null <- geos_is_empty(road_buff_intersection)
            epsilon <- epsilon + 0.000001
          }
          road_centroid <- geos_centroid(road_buff_intersection)
          road_point_lat <- geos::geos_extent(road_centroid)$ymin
          road_point_lon <- geos::geos_extent(road_centroid)$xmin
          orientation <- bearing(cbind(road_point_lon, road_point_lat),
                                 cbind(target_lon, target_lat))
          orientation <- if(orientation<0){
            orientation <- orientation + 360
          } else {
            orientation <- orientation
          }
          dt_trigger <- data.table(target_id = dt_target$target_id,
                                   parcel_id = dt_target$parcel_id,
                                   target_type = 'centered',
                                   target_lon = target_lon,
                                   target_lat = target_lat,
                                   trigger_lon = road_point_lon,
                                   trigger_lat = road_point_lat,
                                   orientation = orientation,
                                   nearest_road_dist = nearest_road_dist,
                                   epsilon = epsilon,
                                   target_road_name = nearest_road$FULLNAME,
                                   target_trigger_match_type = 'road string long dist',
                                   bad_loc = NA)
        }
      }
      if(nrow(dt_target_roads)==0){
        geos_target <- NULL
        geos_target <- as_geos_geometry(sf_target)
        
        roads_dist <- geos_distance(geos_target, geos_roads)
        nearest_road <- dt_roads[which.min(roads_dist),]
        geos_nearest_road <- geos::as_geos_geometry(nearest_road)
        nearest_road_dist <- roads_dist[which.min(roads_dist)]
        nearest_road_buffer <- geos_buffer(geos_nearest_road, distance = 0.00001)
        
        epsilon <- 0
        intersection_null <- TRUE
        iter <- 0
        while(isTRUE(intersection_null) & iter < 10000){
          iter <- iter + 1
          target_buffer <- geos_buffer(geos_target, distance=nearest_road_dist+epsilon)
          #plot(target_buffer, col='yellow',add=TRUE)
          road_buff_intersection <- geos_intersection(target_buffer, nearest_road_buffer)
          intersection_null <- geos_is_empty(road_buff_intersection)
          epsilon <- epsilon + 0.000001
        }
        road_centroid <- geos_centroid(road_buff_intersection)
        road_point_lat <- geos::geos_extent(road_centroid)$ymin
        road_point_lon <- geos::geos_extent(road_centroid)$xmin
        orientation <- bearing(cbind(road_point_lon, road_point_lat),
                               cbind(target_lon, target_lat))
        orientation <- if(orientation<0){
          orientation <- orientation + 360
        } else {
          orientation <- orientation
        }
        dt_trigger <- data.table(target_id = dt_target$target_id,
                                 parcel_id = dt_target$parcel_id,target_type = 'centered',
                                 target_lon = target_lon,
                                 target_lat = target_lat,
                                 trigger_lon = road_point_lon,
                                 trigger_lat = road_point_lat,
                                 orientation = orientation,
                                 nearest_road_dist = nearest_road_dist,
                                 epsilon = epsilon,
                                 target_road_name = nearest_road$FULLNAME,
                                 target_trigger_match_type = 'no string nearest road dist',
                                 bad_loc = NA)
        
      }
      dt_trigger
      #saveRDS(dt_trigger, trigger_progress_location)
      #l_par_triggers[[i]] <- dt_trigger
    }
    
    }
    #l_par_triggers <- foreach(i=1:length(target_ids)) %dopar% {
    #  tic <- Sys.time()
      

      #  tic <- Sys.time()
      # l_par_triggers <- foreach(i=1:nrow(dt_targets),
      #                         .verbose=TRUE,
      #                         .noexport = c('geos_target_roads',
      #                                       'sf_target_roads',
      #                                       'dt_target_roads',
      #                                       'sf_target',
      #                                       'geos_target',
      #                                       'geos_nearest_road',
      #                                       'road_centroid',
      #                                       'epsilon',
      #                                       'buffer_dist',
      #                                       'geos_roads',
      #                                       'intersection_null',
      #                                       'iter',
      #                                       'match_road_ids',
      #                                       'nearest_roads',
      #                                       'nearest_road_buffer',
      #                                       'nearest_road_dist',
      #                                       'orientation',
      #                                       'road_buff_intersection',
      #                                       'road_point_lat',
      #                                       'road_point_lon',
      #                                       'target_buffer',
      #                                       'target_extent',
      #                                       'target_lat',
      #                                       'target_lon',
      #                                       'target_roads_dist',
      #                                       'xmin',
      #                                       'ymin'
      #                                       )
      # ) %dopar% {
      
      #for(i in 1:nrow(dt_targets)){
      #  trigger_progress_location <- paste0('data/progress_triggers/', i, '.rds')
      #  if((i %% 100) == 0){
      #    toc <- Sys.time()
      #    cat('Processing ', i, 'of', nrow(dt_targets), 'time:', toc-tic, '\n')
      #    saveRDS(dt_triggers, 'data/raw/triggers_temp.rds')
      #    tic <- Sys.time()
      #3  }
            #stopCluster(myCluster)
      #toc <- Sys.time()
      #cat(toc-tic)
      #saveRDS(l_par_triggers, 'data/l_par_triggers.rds')
      
      #l_par_triggers <- lapply(list.files('data/progress_triggers/', full.names=TRUE),
      #                          readRDS)
      dt_triggers<- rbindlist(l_par_triggers,use.names=TRUE, fill=TRUE)
      saveRDS(dt_triggers, triggers_location)
      fwrite(dt_triggers, 'data/clean/gps_triggers.csv')
    } else {
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
  