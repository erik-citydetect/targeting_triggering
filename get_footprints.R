get_footprints <- function(refresh=FALSE){
  
  if(!file.exists(clean_footprints_location)|refresh==TRUE){
    dt_parcels <- get_parcels(refresh=FALSE)
    dt_parcel_shapes <- dt_parcels[, .SD[1], by=.(parcel_shape_id)]
    dt_parcel_shapes <- dt_parcel_shapes[, .(parcel_shape_id,
                                             geometry,
                                             geo=as_geos_geometry(geometry))]
    dt_parcel_shapes <- dt_parcel_shapes[, lon_min:=geos::geos_extent(geo)[1]]
    dt_parcel_shapes <- dt_parcel_shapes[, lon_max:=geos::geos_extent(geo)[3]]
    dt_parcel_shapes <- dt_parcel_shapes[, lat_min:=geos::geos_extent(geo)[2]]
    dt_parcel_shapes <- dt_parcel_shapes[, lat_max:=geos::geos_extent(geo)[4]]
    dt_parcel_shapes <- dt_parcel_shapes[, geometry:=st_cast(st_cast(geometry))]
    
    # parcels_xmin <- min(dt_parcel_shapes$lon_min)
    # parcels_xmax <- max(dt_parcel_shapes$lon_max)
    # parcels_ymin <- min(dt_parcel_shapes$lat_min)
    # parcels_ymax <- max(dt_parcel_shapes$lat_max)
    
    # parcels_xmin <- -84.425
    # parcels_xmax <- 0
    # parcels_ymin <- 33.94
    # parcels_ymax <- 90
    # dt_parcel_shapes <- dt_parcel_shapes[lon_min > parcels_xmin]
    # dt_parcel_shapes <- dt_parcel_shapes[lat_min > parcels_ymin]
    # dt_parcel_shapes <- dt_parcel_shapes[lon_min > parcels_xmin]
    # dt_parcel_shapes <- dt_parcel_shapes[lon_min > parcels_xmin]
    saveRDS(dt_parcel_shapes, 'data/dt_parcel_shapes.rds')
    
    if(!file.exists('data/tmp_dt_footprints.rds')){
      if(!(file.exists('data/tmp_foot_stage.rds'))){
        sf_foots <- geojsonsf::geojson_sf(raw_footprints_location) # From Msoft footprints Github database
        sf_footprints <- st_transform(sf_foots, st_crs(proj_WGS84))
        sf_footprints <- st_make_valid(sf_footprints)
        sf_city <- get_city()
        sf_footprints_int <- st_intersection(sf_footprints, sf_city)
        saveRDS(sf_footprints_int, 'data/tmp_foot_stage.rds')
      } else {
        sf_footprints_int <- readRDS('data/tmp_foot_stage.rds')
      }
      sf_footprints_int <- st_cast(sf_footprints_int, 'MULTIPOLYGON')
      dt_footprints <- as.data.table(sf_footprints_int)
      dt_footprints <- dt_footprints[, geo:=as_geos_geometry(geometry)]
      dt_footprints <- dt_footprints[, lon:=geos_extent(geos::geos_centroid(geo))[1]]
      dt_footprints <- dt_footprints[, lat:=geos_extent(geos::geos_centroid(geo))[2]]
      # dt_footprints <- dt_footprints[lon > parcels_xmin]
      # dt_footprints <- dt_footprints[lon < parcels_xmax]
      # dt_footprints <- dt_footprints[lat > parcels_ymin]
      # dt_footprints <- dt_footprints[lat < parcels_ymax]
      # dt_footprints <- dt_footprints[, geo_check:=geos_is_valid(geo)]
      # dt_footprints <- dt_footprints[geo_check==TRUE]
      # dt_footprints <- dt_footprints[, geo:=geos_concave_hull_of_polygons(geo, ratio=0.001)]
      dt_footprints <- dt_footprints[, footprint_area:=geos::geos_area(geo)]
      dt_footprints <- dt_footprints[, footprint_row:=1:nrow(dt_footprints)]
      saveRDS(dt_footprints, 'data/tmp_dt_footprints.rds')
    } else {
      dt_footprints <- readRDS('data/tmp_dt_footprints.rds')
     }
    # Assign each footprint to a parcel shape (if footprint overlaps)
    #dt_footprints$geo <- NUL
    dt_footprints <<- NULL
    dt_footprints <- readRDS('data/tmp_dt_footprints.rds')
    dt_footprints$geo <- NULL
    #dt_footprints <- dt_footprints[, geo:=as_geos_geometry(geometry)]
    #dt_footprints <<- dt_footprints
    dt_parcel_shapes <<- NULL
    dt_parcel_shapes <- readRDS('data/dt_parcel_shapes.rds')
    dt_parcel_shapes$geo <- NULL
    #dt_parcel_shapes$geo <- dt_parcel_shapes[, geo:=as_geos_geometry(geometry)]
    #dt_parcel_shapes <<- dt_parcel_shapes
    
    # myCluster <- parallel::makePSOCKcluster(getOption("mc.cores", detectCores()-3))
    # doParallel::registerDoParallel(myCluster)
    # tic <- Sys.time()
    # l_footprints <- foreach(i_foot=1:nrow(dt_footprints),
    #                         .verbose=TRUE,
    #                         .noexport = c('dt_parcel_shapes',
    #                                       'dt_parcel_shapes_intersections',
    #                                       'parcel_footprint_areas',
    #                                       'dt_parcel_shapes_sub',
    #                                       'row_inds_intersection',
    #                                       'n_intersections')
    # ) %dopar% {
    l_footprints <- vector('list', nrow(dt_footprints))
    dt_footprints <- dt_footprints[, geo:=as_geos_geometry(geometry)]
    dt_footprints <- dt_footprints[, lon:=geos_extent(geos::geos_centroid(geo))[1]]
    dt_footprints <- dt_footprints[, lat:=geos_extent(geos::geos_centroid(geo))[2]]
    tic <- Sys.time()
    for(i_foot in 1:nrow(dt_footprints)){
      if((i_foot %% 1000)==0){
        toc <- Sys.time()
        saveRDS(i_foot, paste0('data/progress/',i_foot,'.rds'))
        cat(i_foot, 'of', nrow(dt_footprints), 'in', toc-tic, 'seconds \n')
        tic <- Sys.time()
      }
      dt_footprint <- dt_footprints[i_foot]
      dt_footprint <- dt_footprint[, geo:=as_geos_geometry(geometry)]
      geo_footprint <- as_geos_geometry(dt_footprint$geometry)
      dt_parcel_shapes_sub <- dt_parcel_shapes[which(abs(lat_min-dt_footprint$lat)<0.0025)][
        which(abs(abs(lon_min)-abs(dt_footprint$lon))<0.0025)]
      geo_parcel_shapes_sub <- as_geos_geometry(dt_parcel_shapes_sub$geometry)
      row_inds_intersections <- which(geos_intersects(geo_footprint, geo_parcel_shapes_sub))
      dt_parcel_shapes_intersections <- dt_parcel_shapes_sub[row_inds_intersections]
      n_intersections <- nrow(dt_parcel_shapes_intersections)
      if(n_intersections>1){
        geo_intersection <- as_geos_geometry(dt_parcel_shapes_intersections$geometry)
        geos_footprint_parcels <- geos::geos_intersection(geo_footprint, geo_intersection)
        parcel_footprint_areas <- geos::geos_area(geos_footprint_parcels)
        dt_footprint$parcel_shape <- dt_parcel_shapes_intersections[which.max(parcel_footprint_areas)]$parcel_shape_id
        dt_footprint$geo <- NULL
        dt_footprint$n_parcel_ints <- n_intersections
        dt_footprint <- dt_footprint[, geometry:=st_as_sf(geos_footprint_parcels[which.max(parcel_footprint_areas)])]
      }
      if(n_intersections==1){
        dt_footprint$parcel_shape <- dt_parcel_shapes_intersections$parcel_shape_id[1]
        # if(dt_footprint$parcel_shape==38029) break
        dt_footprint$n_parcel_ints <- n_intersections
        dt_footprint$geo <- NULL
        #dt_footprint <- dt_footprint[, geometry:=st_as_sf(geos_footprint_parcels[which.max(parcel_footprint_areas)])]
      }
      if(n_intersections==0){
        dt_footprint$parcel_shape <- NA
        dt_footprint$n_parcel_ints <- 0
      }
      if('geo' %in% names(dt_footprint)){
        dt_footprint$geo <- NULL
      }
      l_footprints[[i_foot]]<-dt_footprint
    }                                
    #stopCluster(myCluster)
    #toc <- Sys.time()
    # cat(toc-tic)
    saveRDS(l_footprints, 'data/l_footprints.rds')
    l_footprints <- readRDS('data/l_footprints.rds')
    for(i_foot in 1:length(l_footprints)){
      dt_footprint <- l_footprints[[i_foot]]
      if(!is_null(dt_footprint)){
        if(dt_footprint$n_parcel_ints>1){
          if((i_foot %% 50)==0) cat(i_foot, '\n')
          sf_footprint <- st_as_sf(dt_footprint, sf_column_name = 'geometry')
          sf_footprint <- st_cast(sf_footprint, 'MULTIPOLYGON')
          l_footprints[[i_foot]] <- as.data.table(sf_footprint)
        }
      }
    }
    dt_footprints <- rbindlist(l_footprints, use.names = TRUE, fill=TRUE)
    dt_footprints <- dt_footprints[, row:=1:nrow(dt_footprints)]
    setnames(dt_footprints, names(dt_footprints), paste0('foot_', names(dt_footprints)))
    dt_footprints <- dt_footprints[!(foot_n_parcel_ints==0)]
    setnames(dt_footprints, 'foot_parcel_shape', 'parcel_shape_id')
    dt_footprints <- dt_footprints[!(is.na(parcel_shape_id))]
    geo_footprints <- geos::as_geos_geometry(dt_footprints$foot_geometry)
    footprints_area <- geos::geos_area(geo_footprints)
    dt_footprints$foot_area <- footprints_area
    setkey(dt_footprints, parcel_shape_id)
    dt_footprints$foot_footprint_row <- NULL
    st_write(dt_footprints, 'data/map/footprints.shp')
    saveRDS(dt_footprints, clean_footprints_location)
  } else {
    dt_footprints <- readRDS(clean_footprints_location)
  }
  return(dt_footprints)
}




















