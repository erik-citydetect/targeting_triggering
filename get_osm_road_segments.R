get_osm_road_segments<- function(){
  # https://medium.com/@harshalpuri/connecting-postgresql-via-ssh-tunnel-in-r-5c5afd949319
  if(!file.exists(osm_road_segments_location)){
    l_osmdata <- get_osmdata()
    sf_osm_lines <- l_osmdata$osm_lines
    sf_osm_roads <- sf_osm_lines[!(is.na(sf_osm_lines$highway)),]
    
    # Subset roads by study area (OSM too broad)
    dt_osm_name_key <- as.data.table(sf_osm_roads)[, .(osm_id, osm_name=name)]
    n_roads <- nrow(sf_osm_roads)
    l_sf_road_groups <- vector('list', length=n_roads)
    for(i_road in 1:n_roads){
      cat(i_road, 'of', n_roads, '\n')
      sf_osm_road <- sf_osm_roads[i_road,]
      #sf_osm_road <- sf_osm_roads[sf_osm_roads$osm_id=='179215365',]
      s_osm_road_id <- sf_osm_road$osm_id 
      s_osm_road_name <- sf_osm_road$name
      if(is.na(s_osm_road_name)) s_osm_road_name <- ''
      sf_road_points <- st_cast(sf_osm_road, 'MULTIPOINT')
      tmp_road_location <- paste0('data/raw/tmp_roads/', s_osm_road_id, '.rds')
      if(file.exists(tmp_road_location)){
        road_coords <- st_coordinates(sf_road_points)
        dt_road <- data.table(osm_road_id = s_osm_road_id,
                              lon_road = road_coords[,1], 
                              lat_road = road_coords[,2])
        dt_road$lon <- dt_road$lon_road
        dt_road$lat <- dt_road$lat_road
        dt_road$road_index <- 1:nrow(dt_road)
        
        sf_intersecting_roads <- osm_lines(l_osmdata, rownames(sf_osm_road))
        sf_intersecting_roads <- sf_intersecting_roads[!(rownames(sf_intersecting_roads)==rownames(sf_osm_road)),]
        sf_intersecting_roads$name[is.na(sf_intersecting_roads$name)] <- ''
        sf_intersecting_roads <- sf_intersecting_roads[!(sf_intersecting_roads$name == s_osm_road_name),]
        sf_intersecting_roads <- sf_intersecting_roads[is.na(sf_intersecting_roads$access=='private'),]
        
        
        if(nrow(sf_intersecting_roads)>0){
          sf_intersections <- st_intersection(sf_intersecting_roads, sf_osm_road)
          sf_intersections <- st_cast(sf_intersections, 'POINT')
          intersection_centroids <- st_coordinates(sf_intersections)
          dt_intersections <- as.data.table(sf_intersections)
          dt_intersections <- dt_intersections[, .(osm_id, geometry, 
                                                   coords=st_coordinates(geometry))]
          dt_intersections <- dt_intersections[, .SD[1], by=.(coords.X, coords.Y)]
          sf_intersections <- st_sf(dt_intersections)
          sf_intersections$id <- 1:nrow(sf_intersections)
          dt_intersects <- dt_intersections[, .(osm_id_intersect=osm_id,
                                                lon_intersect=coords.X,
                                                lat_intersect=coords.Y)][
                                                  ,lon:=lon_intersect][
                                                    ,lat:=lat_intersect]
          
          ggplot() + 
            geom_sf(data=sf_osm_road, color='red') +
            geom_sf(data=sf_intersecting_roads) +
            geom_sf(data=sf_intersections, aes(color=id), size = 3)
          
          
          setkey(dt_road, lon, lat)
          setkey(dt_intersects, lon, lat)
          dt_point_groups <- dt_intersects[dt_road]
          setorder(dt_point_groups, road_index)
          dt_point_groups <- dt_point_groups[, intersect_score:=0][
            !is.na(lon_intersect), intersect_score:=1][
              , start_group:=cumsum(intersect_score)][
                , end_group:=data.table::shift(start_group, n=1, type='lag' )][
                  is.na(end_group), end_group:=0]
          
          n_point_groups <- max(dt_point_groups$end_group)
          dt_road_groups <- data.table()
          for(i_point_group in 1:n_point_groups){
            dt_point_group <- dt_point_groups[start_group==i_point_group | end_group==i_point_group]
            dt_osm_ids_intersect <- dt_point_group[!is.na(osm_id_intersect), .(osm_id=osm_id_intersect, osm_id_intersect)]
            setkey(dt_osm_ids_intersect, osm_id)
            setkey(dt_osm_name_key, osm_id)
            dt_osm_ids_intersect <- dt_osm_name_key[dt_osm_ids_intersect]
            # Create multipoint line
            sf_group_points <- sfheaders::sf_multipoint(dt_point_group, x='lon', y='lat')
            st_crs(sf_group_points) <- proj_WGS84
            sf_group_line <- sf::st_cast(sf_group_points, 'LINESTRING')
            # Add in the group metadata
            sf_group_line$road_osm_id <- s_osm_road_id
            sf_group_line$road_osm_group_id <- i_point_group
            sf_group_line$road_osm_name <- s_osm_road_name
            sf_group_line$segment_distance <- st_distance(sf_group_line)
            sf_group_line$interesction_osm_id_0 <- dt_osm_ids_intersect$osm_id[1]
            sf_group_line$interesction_osm_name_0 <- dt_osm_ids_intersect$osm_name[1]
            sf_group_line$interesction_osm_id_1 <- dt_osm_ids_intersect$osm_id[2]
            sf_group_line$interesction_osm_name_1 <- dt_osm_ids_intersect$osm_name[2]
            dt_group_line <- as.data.table(sf_group_line)
            dt_road_groups <- rbindlist(list(dt_group_line, dt_road_groups), use.names=TRUE, fill=TRUE)
          }
        } else {
          dt_road_groups <- data.table(road_osm_id = s_osm_road_id,
                                      road_osm_name = s_osm_road_name,
                                      geometry = sf_osm_road$geometry)
        }
        saveRDS(dt_road_groups, tmp_road_location)
      } else {
        dt_road_groups <- readRDS(tmp_road_location)
      }
      l_sf_road_groups[[i_road]] <- dt_road_groups
    }
    l_sf_road_collapse <- lapply(l_sf_road_groups, function(x) st_as_sf(x))
    dt <- rbindlist(l_sf_road_collapse, use.names = TRUE, fill=TRUE)
    dt <- dt[, osm_id:=road_osm_id]
    setkey(dt, osm_id)
    dt_osm <- as.data.table(sf_osm_roads)
    dt_osm <- dt_osm[, .(osm_id, highway, footway)]
    setkey(dt_osm, osm_id)
    dt <- dt_osm[dt]
    dt <- dt[is.na(footway)]
    dt <- dt[!(highway=='path')]
    dt <- dt[!(highway=='proposed')]
    dt <- dt[!(highway=='raceway')]
    sf <- st_as_sf(dt)
    sf::st_write(sf, "data/clean/osm_roads.geojson", delete_dsn = TRUE, delete_layer = TRUE)

    
    # 1. Names only (Not the same road)
    # 1. Distance
    # 2. Export to postgis
    # 3. Litter/Debris assignment to segment
  } else {
    sf_osm_road_segments <- readRDS(osm_road_segements_location)
  }
  return(sf_osm_road_segments)
}
