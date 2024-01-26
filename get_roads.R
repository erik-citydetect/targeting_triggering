get_roads <- function(){

  if(!file.exists(roads_location)){
    sf_roads <- st_read('data/raw/tl_2023_06077_roads/tl_2023_06077_roads.shp')
    sf_roads <- st_transform(sf_roads, proj_WGS84)
    sf_roads$id<- sf_roads$LINEARID
    sf_roads$FULLNAME <- stringr::str_to_upper(sf_roads$FULLNAME)
    sf_roads$FULLNAME <- stringr::str_replace_all(sf_roads$FULLNAME, 'STATE RTE', 'HWY')
    dt_roads <- as.data.table(sf_roads)
    
    i_road_ids <- 1:nrow(dt_roads)
    plan(multisession)
    handlers(global = TRUE)
    #handlers(c("progress", 'rstudio'))
    handlers('rstudio')
    
    my_fcn <- function(i_road_ids, dt_roads){
      pb <- progressor(along = i_road_ids)
      
      foreach(i=i_road_ids, .options.future = list(seed = TRUE)) %dofuture% {
        
        pb(sprintf("i=%g", i))
        # cat(i , '\n')
        dt_road <- dt_roads[i]
        vec_address <- sapply(unlist(str_split(dt_road$FULLNAME, ' ')), str_trim)
        if(!is.na(vec_address[1])){
          normalized_street <- street_normalization(vec_address)
        } else {
          normalized_street <- NA
        }
        dt_road <- data.table(id = dt_road$id, normalized_street)
        #   dt_roads_normal <- rbindlist(list(dt_roads_normal, dt_road),use.names = TRUE, fill=TRUE)
      }
    }
  l_roads_normalized <- my_fcn(i_road_ids, dt_roads)
  dt_roads_normalized <- rbindlist(l_roads_normalized, use.names=TRUE, fill=TRUE)
  setkey(dt_roads, id)
  setkey(dt_roads_normalized, id)
  dt_roads <- dt_roads_normalized[dt_roads]
  sf_roads <- st_sf(dt_roads)
  saveRDS(sf_roads, roads_location)
} else {
  sf_roads <- readRDS(roads_location)
}
return(sf_roads)
}
