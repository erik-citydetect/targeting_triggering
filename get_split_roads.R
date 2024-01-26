get_split_roads <- function(split_distance_m=100){
  split_roads_location <- 'data/clean/dt_split_roads.rds'
  if(!file.exists(roads_breaks_long_location)){
    source('R/split_lines.R')
    #sf_roads <- readRDS('data/clean/roads.rds')
    roads <- sf::st_read('data/raw/tl_2021_01073_roads/', layer = "tl_2021_01073_roads")
    roads$id <- roads$LINEARID
    roads$FULLNAME <- stringr::str_to_upper(roads$FULLNAME)
    roads$FULLNAME <- stringr::str_replace_all(roads$FULLNAME, 'STATE RTE', 'HWY')
    roads <- as.data.table(roads)
    roads <- roads[, road_length_m:=as.double(sf::st_length(geometry))]
    roads_short <- roads[road_length_m<split_distance_m]
    roads_short <- roads_short[, .(id, split_fID=id, geometry)]
    roads_long <- roads[road_length_m>split_distance_m]
    roads_long <- st_as_sf(roads, agr = 'unique')
    sf_roads_breaks_long <- split_lines(roads_long, split_distance_m, id='id')
    roads_breaks_long <- as.data.table(sf_roads_breaks_long)
    roads_combined <- rbindlist(list(roads_short, roads_breaks_long),
                                use.names=TRUE,
                                fill=TRUE)
    roads_combined$length_m <- as.numeric(sf::st_length(roads_combined$geometry))
    dt_split_roads <- roads_combined
    saveRDS(dt_split_roads, split_roads_location)
  } else {
    dt_split_roads <- readRDS(split_roads_location)
  }
  return(dt_split_roads)
}