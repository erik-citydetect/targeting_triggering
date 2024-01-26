get_city <- function(refresh=FALSE){
  
  if(!file.exists(clean_city_location)|refresh==TRUE){
    sf_city <- st_read(raw_city_location)
    sf_city <- ensure_multipolygons(sf_city)
    # sf_parcels <- sf_parcels[sf_parcels$Property_C %in% study_city,]
    saveRDS(st_crs(sf_city)$proj4string, proj_orig_location)
    sf_city <- st_transform(sf_city, st_crs(proj_WGS84))
    saveRDS(sf_city, clean_city_location)
  } else {
    sf_city <- readRDS(clean_city_location)
  }
  return(sf_city)
}