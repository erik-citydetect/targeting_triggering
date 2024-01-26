# https://cran.r-project.org/web/packages/osmdata/vignettes/osmdata.html
get_osmdata <- function(){
  if(!file.exists(osmdata_location)){
    set_overpass_url(get_overpass_url())
    q <- opq(bbox = city_name) %>%
      add_osm_feature(key = 'name') %>%
      osmdata_xml(filename = 'data/raw/osmdata.osm')
    sf_osmdata <- osmdata_sf(doc='data/raw/osmdata.osm')
    # Intersect with 
    
    saveRDS(sf_osmdata, osmdata_location)
  } else {
    sf_osmdata <- readRDS(osmdata_location)
  }
  return(sf_osmdata)
}