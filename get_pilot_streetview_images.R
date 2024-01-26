get_pilot_streetview_images <- function(sf_roads_pilot_city){
  pilot_streetview_images_location <- 'data/clean/pilot_streetview_images/'
  dt_metadatas <- get_pilot_streetview_metadata(sf_roads_pilot_city)
  metadata_ids <- dt_metadatas$metadata_id
  #metadata_ids <- metadata_ids[metadata_ids>metadata_ids[1340]]
  headings <- c(45, 135, 225, 315)
  i <- 0
  for(id in metadata_ids){
    i <- i + 1 
    cat(i, 'of', length(metadata_ids), '\n')
    dt_metadata <- dt_metadatas[metadata_id %in% id]
    lat <- dt_metadata$actual_lat
    lon <- dt_metadata$actual_lon
    bearing <- dt_metadata$bearing
    for(heading in headings){
      camera_bearing <- bearing + heading
      dest_file <- paste0(pilot_streetview_images_location, 'metadataID:', id, '---lat:', lat, '---lon:', lon, '---cameraBearing:', camera_bearing, '.jpg')
      if(!(file.exists(dest_file))){
        if(camera_bearing > 360) camera_bearing <- camera_bearing-360
        api <- list()
        api[[1]] <- 'https://maps.googleapis.com/maps/api/streetview?size=640x640&location='
        api[[2]] <- paste0(lat, ',', lon, '&')
        api[[3]] <- paste0('&fov=80&heading=', camera_bearing)
        api[[4]] <- paste0('&pitch=0&key=', google_key)
        api.url <- paste0(unlist(api), collapse = '')
        download.file(api.url, dest_file)
      }
    }
  }
}