get_pilot_streetview_metadata <- function(sf_roads_pilot_tiger){
  pilot_streetview_metadata_location <- 'data/clean/pilot_streetview_metadata.rds'
  if(!file.exists(pilot_streetview_metadata_location)){
    n_roads <- length(sf_roads_pilot_tiger$OBJECTID)
    for(i_road in 1:n_roads){
      sf_road <- sf_roads_pilot_tiger[i_road,]
      object_id <- paste0(sf_road$OBJECTID,'_',i_road)
      n_points = as.numeric(floor(st_length(sf_road)/5))
      sf_road_segment <- st_segmentize(sf_road, dfMaxLength =0.0001)
      dt_samples <- as.data.table(st_coordinates(st_sample(x = sf_road_segment, n_points, type='regular')))
      setnames(dt_samples, names(dt_samples), c('lon', 'lat','n'))
      dt_samples$ID <- 1:nrow(dt_samples)
      dt_samples <- dt_samples[, lon_lead:=data.table::shift(lon, n=1, type='lead')]
      dt_samples <- dt_samples[, lat_lead:=data.table::shift(lat, n=1, type='lead')]
      dt_samples <- dt_samples[, bearing:=geosphere::bearing(cbind(lon, lat))]
      dt_samples <- dt_samples[bearing<0, bearing:=bearing + 360]
      dt_samples <- dt_samples[!is.na(lon_lead)]
      myPalette <- colorRampPalette(rev(brewer.pal(11, "Spectral")))
      ggplot(dt_samples, aes(x=lon, y=lat, colour=bearing)) + 
        geom_point() +
        scale_colour_gradientn(colours = myPalette(100))
      cam_degrees <- c(45, 135, 225, 315)
      for(i_sample in 1:nrow(dt_samples)){
        cat(i_sample, 'of', nrow(dt_samples), '\n')
        #for(cam_degree in cam_degrees){  
        dt <- dt_samples[i_sample]
        ID <- dt$ID; lat <- dt$lat; long <- dt$lon
        api <- list()
        api[[1]] <- 'https://maps.googleapis.com/maps/api/streetview/metadata?size=600x300&location='
        api[[2]] <- paste0(lat, ',', long, '&')
        api[[3]] <- paste0('key=', google_key)
        api.url <- paste0(unlist(api), collapse = '')
        panorama <- try(unlist(rjson::fromJSON(file=api.url)))
        dt.panoid <- try(data.table::data.table(roads.point.id = ID, t(panorama)))
        dt.panoid$actual_lat <- lat
        dt.panoid$actual_long <- long
        dt.panoid$bearing <- dt$bearing
        dt.panoid$OBJECTID <- object_id
        cat(i_sample, dt.panoid$status, '\n')
        # Save the road pano match
        save.file <- paste0('~/Documents/Github/atlanta-demo/data/panoids/', 'objectid-', object_id, '---samplepoint-', i_sample, '.rds')
        saveRDS(dt.panoid, file=save.file)
        # }
      }
    }
    # Bind all of the panoids
    l_panoids <- list.files('~/Documents/Github/atlanta-demo/data/panoids/', full.names = TRUE)
    f_panoids <- lapply(l_panoids, readRDS)
    dt_panoids <- rbindlist(f_panoids, use.names = TRUE, fill=TRUE)
    dt_panoids <- dt_panoids[!is.na(pano_id)]
    dt_panoids <- dt_panoids[, pano_id_count:=.N, by=pano_id]
    table(dt_panoids[, .(OBJECTID, pano_id_count)])
    dt_panoids <- dt_panoids[,.SD[1], by=.(pano_id)]
    dt_panoids$metadata_id <- 1:nrow(dt_panoids)
    saveRDS(dt_panoids, pilot_streetview_metadata_location)
  } else {
    dt_panoids <- readRDS(pilot_streetview_metadata_location)
  }
  return(dt_panoids)
}
