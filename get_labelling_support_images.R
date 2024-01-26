get_labeling_support_images <- function(start_row, end_row){
  library(data.table)
  library(ggplot2)
  library(stringr)
  library(ggmap)
  library(aws.s3)
  
  #load('~/Dropbox/pkg.data/api.keys/raw/l.pkg.rdata')
  #API_key <- 'AIzaSyDZ-BwQR19TaPoQPxPn_DMveYHvlHo6G1k'
  API_key <- 'AIzaSyC9xlFRDhuPASNDmmA4boAvt5G6uGNPcDk'
  ggmap::register_google('AIzaSyC9xlFRDhuPASNDmmA4boAvt5G6uGNPcDk')
  
  s3_bucket_name <- 'citydetect--upload'
  city_state <- 'columbia_sc'
  dt_parcels <- get_parcels()
  
  dt_parcels <- dt_parcels[, .(parcel_row, parcel_shape_id, parcel_id, address, geometry=geom)]
  
  # dt_parcels <- dt_parcels[, g_address := paste0(ADDR_PSPR, ', ', Property_C, ', ', Property_S)]
  dt_targets <- get_targets()
  dt_targets <- dt_targets[,.(target_id, parcel_row, target_type, target_lon, target_lat)]
  setkey(dt_parcels, parcel_row)
  setkey(dt_targets, parcel_row)
  dt_parcel_targets <- dt_parcels[dt_targets]
  
  # dt_parcel_shapes <- readRDS('~/Documents/Github/lambda_functions_video/data/dt_parcel_shapes.rds')
  # dt_parcel_shapes <- dt_parcel_shapes[,.(parcel_shape_id, geometry)]
  # setkey(dt_parcel_shapes, parcel_shape_id)
  # setkey(dt_parcel_targets, parcel_shape_id)
  # dt_parcel_targets <- dt_parcel_shapes[dt_parcel_targets]
  
  # dt_frames_to_targets <- readRDS('data/dt_frames_to_targets.rds')
  # setkey(dt_frames_to_targets, target_id)
  # setkey(dt_parcel_targets, target_id)
  # dt_parcel_targets <- dt_parcel_targets[dt_frames_to_targets]
  
  # target_data/[city]_[state]/[target_id]_[type]
  dt_parcel_targets <- dt_parcel_targets[, image_s3_Key_streetview:=paste0('target_data/',city_state,'/',target_id,'---streetview.jpg')]
  dt_parcel_targets <- dt_parcel_targets[, image_s3_Key_aerial:=paste0('target_data/',city_state,'/',target_id,'---aerial.jpg')]
  dt_parcel_targets <- dt_parcel_targets[, image_url_streetview:=paste0('https://city-detect--upload.s3.amazonaws.com',image_s3_Key_streetview)]
  dt_parcel_targets <- dt_parcel_targets[,image_url_aerial:=paste0('https://city-detect--upload.s3.amazonaws.com',image_s3_Key_aerial)]
  dt_parcel_targets <- dt_parcel_targets[,image_s3_streetview:=paste0('s3://city-detect--upload/',image_s3_Key_streetview)]
  dt_parcel_targets <- dt_parcel_targets[,image_s3_aerial:=paste0('s3://city-detect--upload/',image_s3_Key_aerial)]
  dt_triggers <- get_triggers()
  dt_triggers <- dt_triggers[, .(target_id, trigger_lon, trigger_lat)]
  setkey(dt_triggers, target_id)
  setkey(dt_parcel_targets, target_id)
  dt_parcel_targets <- dt_triggers[dt_parcel_targets]
  
  f_time_old<-''
  f_time_aerial_old<-''
  end_row <- min(end_row, nrow(dt_parcel_targets))
  for(i_target in start_row:end_row){
    cat(i_target, 'of', nrow(dt_parcel_targets), '\n')
    dt_image <- dt_parcel_targets[i_target]
    sf_image <- st_cast(st_as_sf(dt_image))
    #target_id <- dt_image$target_ids
    # sv_path <- dt_image$image_url_streetview
    # aerial_path <- paste0('data/truth/aerial/', target_id, '.png')
    if(!(suppressMessages(aws.s3::object_exists(dt_image$image_s3_streetview)))){
      gAddress <- paste(dt_image$address, 'Columbia, SC')
      shotLoc <- paste0('https://maps.googleapis.com/maps/api/streetview?size=600x600&location=',
                        gAddress, '&key=', API_key)
      shotLoc <- URLencode(shotLoc)
      if(file.exists(paste0(dt_image$target_id,'_sv_temp.jpg'))){
        cat('Streetview file already exists, pushing locally cached \n')
        aws.s3::put_object(paste0(dt_image$target_id,'_sv_temp.jpg'), object = dt_image$image_s3_Key_streetview, bucket = 'city-detect--upload')
        file.remove(paste0(dt_image$target_id,'_sv_temp.jpg'))
      } else {
        try(download.file(shotLoc, destfile=paste0(dt_image$target_id,'_sv_temp.jpg')))
        cat('AWS PUT OBJECT', '\n')
        try(aws.s3::put_object(paste0(dt_image$target_id,'_sv_temp.jpg'), object = dt_image$image_s3_Key_streetview, bucket = 'city-detect--upload'))
        file.remove(paste0(dt_image$target_id,'_sv_temp.jpg'))
      }
    }
    if(!(aws.s3::object_exists(dt_image$image_s3_aerial))){
      cat('New aerial', '\n')
      image_coords <- suppressWarnings(st_coordinates(st_centroid(sf_image)))
      p_out <- try(ggmap(get_googlemap(center = as.numeric(image_coords),
                                       zoom = 20, scale = 2,
                                       maptype ='satellite',
                                       color = 'color')) +
                     geom_sf(data=sf_image,fill=alpha("red",0.2), inherit.aes=FALSE) +
                     geom_point(data=sf_image, aes(x=target_lon, y=target_lat), color='yellow', size=6, shape=10) +
                     #      geom_point(data=sf_image, aes(x=camera_lon, y=camera_lat), color='black', fill='blue',size=4, shape=23) +
                     geom_point(data=sf_image, aes(x=trigger_lon, y=trigger_lat), color='green', fill='green',size=4, shape=23) +
                     theme_void())
      ggsave(paste0(dt_image$target_id,'_aerial_temp.jpg'), p_out, width=4, height=4, units='in')
      try(aws.s3::put_object(paste0(dt_image$target_id,'_aerial_temp.jpg'), object = dt_image$image_s3_Key_aerial, bucket = 'city-detect--upload'))
      file.remove(paste0(dt_image$target_id,'_aerial_temp.jpg'))
    }
  }
}
