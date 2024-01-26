get_targets <- function(dt_parcels, dt_footprints){
  if(!file.exists(clean_targets_location)){
    # dt_footprints <- get_footprints()
    # dt_footprints <- dt_footprints[, .SD[which.max(foot_area)], by=parcel_shape_id]
    # setkey(dt_footprints, parcel_shape_id)
    # 
    # l_targets <- list()
    # list_iter <- 1
    # dt_parcels_ref<- copy(dt_parcels)
    # for(i_iter in 1:10){
    #   cat(i_iter, ':', nrow(dt_parcels_ref), '\n')
    #   # 1) Parcels with i) unique parcel shape, ii) a unique address
    #   # 1a)  no footprint
    #   dt_parcels_sub <- dt_parcels_ref[n_parcel_shapes==1]
    #   setkey(dt_parcels_sub, parcel_shape_id)
    #   setkey(dt_footprints, parcel_shape_id)
    #   dt_target <- dt_footprints[dt_parcels_sub]
    #   dt_target <- dt_target[is.na(foot_row)]
    #   if(nrow(dt_target)>0){
    #     geos_sf <- as_geos_geometry(st_as_sf(dt_target$geometry))
    #     target_lon <- geos_extent(geos_centroid(geos_sf))[1]
    #     target_lat <- geos_extent(geos_centroid(geos_sf))[2]
    #     dt_target <- data.table(parcel_row=dt_target$parcel_row,
    #                             parcel_id=dt_target$parcel_id,
    #                             parcel_shape_id=dt_target$parcel_shape_id,
    #                             target_lon=target_lon$xmin,
    #                             target_lat=target_lat$ymin,
    #                             target_type='parcel_centroid',
    #                             target_address = dt_target$address
    #                             #target_address_street=dt_target$address_street,
    #                             #target_address_type=dt_target$address_type,
    #                             #target_address_dir=dt_target$address_dir
    #     )
    #     list_iter <- list_iter + 1
    #     l_targets[[list_iter]] <- dt_target
    #   }
    #   # 1b) with footprint
    #   dt_targets <- rbindlist(l_targets, use.names = TRUE, fill=TRUE)
    #   parcel_rows_assigned <- dt_targets$parcel_row
    #   dt_parcels_sub <- dt_parcels_ref[!(parcel_row %in% parcel_rows_assigned)]
    #   dt_parcels_sub <- dt_parcels_sub[n_parcel_shapes==1]
    #   setkey(dt_parcels_sub, parcel_shape_id)
    #   setkey(dt_footprints, parcel_shape_id)
    #   dt_target <- dt_footprints[dt_parcels_sub]
    #   dt_target <- dt_target[!is.na(foot_lon)]
    #   if(nrow(dt_target)>0){
    #     dt_target <- data.table(parcel_row=dt_target$parcel_row,
    #                             parcel_id=dt_target$parcel_id,
    #                             parcel_shape_id=dt_target$parcel_shape_id,
    #                             target_lon=dt_target$foot_lon,
    #                             target_lat=dt_target$foot_lat,
    #                             target_address=dt_target$address,
    #                             target_type='footprint_centroid'
    #                             # target_address_street=dt_target$address_street,
    #                             # target_address_type=dt_target$address_type,
    #                             # target_address_dir=dt_target$address_dir
    #     )
    #     list_iter <- list_iter+1
    #     l_targets[[list_iter]] <- dt_target
    #   }
    #   # 2) Multiple addresses for id (use geocodes)
    #   # dt_targets <- rbindlist(l_targets, use.names = TRUE, fill=TRUE)
    #   # parcel_rows_assigned <- dt_targets$parcel_row
    #   # dt_parcels_sub <- dt_parcels_ref[!(parcel_row %in% parcel_rows_assigned)]
    #   # dt_target <- dt_parcels_sub[geocode_type %in% c('ROOFTOP', 'GEOMETRIC_CENTER')]
    #   # if(nrow(dt_target)>0){
    #   #   dt_target <- data.table(parcel_row=dt_target$parcel_row,
    #   #                           parcel_id=dt_target$parcel_id,
    #   #                           parcel_shape_id=dt_target$parcel_shape_id,
    #   #                           target_lon=dt_target$geocode_lon,
    #   #                           target_lat=dt_target$geocode_lat,
    #   #                           target_address=dt_target$address,
    #   #                           #target_address_street=dt_target$address_street,
    #   #                           #target_address_type=dt_target$address_type,
    #   #                           #target_address_dir=dt_target$address_dir,
    #   #                           target_type='footprint_centroid')
    #   #   list_iter <- list_iter + 1
    #   #   l_targets[[list_iter]] <- dt_target
    #   # }
    #   # 3) The rest of them
    #   dt_targets <- rbindlist(l_targets, use.names = TRUE, fill=TRUE)
    #   parcel_rows_assigned <- dt_targets$parcel_row
    #   dt_parcels_sub <- dt_parcels_ref[!(parcel_row %in% parcel_rows_assigned)]
    #   dt_parcels_sub <- dt_parcels_sub[,n_parcel_shapes:=.N,
    #                                    by=parcel_shape_id]
    #   dt_parcels_sub <- dt_parcels_sub[,n_parcel_addresses:=.N, by=address]
    #   dt_parcels_ref <- copy(dt_parcels_sub)
    #   # }
    #   dt_parcels_sub <- copy(dt_parcels_ref)
    #   setkey(dt_parcels_sub, parcel_shape_id)
    #   setkey(dt_footprints, parcel_shape_id)
    #   dt_target <- dt_footprints[dt_parcels_sub]
    #   dt_target <- dt_target[is.na(foot_row)]
    #   if(nrow(dt_target)>0){
    #     geos_sf <- as_geos_geometry(st_as_sf(dt_target$geometry))
    #     target_lon <- geos_extent(geos_centroid(geos_sf))[1]
    #     target_lat <- geos_extent(geos_centroid(geos_sf))[2]
    #     dt_target <- data.table(parcel_row=dt_target$parcel_row,
    #                             parcel_id=dt_target$parcel_id,
    #                             parcel_shape_id=dt_target$parcel_shape_id,
    #                             target_lon=target_lon$xmin,
    #                             target_lat=target_lat$ymin,
    #                             target_address=dt_target$address,
    #                             target_type='parcel_centroid_leftover'
    #                             # target_address_street=dt_target$address_street,
    #                             # target_address_type=dt_target$address_type,
    #                             # target_address_dir=dt_target$address_dir
    #     )
    #     list_iter <- list_iter + 1
    #     l_targets[[list_iter]] <- dt_target
    #   }
    # }
    # dt_targets <- rbindlist(l_targets, use.names = TRUE, fill=TRUE)
    # dt_targets <- dt_targets[, target_id:=.GRP, by=.(target_lon,
    #                                                  target_lat,
    #                                                  target_address
    #                                                  #target_address_street,
    #                                                  #target_address_type,
    #                                                  #target_address_dir
    #                                                  )]
    # #saveRDS(dt_targets, 'data/clean/dt_parcel_targets.rds')
    # #dt_targets <- dt_targets[,.SD[1],by=target_id]
    # #dt_targets <- dt_targets[, target_street_names_modified:=str_squish(clean_addresses(target_address_street, abbrev))]
    # dt_targets <- dt_targets[!(parcel_id %in% '-----.')]
    # 

    dt_targets <- copy(dt_parcels)
    dt_targets$target_id <- 1:nrow(dt_targets)
    setnames(dt_targets, 'physical_address', 'target_address')
    setnames(dt_targets, 'ParcelID', 'parcel_id')
    i_target_ids <- 1:nrow(dt_targets)
    
    plan(multisession)
    handlers(global = TRUE)
    #handlers(c("progress", 'rstudio'))
    handlers('rstudio',handler_pbcol(
      adjust = 1.0,
      complete = function(s) cli::bg_red(cli::col_black(s)),
      incomplete = function(s) cli::bg_cyan(cli::col_black(s))
    ))
    
    my_fcn <- function(i_target_ids, dt_targets){
      pb <- progressor(along = i_target_ids)
      
      foreach(i=i_target_ids, .options.future = list(seed = TRUE)) %dofuture% {
        
        pb(sprintf("i=%g", i))
       # cat(i , '\n')
        dt_target <- dt_targets[i]
        vec_address <- sapply(unlist(str_split(dt_target$target_address, ' ')), str_trim)
        if(!is.na(vec_address[1])){
          normalized_street <- try(street_normalization(vec_address))
        } else {
          normalized_street <- NA
        }
        dt_target_normalized <- data.table(parcel_id = dt_target$parcel_id, normalized_street)
        #   dt_roads_normal <- rbindlist(list(dt_roads_normal, dt_road),use.names = TRUE, fill=TRUE)
      }
    }
    l_targets_normalized <- my_fcn(i_target_ids, dt_targets)
    dt_targets_normalized <- rbindlist(l_targets_normalized, use.names=TRUE, fill=TRUE)
    setkey(dt_targets, parcel_id)
    setkey(dt_targets_normalized, parcel_id)
    dt_targets <- dt_targets_normalized[dt_targets]
    saveRDS(dt_targets, clean_targets_location)
  } else {
    dt_targets <- readRDS(clean_targets_location)
  }
  return(dt_targets)
}
