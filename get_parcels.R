get_parcels <- function(refresh=FALSE){
  
  if(!file.exists(clean_parcels_location)|refresh==TRUE){
    
    sf_parcels <- st_read(raw_parcels_location)
    sf_parcels <- ensure_multipolygons(sf_parcels)
    # sf_parcels <- sf_parcels[sf_parcels$Property_C %in% study_city,]
    saveRDS(st_crs(sf_parcels)$proj4string, proj_orig_location)
    sf_parcels <- st_transform(sf_parcels, st_crs(proj_WGS84))

    ## Intersect with city boundary
    sf_city <- get_city()
    
    sf_parcels  <- st_intersection(sf_parcels, sf_city)
    ## START EDIT HERE
    setnames(sf_parcels, 'APN_CHR', 'ParcelID')
    sf_parcels$geometry <- sf_parcels$geom
    sf_parcels$SHAPE_AREA <- NULL
    sf_parcels$SHAPE_LEN <- NULL
    sf_parcels$POLY_CODE <- NULL
    sf_parcels$ACC_CODE <- NULL
    sf_parcels$APN <- NULL
    ## END EDIT HERE
    
    sf_parcels <- st_make_valid(sf_parcels)
    sf_parcels$geo <- geos::as_geos_geometry(sf_parcels$geom)
    sf_parcels$parcel_row <- 1:nrow(sf_parcels)
    sf_parcels$TMS <- NULL
   # l_parcels <- vector('list', nrow(sf_parcels))
   # for(i_parcel in 1:nrow(sf_parcels)){
   #   sf_parcel <- sf_parcels[i_parcel,]
   # }
    # Unique parcel geometries
    
    
    # Drop parcels with redundant geometry
    bucket_size <- 10
    start_ind <- seq(1, nrow(sf_parcels), bucket_size-3)
    end_ind <- c(seq(bucket_size, nrow(sf_parcels), bucket_size-3), nrow(sf_parcels))
    dt_chunks <- data.table(start_ind=start_ind,
                            end_ind=end_ind)
    l_chunks <- vector('list', length=nrow(dt_chunks))
    #myCluster <- parallel::makeForkCluster(getOption("mc.cores", detectCores()-2))
    #doParallel::registerDoParallel(myCluster)
    #with_progress({
    #  p <- progressor(along=1:nrow(dt_chunks))
    #  l_chunks <- f <- <- oreach(i_address=1:nrow(dt_chunks)) %dopar% {
    for(i_chunk in 1:nrow(dt_chunks)){
        if((i_chunk %% 50)==0) cat('chunk', i_chunk, 'of', nrow(dt_chunks),'\n')
        sf_parcels_chunk <- sf_parcels[dt_chunks[i_chunk]$start_ind:dt_chunks[i_chunk]$end_ind,]
        l_geometry_equals <- sf::st_equals(sf_parcels_chunk, sf_parcels_chunk)
        dt_geometry_equals <- rbindlist(lapply(l_geometry_equals, function(x) as.data.table(t(x))),
                                        use.name=TRUE, fill = TRUE)
        dt_geometry_equals <- unique(dt_geometry_equals)
        setnames(dt_geometry_equals, names(dt_geometry_equals)[1], 'chunk_parcel_ind')
        if(length(dt_geometry_equals)>1){
          setnames(dt_geometry_equals, names(dt_geometry_equals)[2:length(names(dt_geometry_equals))],
                   paste0('match_',1:(length(names(dt_geometry_equals))-1)))
          dt_geometry_equals <- melt(dt_geometry_equals, id.vars=c('chunk_parcel_ind'),
                                     measure.vars=names(dt_geometry_equals)[str_detect(names(dt_geometry_equals),'match')])
          dt_geometry_equals$parcel_row_match <- sf_parcels_chunk[dt_geometry_equals$value,]$parcel_row 
          dt_geometry_equals$chunk_ind <- i_chunk
          dt_geometry_equals$parcel_row_source <- sf_parcels_chunk[dt_geometry_equals$chunk_parcel_ind,]$parcel_row
          dt_geometry_equals <- dt_geometry_equals[,.(chunk_parcel_ind,
                                                      parcel_row_source,
                                                      parcel_row_match)]
        } else {
          dt_geometry_equals$chunk_ind <- i_chunk
          dt_geometry_equals$parcel_row_source <- sf_parcels_chunk[dt_geometry_equals$chunk_parcel_ind,]$parcel_row
          dt_geometry_equals <- dt_geometry_equals[,.(chunk_parcel_ind,
                                                      parcel_row_source)]
        }
        l_chunks[[i_chunk]] <- dt_geometry_equals
        #dt_geometry_equals
    }
    # Back up chunks so do not have to restart process if next portion fails.
    tmp_chunks <- tempfile(fileext = ".rds")
    saveRDS(l_chunks, tmp_chunks)
    l_chunks <- readRDS(tmp_chunks)
    
    # Process matches
    dt_matches <- rbindlist(l_chunks, use.names=TRUE, fill=TRUE)
    if(!('parcel_row_match' %in% names(dt_matches))) dt_matches$parcel_row_match <- 0
    dt_matches <- dt_matches[is.na(parcel_row_match), parcel_row_match:=0]
    dt_matches <- dt_matches[,.SD[1],by=.(parcel_row_source, 
                                          parcel_row_match)][,.(parcel_row_source, 
                                                                parcel_row_match)]
    dt_parcels <- as.data.table(sf_parcels)
    dt_parcels <- dt_parcels[, .(parcel_row_source = parcel_row,
                                 parcel_id_source = ParcelID)]
    setkey(dt_matches, parcel_row_source)
    setkey(dt_parcels, parcel_row_source)
    dt_matches <- dt_parcels[dt_matches]
    
    dt_parcels <- dt_parcels[, .(parcel_row_match = parcel_row_source,
                                 parcel_id_match = parcel_id_source)]
    setkey(dt_matches, parcel_row_match)
    setkey(dt_parcels, parcel_row_match)
    dt_matches <- dt_parcels[dt_matches]
    
    dt_matches <- dt_matches[,.(row_source = parcel_row_source,
                                id_source = parcel_id_source,
                                row_match = parcel_row_match,
                                id_match = parcel_id_match)]
    
    # Iterate over parcels and create unique shape ids
    dt_matches_multi <- dt_matches[!(row_match %in% 0)]
    dt_matches_multi_flip <- dt_matches[, .(row_source=row_match,
                                                    id_source=id_match,
                                                    row_match=row_source,
                                                    id_match=id_source)]
    dt_matches_multi <- rbindlist(list(dt_matches_multi, dt_matches_multi_flip),
                                   use.names=TRUE, fill=TRUE)
    dt_matches_multi <- dt_matches_multi[!is.na(id_source)]
    #dt_matches_unique <- dt_matches[!(row_source %in% dt_matches_multi$row_source)]
    setorder(dt_matches_multi, row_source, row_match)
    l_matches_multi_to_parcel_shape_id <- vector('list', length(dt_matches_multi))
    parcel_shape_id <- 0
    while(nrow(dt_matches_multi)>0){
      parcel_shape_id <- parcel_shape_id + 1
      q_row_source <- dt_matches_multi[1]$row_source
      row_matches <- c(q_row_source,
                       dt_matches_multi[row_source %in% q_row_source]$row_match,
                       dt_matches_multi[row_match %in% q_row_source]$row_source)
      row_matches <- unique(row_matches)
      dt_row_matches <- data.table(parcel_row=row_matches,
                                   parcel_shape_id=parcel_shape_id)
      dt_matches_multi <- dt_matches_multi[!(row_source %in% row_matches)]
      l_matches_multi_to_parcel_shape_id[[parcel_shape_id]] <- dt_row_matches
      if((parcel_shape_id %% 10)==0){
        cat('Unique parcel_id:', parcel_shape_id, 'remaining:', nrow(dt_matches_multi), '\n')
      }
    }
    dt_matches_multi_to_parcel_shape_id <- rbindlist(l_matches_multi_to_parcel_shape_id,
                                              use.names = TRUE,
                                              fill = TRUE)
    tmp_dt_matches_multi_to_parcel_shape_id <-  tempfile(fileext = ".rds")
    saveRDS(dt_matches_multi_to_parcel_shape_id, tmp_dt_matches_multi_to_parcel_shape_id)
    dt_matches_multi_to_parcel_shape_id <- readRDS(tmp_dt_matches_multi_to_parcel_shape_id)
    max_parcel_shape_id <- max(dt_matches_multi_to_parcel_shape_id$parcel_shape_id)
    # Unique Matches
    dt_matches_unique_to_parcel_shape_id <- dt_matches[!(row_source %in% dt_matches_multi_to_parcel_shape_id$parcel_row)]
    dt_matches_unique_to_parcel_shape_id <- unique(dt_matches_unique_to_parcel_shape_id)
    nrow_dt_matches_unique <- nrow(unique(dt_matches_unique_to_parcel_shape_id))
    unique_shape_ids <- (max_parcel_shape_id+1):(nrow_dt_matches_unique+max_parcel_shape_id)
    dt_matches_unique_to_parcel_shape_id <- dt_matches_unique_to_parcel_shape_id[, .(parcel_row=row_source,
                                                                                     parcel_shape_id=unique_shape_ids)]
    dt_parcel_rows_to_parcel_shape_ids <- rbindlist(list(dt_matches_multi_to_parcel_shape_id,
                                                         dt_matches_unique_to_parcel_shape_id),
                                                    use.names = TRUE,
                                                    fill = TRUE)
    
    # Check for multifamily with the same parcel_id but different addresses
    dt_parcels <- copy(as.data.table(sf_parcels))
#    dt_parcels <- dt_parcels[is.na(APARTMENT), APARTMENT:='']
#    dt_parcels <- dt_parcels[is.na(ADDR_PSPR), ADDR_PSPR:='']
#    dt_parcels <- dt_parcels[is.na(Property_C), Property_C:='']
#    dt_parcels <- dt_parcels[is.na(Property_C), Property_C:='']
#    dt_parcels <- dt_parcels[, physical_address:=str_squish(paste(ADDR_PSPR,
#                                                                  APARTMENT,
#                                                                  Property_C,
#                                                                  Property_S))]
    
    # If no address
    dt_parcels <- saveRDS(dt_parcels, 'data/tmp_dt_parcels.rds')
    dt_parcels <- readRDS('data/tmp_dt_parcels.rds')
    dt_parcels$geo <- geos::as_geos_geometry(dt_parcels$geometry) 
      
    if(!('physical_address') %in% names(dt_parcels)){
      l_parcel_geocode <- vector('list', nrow(dt_parcels))
      for(i_parcel in 90000:nrow(dt_parcels)){
        dt <- NULL
        dt <- dt_parcels[i_parcel]
        coords <- st_coordinates(st_as_sf(geos::geos_centroid(dt$geo)))
        parcelID <- dt$ParcelID; lat <- dt$lat; long <- dt$lon
        parcel_address_location <- paste0('data/raw/parcel_address/', 'ParcelID-', dt$ParcelID, '.rds')
        if(!file.exists(parcel_address_location)){
        api <- list()
        api[[1]] <- 'https://maps.googleapis.com/maps/api/geocode/json?latlng='
        api[[2]] <- paste0(coords[2], ',', coords[1], '&')
        api[[3]] <- paste0('key=', google_key)
        api.url <- paste0(unlist(api), collapse = '')
        address <- try(unlist(rjson::fromJSON(file=api.url)))
        dt_geocode <- try(data.table::data.table(parcelID = dt$ParcelID, t(address)))
        # Save the road pano match
        saveRDS(dt_geocode, file=parcel_address_location)
        } else {
          dt_geocode <- readRDS(parcel_address_location)
        }
        if((i_parcel %% 10)==0) cat('i_parcel', i_parcel, 'of', nrow(dt_parcels), dt_geocode$status, '\n')
        l_parcel_geocode[[i_parcel]] <- dt_geocode
      }
        # Join address data to parcel data here
    }
    l_address_locations <- list.files('data/raw/parcel_address', full.names = TRUE)
    l_parcel_addresses <- lapply(l_address_locations, readRDS)
    dt_parcel_addresses <- rbindlist(l_parcel_addresses, use.names=TRUE, fill=TRUE)
    saveRDS(dt_parcel_addresses, 'data/raw/tmp_dt_parcel_addresses.rds')
    dt_parcel_addresses <- readRDS('data/raw/tmp_dt_parcel_addresses.rds')
    dt_parcel_addresses <- dt_parcel_addresses[,.(ParcelID=parcelID,
                                                  Address=results.formatted_address,
                                                  geocode_lat=results.geometry.location.lat,
                                                  geocode_lon=results.geometry.location.lng)]
    setkey(dt_parcel_addresses, ParcelID)
    setkey(dt_parcels, ParcelID)
    dt_parcels <- dt_parcel_addresses[dt_parcels]
    
    saveRDS(dt_parcels, 'data/tmp/dt_parcels.rds')
    
    dt_parcels <- dt_parcels[, physical_address:=str_squish(paste(Address))]
    setkey(dt_parcel_rows_to_parcel_shape_ids, parcel_row)
    setkey(dt_parcels, parcel_row)
    dt_parcels <- dt_parcel_rows_to_parcel_shape_ids[dt_parcels]
    dt_parcels <- dt_parcels[!is.na(parcel_row)]
    
    dt_parcel_shapes_addr <- dt_parcels
    setnames(dt_parcel_shapes_addr, 'Address', 'physical_address')
    dt_parcel_shapes_addr <- dt_parcel_shapes_addr[!(physical_address %in% '')]
    dt_parcel_shapes_addr <- dt_parcel_shapes_addr[,address_count:=.N, by=.(parcel_shape_id)]
    dt_parcel_shapes_addr <- dt_parcel_shapes_addr[address_count>1]
    setorder(dt_parcel_shapes_addr, parcel_row)
    q_addresses <- paste0(unique(dt_parcel_shapes_addr$physical_address), ',', city_name)
    saveRDS(q_addresses, 'data/clean/q_addresses.rds')
    if(!(dir.exists('data/tmp'))) dir.create('data/tmp')
    if(!(dir.exists('data/tmp/geocoding'))) dir.create('data/tmp/geocoding')
    myCluster <- parallel::makeForkCluster(getOption("mc.cores", detectCores()-2))
    doParallel::registerDoParallel(myCluster)
    with_progress({
      p <- progressor(along=1:length(q_addresses))
      l_geocodes <- foreach(i_address=1:length(q_addresses)) %dopar% {
        q_address <- q_addresses[i_address]
        geocode_cache_location <- paste0('data/tmp/geocoding/', q_address, '.rds')
        
        if(!file.exists(geocode_cache_location)){
          #cat('geocoding', i_address, 'of', length(q_addresses), '\n')
          l_geo <- ggmap::geocode(q_address, output='all')
          geocode_ok <- l_geo$status
          geocode_type <- l_geo$results[[1]]$geometry$location_type
          geocode_lat <- l_geo$results[[1]]$geometry$location$lat
          geocode_lon <- l_geo$results[[1]]$geometry$location$lng
          dt_geocode <- data.table(q_address, geocode_ok, geocode_type, geocode_lat, geocode_lon)
          #saveRDS(dt_geocode, geocode_cache_location)
        } else {
          dt_geocode <- readRDS(geocode_cache_location)
          #cat('skipping ', i_address, 'of', length(q_addresses), '\n')
        }
       # dt_geocode
      }
    })
    parallel::stopCluster(myCluster)
   # geocodes_location <- 'data/tmp/geocoding'
   # l_geocodes_locs <- list.files(geocodes_location, full.names = TRUE)
   # l_dt_geocodes <- lapply(l_geocodes_locs, readRDS)
    dt_geocodes <- rbindlist(l_geocodes,
                             use.names = TRUE,
                             fill=TRUE)
    dt_parcels <- dt_parcels[, q_address:=physical_address]
    setkey(dt_geocodes, q_address)
    setkey(dt_parcels, q_address)
    dt_parcels <- dt_geocodes[dt_parcels]
    dt_parcels <- dt_parcels[, .(parcel_id=ParcelID,
                                 parcel_shape_id,
                                 parcel_row,
                                 address=physical_address,
#                                 geocode_type,
#                                 geocode_lat,
#                                 geocode_lon,
                                 geometry=geom)]
    dt_parcels <- dt_parcels[, n_parcel_shapes:=.N,by=parcel_shape_id]
    dt_parcels <- dt_parcels[, n_address:=.N,by=physical_address]
    
    sf_parcels$Address <- NULL
    sf_parcels$ZoningDistrict <- NULL
    sf_parcels$Shape_Length <- NULL
    sf_parcels$Shape_Area <- NULL
    sf_parcels$geo <- NULL
    dt_parcels_geo <- as.data.table(sf_parcels)
    setkey(dt_parcels, parcel_row)
    setkey(dt_parcels_geo, parcel_row)
    dt_parcels <- dt_parcels_geo[dt_parcels]
    saveRDS(sf_parcels, 'data/clean/sf_parcels.rds')
    sf_polygon_points <- st_write(st_set_crs(st_as_sf(dt_parcels[, .(ParcelID, geocode_lat, geocode_lon)], coords = c("geocode_lon","geocode_lat")),
                                         st_crs(proj_WGS84)),'data/map/polgon_google_points.shp', append=FALSE)
    saveRDS(dt_parcels, clean_parcels_location)    
  } else {
    dt_parcels <- readRDS(clean_parcels_location)
    
  }
  return(dt_parcels)
}
