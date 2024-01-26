dt_trigger_targets <- readRDS('data/clean/dt_trigger_targets.rds')
sf_parcels <- readRDS('data/clean/sf_parcels.rds')
dt_parcels <- as.data.table(sf_parcels)
dt_parcels <- dt_parcels[, geo_geom:=geos::as_geos_geometry(geometry)]
setkey(dt_parcels, parcel_row)
setkey(dt_trigger_targets, parcel_row)
dt_parcels <- dt_parcels[dt_trigger_targets]

# Get the labeled image image list
session <- ssh_connect("jonathanrichardson@100.104.32.67")
ssh::scp_download(session, '/home/jonathanrichardson/dt_frame_target_2023_05_18.rds', 
                  paste0('~/Documents/Github/lambda_functions_video/data/clean/'))
dt_frame_target <- readRDS('~/Documents/Github/lambda_functions_video/data/clean/dt_frame_target_2023_05_18.rds')
dt_frame_target$geometry <- NULL
dt_frame_target$i.geometry <- NULL
dt_frame_target$i.address <- NULL
setkey(dt_frame_target, target_id)
setkey(dt_parcels, target_id)

# Drop redundant columns
dt_parcels_targets <- dt_frame_target[dt_parcels]
drop_cols <- which(str_detect(names(dt_parcels_targets),'i\\.'))
names_drop <- names(dt_parcels_targets)[drop_cols]
names_keep <- names(dt_parcels_targets)[!(names(dt_parcels_targets) %in% names_drop)]
dt_parcels_targets <- dt_parcels_targets[, names_keep, with=FALSE]
dt_parcels_targets <- dt_parcels_targets[, labeled:=0]
dt_parcels_targets <- dt_parcels_targets[!is.na(frame), labeled:=1]
dt_parcels_targets$geometry <- NULL
dt_parcels_targets <- dt_parcels_targets[!is.na(geo_geom)]
dt_parcels_targets <- dt_parcels_targets[, geometry:=st_as_sf(geo_geom)]
dt_parcels_targets$geo_geom <- NULL
sf_parcels_targets <- st_as_sf(dt_parcels_targets, agr = 'unique')
sf_parcels_targets <- st_cast(sf_parcels_targets, 'MULTIPOLYGON')
st_write(sf_parcels_targets,'maps/parcels_targets.geojson', 'parcels_targets')

# Subset images on community boundaries
ssh::scp_download(session, '/home/jonathanrichardson/dt_gpxs.rds', 
                  paste0('~/Documents/Github/lambda_functions_video/data/clean/'))
dt_gpxs <- readRDS('~/Documents/Github/lambda_functions_video/data/clean/dt_gpxs.rds')
sf_gpxs <- st_as_sf(dt_gpxs, coords=c('lon', 'lat'), crs=4326, agr='constant')
saveRDS(sf_gpxs, 'data/analysis/sf_gpxs.rds')
st_write(sf_gpxs,'maps/gpxs.geojson', 'gpxs')


sf_gpxs_2022 <- st_as_sf(dt_gpxs_2022, coords=c('lon', 'lat'), crs=4326, agr='constant')
st_write(sf_gpxs_2022,'maps/gpxs_2022.geojson', 'gpxs')

sf_gpxs_2023 <- st_as_sf(dt_gpxs_2023, coords=c('lon', 'lat'), crs=4326, agr='constant')
st_write(sf_gpxs_2023,'maps/gpxs_2023.geojson', 'gpxs')

# Get Birmingham community boundaries
sf_bham <- st_read('maps/community_area.shp')
sf_bham <-  st_transform(sf_bham, st_crs(proj_WGS84))
sf_bham <- st_make_valid(sf_bham)
st_write(sf_bham, 'maps/bham.geojson', 'bham')

# Subset GPXs
sfc_bham_bbox <- st_as_sfc(st_bbox(sf_bham))
sf_bham_bbox <- st_as_sf(sfc_bham_bbox)
sf_gpxs_bham <- st_as_sf(st_intersection(sf_bham_bbox, sf_gpxs))
saveRDS(sf_gpxs_bham, 'data/analysis/sf_gpxs_bham.rds')
st_write(sf_gpxs_bham,'maps/gpxs_bham.geojson', 'gpxs_bham')

# Subset parcel targets
sf_parcels_targets_bham <- st_intersection(sf_bham, sf_parcels_targets)
st_write(sf_parcels_targets_bham, 'maps/parcels_targets_bham.geojson', 'parcels_target_bham')
dt_parcels_targets_bham <- as.data.table(sf_parcels_targets_bham)
saveRDS(sf_parcels_targets_bham, 'data/analysis/sf_parcels_targets_bham.rds')
