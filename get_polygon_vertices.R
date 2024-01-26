get_polygon_vertices <- function(target, parcels, nearest_road_84){
  # Find vertices of parcel polygonspTransform(targets_proj_new, CRS(proj_orig))
  sf_multipolygon <- parcels[parcels$PARCELID %in% target$PARCELID,]
  sp_polygon <- as(sf_multipolygon, 'Spatial')
  sf_vertices <- as.data.table(spatialEco::extract.vertices(sp_polygon))
  sf_vertices$index <- 1:nrow(sf_vertices)
  l_vert <- list()
  l_vert$xmin <- sf_vertices[which.min(X)][1]
  sf_vertices <- sf_vertices[!(index %in% l_vert$xmin$index)]
  l_vert$xmax <- sf_vertices[which.max(X)][1]
  sf_vertices <- sf_vertices[!(index %in% l_vert$xmax$index)]
  l_vert$ymin <- sf_vertices[which.min(Y)][1]
  sf_vertices <- sf_vertices[!(index %in% l_vert$ymin$index)]
  l_vert$ymax <- sf_vertices[which.max(Y)][1]
  sf_vertices <- sf_vertices[!(index %in% l_vert$yax$index)]
  
  dt_vertices <- rbindlist(l_vert, use.names = TRUE, fill=TRUE)
  sf_vertices = st_as_sf(dt_vertices, coords = c("X", "Y"), 
                         crs = CRS(proj_wgs84))
  sf_vertices$dist2road <- 10000
  l_vert <- vector('list', length=length(sf_vertices))
  for(i_vertex in 1:length(sf_vertices)){
    nearest_distance <- geosphere::dist2Line(st_coordinates(sf_vertices[i_vertex,]), st_coordinates(nearest_road_84)[,1:2])
    nearest_distance <- as.numeric(nearest_distance[1,1])
    vert_index <- sf_vertices[i_vertex,]$index
    l_vert[[i_vertex]] <- as.data.table(sf_vertices[i_vertex,])
    l_vert[[i_vertex]]$dist2road <- nearest_distance
  }
  dt_vert <- rbindlist(l_vert,use.names = TRUE, fill = TRUE)
  dt_vert <- dt_vert[order(dist2road)][1:2]
  return(dt_vert)
}