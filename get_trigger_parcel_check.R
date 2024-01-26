get_trigger_parcel_check <- function(nearest_road, dt_target){
  
  ggmap(get_googlemap(center = c(lon = dt_target$target_lon,
                                lat = dt_target$target_lat),
                     zoom = 17, scale = 2,
                      maptype ='satellite',
                     color = 'color')) +
  #  geom_sf(data=sf_target_roads, color='red', size=10, inherit.aes=FALSE) +
  #  geom_point(data=target_extent, aes(x=target_lon, y=target_lat), color='yellow', size=6, shape=10)
  ggplot2::ggplot(dt_target, aes(x=target_lon, y=target_lat)) +
    geom_point(aes(x=target_lon, y=target_lat),alpha=0.8, fill='gray', col='red', size=7, stroke=1, shape=13, inherit.aes=FALSE) + 
    geom_line(nearest_road)
}