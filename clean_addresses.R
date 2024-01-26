clean_addresses <- function(vec_address){

  vec_address_base <- copy(vec_address)
  dt_address <- data.table(ind=1:length(vec_address),
                           value=str_to_upper(str_trim(vec_address)),
                           clean_value=vector('character', length=length(vec_address)),
                           potential_class=vector('character', length=length(vec_address)),
                           class=vector('character', length=length(vec_address)))
  dt_address <- dt_address[class=='', class:=NA]
  dt_address <- dt_address[clean_value=='', clean_value:=NA]
  dt_address <- dt_address[potential_class=='', potential_class:=NA]
  # Remove leading numbers
  address_length <- length(vec_address)
  length_prefix <- str_length(vec_address[1])
  if((str_detect(vec_address[1], paste0('[0-9]{1,', length_prefix, '}'))==TRUE)){
    dt_address <- dt_address[ind==1, class := 'street_number']
    dt_address <- dt_address[ind==1, clean_value := value]
  }
  
  # Suffix Clean
  dt_address <- dt_address[which(str_detect(value, regex_suffix)), potential_class:='suffix']
  dt_address <- dt_address[potential_class=='suffix', suffix_ind_lead:=data.table::shift(ind, n=1, type='lead')]
  max_suffix_ind_lead <- max(dt_address[!is.na(suffix_ind_lead)]$suffix_ind_lead)
  dt_address <- dt_address[nrow(dt_address), suffix_ind_lead:=max_suffix_ind_lead+1]
  dt_address <- dt_address[, suffix_ind_diff:=suffix_ind_lead-ind]
  dt_address <- dt_address[!is.na(suffix_ind_diff), suffix_ind_jump:=ifelse(suffix_ind_diff>1, TRUE, FALSE)]
  suffix_jump_inds <- dt_address[suffix_ind_jump==TRUE]$ind
  suffix_jump_test <- length(suffix_jump_inds)>0
  if(suffix_jump_test==TRUE){
    suffix_jump_ind <- max(suffix_jump_inds)
    dt_address <- dt_address[suffix_jump_inds, potential_class:='']
  }
  multi_suffix_test <- nrow(dt_address[potential_class=='suffix'])>0
  if(multi_suffix_test==TRUE){
    min_multi_index <- min(dt_address[potential_class %in% 'suffix']$ind)
    suffix_inds <- dt_address[potential_class %in% 'suffix']$ind
    suffix_inds <- suffix_inds[!(suffix_inds %in% min_multi_index)]
    dt_address <- dt_address[suffix_inds, class:='suffix']
    dt_address <- dt_address[min_multi_index, class:='street_name']
  }
  if(multi_suffix_test==FALSE){
    dt_address <- dt_address[potential_class=='suffix', class='suffix']
  }
  setkey(dt_address, value)
  dt_abbrev_suffix <- dt_abbrev[class=='suffix'][, .(search_upper, clean_value=str_to_upper(return))]
  setkey(dt_abbrev_suffix, search_upper)
  dt_address_suffix <- dt_abbrev_suffix[dt_address[class=='suffix']]
  dt_address <- rbindlist(list(dt_address[!(class %in% 'suffix')], dt_address_suffix[class=='suffix']), use.names = TRUE, fill=TRUE)
  setorder(dt_address, ind)
  dt_address <- dt_address[, .(ind, value, clean_value, potential_class, class)]
  
  # Unit Identify
  suffix_ind_max <- max(dt_address[class %in% 'suffix']$ind)
  dt_address <- dt_address[is.na(class) & str_detect(value, regex_unit) & ind>suffix_ind_max, class:='unit_name']
  if(nrow(dt_address[class %in% 'unit_name'])>0){
    dt_address_unit <- dt_address[class %in% 'unit_name']
    dt_address_unit$clean_value <- NULL
    dt_abbrev_unit <- dt_abbrev[class=='unit'][, .(search_upper, clean_value=str_to_upper(return))]
    setkey(dt_abbrev_unit, search_upper)
    setkey(dt_address_unit, value)
    dt_address_unit <- dt_abbrev_unit[dt_address_unit[class=='unit_name']]
    dt_address <- rbindlist(list(dt_address[!(class %in% 'unit_name')], dt_address_unit), use.names = TRUE, fill=TRUE)
    setorder(dt_address, ind)
    dt_address <- dt_address[, .(ind, value, clean_value, potential_class, class)]
    # Unit Number 
    max_ind_unit <- max(dt_address[class %in% 'unit_name']$ind)
    dt_address <- dt_address[ind > max_ind_unit & str_detect(value, '([0-9]{1,10}|[a-z]{1,10}|[A-Z]{1,10})($)'), class:='unit_number']
    dt_address <- dt_address[class=='unit_number', clean_value:=value]
  }

  # Street name identify
  street_num_test <- nrow(dt_address[class %in% 'street_number']) > 0
  empty_class_test <- nrow(dt_address[is.na(class)]) > 0
  suffix_test <- nrow(dt_address[class %in% 'suffix']) > 0
  street_name_test <- nrow(dt_address[class %in% 'street_name'])>0
  unit_test <- nrow(dt_address[class %in% 'unit'])>0
  if(empty_class_test==TRUE){
    if(street_num_test==TRUE){
      max_street_num_ind <- max(dt_address[class %in% 'street_number']$ind)
      max_ind <- max(dt_address$ind)
      street_name_ind_start <- min(dt_address[ind %in% (max_street_num_ind+1):max_ind][is.na(class)]$ind)
    }
    if(street_num_test==FALSE){
      street_name_ind_start <- min(dt_address[is.na(class)]$ind)
    }
    dt_address <- dt_address[street_name_ind_start, class:='street_name']
  }
  # street fill
  if(nrow(dt_address[class=='street_name'])==2){
    street_name_ind_start <- min(dt_address[class=='street_name']$ind)
    street_name_ind_end <- max(dt_address[class=='street_name']$ind)
    potential_street_name_range <- street_name_ind_start:street_name_ind_end
    NA_classes_inds <- dt_address[ind %in% potential_street_name_range][is.na(class)]$ind
    NA_potential_classes_inds <- dt_address[ind %in% potential_street_name_range]$ind
    NA_potential_classes_inds <- NA_potential_classes_inds[!(NA_potential_classes_inds) %in% c(street_name_ind_start, street_name_ind_end)]
    NA_mislabeled_inds <- NA_potential_classes_inds[!(NA_potential_classes_inds %in% NA_classes_inds)]
    if(length(NA_classes_inds)==0){
      dt_address <- dt_address[ind %in% NA_potential_classes_inds, class:='street_name']
    } else {
      dt_address <- dt_address[ind %in% NA_classes_inds, class:='street_name']
      dt_address <- dt_address[ind %in% NA_mislabeled_inds, class:='uncertain']
    }
  }
  if(nrow(dt_address[class %in% 'street_name'])>0){
    dt_address_street_name <- dt_address[class %in% 'street_name']
    dt_address_street_name$clean_value <- NULL
    dt_abbrev_street_name <- dt_abbrev[, .(search_upper, clean_value=str_to_upper(return))]
    setkey(dt_abbrev_street_name, search_upper)
    setkey(dt_address_street_name, value)
    dt_address_street_name <- dt_abbrev_street_name[dt_address_street_name[class=='street_name']]
    dt_address <- rbindlist(list(dt_address[!(class %in% 'street_name')], dt_address_street_name), use.names = TRUE, fill=TRUE)
    dt_address <- dt_address[is.na(clean_value) & class %in% 'street_name', clean_value:=search_upper]
    setorder(dt_address, ind)
    dt_address <- dt_address[, .(ind, value, clean_value, potential_class, class)]
  }
  # Clean highway
  
  return(vec_address)
}

