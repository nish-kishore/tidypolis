#date_min_conv (Note: this is copied from idm_polis_api)

#' @param field_name The name of the field used for date filtering.
#' @param date_min The 10 digit string for the min date.
#' @return String compatible with API v2 syntax.

#convert field_name and date_min to the format needed for API query
date_min_conv <- function(field_name = load_query_parameters()$field_name,
                          date_min = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin)){
  if(is.null(field_name) || is.null(date_min)) return(NULL)
  paste0("(",
         paste0(field_name, " ge DateTime'",
                date_min, "'"),
         ")")
}

#convert field_name to the format needed for API query for null query

#' @param field_name The name of the field used for date filtering.
#' @return String compatible with API v2 syntax.
date_null_conv <- function(field_name = load_query_parameters()$field_name){
  if(is.null(field_name)) return(NULL)
  paste0("(",
         paste0(field_name, " eq null",
                ")"))
}

#make_url_general (Note: this is copied from idm_polis_api, with null added)

#' @param field_name The date field to which to apply the date criteria, unique to each data type.
#' @param min_date Ten digit date string YYYY-MM-DD indicating the minimum date, default 2010-01-01
#' @param ... other arguments to be passed to the api
make_url_general <- function(field_name = load_query_parameters()$field_name,
                             min_date = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin)){

  paste0("(",
         date_min_conv(field_name, min_date),
         ") or (",
         date_null_conv(field_name), ")")
  # %>%
  #   paste0("&$inlinecount=allpages")
}
#Single function to create a url array by any method, avoiding repetition of some steps

create_url_array_combined <- function(table_name = load_query_parameters()$table_name,
                                      min_date = as.Date(load_query_parameters()$latest_date, origin=lubridate::origin),
                                      field_name = load_query_parameters()$field_name,
                                      download_size = as.numeric(load_query_parameters()$download_size),
                                      id_vars = load_query_parameters()$id_vars,
                                      method = NULL){
  #Turn off scientific-notation for numbers
  prior_scipen <- getOption("scipen")
  options(scipen = 999)

  #Identify which method to use based on table_name, field_name, and id_vars
  if(is.null(method)){
    if(load_query_parameters()$field_name == "None" |
       grepl("IndicatorValue", table_name) == TRUE){
      method <- "skip_top"
    }
    if(load_query_parameters()$field_name != "None" &
       grepl("IndicatorValue", table_name) == FALSE){
      method <- "id_filter"
    }
  }
  filter_url_conv <- make_url_general(field_name, min_date)
  if(method == "skip_top"){
    if(field_name == "None"){
      if(table_name != "Activity"){
        my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                              paste0(table_name, "?"),
                              "&$inlinecount=allpages",
                              # "$inlinecount=allpages",
                              '&token=',load_specs()$polis$token)
      }
      if(table_name == "Activity"){
        my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                              paste0(table_name, "?"),
                              "$select=Id,ActivityNumber,ActivityParentCode,ActivityDateFrom,ActivityType,WHORegion,Admin0Id,Admin0Name,ActivityAgeGroupCode,ActivityStatusCode,ActivityVaccineType,LqasDataLoaded,NbDoses,CountryPopulation,TargetPopulation,ActivityParentChildrenInaccessible,ActivityChildrenImmunized,ActivityParentLinkedOutbreaksList,Admin0TrendId,SIASubActivityCode,ActivityParentId,InitialPlannedDate,ActivityDateTo,IsPlannedDatesConfirmed,ISTName,CountryISO3Code,Admin0Guid,ActivityAgeGroup,ActivityStatus,ActivityAdditionalInterventions,IMDataLoaded,AreaPopulationPercent,AreaPopulation,PostCampaignImmunizedPercentage,ActivityChildrenInaccessible,LastUpdateDate,Admin0ShapeId",
                              "&$inlinecount=allpages",
                              # "$inlinecount=allpages",
                              '&token=',load_specs()$polis$token)
      }
    }
    #If there is a data filter, create a url with the date filter:
    if(field_name != "None"){
      my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                            paste0(table_name, "?"),
                            "$filter=",
                            if(filter_url_conv == "") "" else paste0(filter_url_conv),
                            "&$inlinecount=allpages",
                            '&token=',load_specs()$polis$token)
    }
  }
  if(method == "id_filter"){
    my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                          paste0(table_name, "?"),
                          "$filter=",
                          if(filter_url_conv == "") "" else paste0(filter_url_conv),
                          "&$select=", paste0(paste(id_vars, collapse=","), ", ", field_name),
                          "&$inlinecount=allpages",
                          '&token=',load_specs()$polis$token)

  }
  if(method == "id_only"){
    my_url_base <- paste0('https:/extranet.who.int/polis/api/v2/',
                          paste0(table_name, "?"),
                          "$select=", paste(id_vars, collapse=","),
                          "&$inlinecount=allpages",
                          '&token=',load_specs()$polis$token)
  }

  #Get table size
  my_url_table_size <- paste0(my_url_base, "&$top=0") %>%
    httr::modify_url()

  response <- call_url(url=my_url_table_size,
                       error_action = "STOP")

  table_size <- response %>%
    httr::content(type='text',encoding = 'UTF-8') %>%
    jsonlite::fromJSON() %>%
    {.$odata.count} %>%
    as.integer()

  #modify url for html
  my_url <- my_url_base %>%
    httr::modify_url()

  #Create sequence for URLs
  urls <- paste0(my_url, "&$top=", as.numeric(download_size), "&$skip=",seq(0,as.numeric(table_size), by = as.numeric(download_size)))
  if(method == "id_filter" & table_size > 0){
    id_list <- call_urls_combined(urls = urls,
                                  type = "id_filter")
    id_list2 <- id_list %>%
      select(id_vars) %>%
      unique() %>%
      mutate_all(as.numeric)
    colnames(id_list2) <- c("Id")
    id_list2 <- id_list2 %>%
      arrange(Id) %>%
      mutate(is_lag_sequential_fwd = ifelse(lag(Id) == Id -1, 1, 0)) %>%
      mutate(is_lag_sequential_fwd = ifelse(is.na(is_lag_sequential_fwd), 0, is_lag_sequential_fwd)) %>%
      arrange(desc(Id)) %>%
      mutate(is_lag_sequential_rwd = ifelse(lag(Id) == Id + 1, 1, 0)) %>%
      mutate(is_lag_sequential_rwd = ifelse(is.na(is_lag_sequential_rwd), 0, is_lag_sequential_rwd)) %>%
      rowwise() %>%
      mutate(flag = sum(is_lag_sequential_fwd, is_lag_sequential_rwd)) %>%
      mutate(flag = ifelse(is.na(flag), 0, flag)) %>%
      ungroup()
    individual_ids <- id_list2 %>%
      filter(flag==0) %>%
      mutate(start = Id, end = Id) %>%
      select(start, end)

    id_grps <- id_list2 %>%
      filter(flag == 1) %>%
      select(Id) %>%
      arrange(Id) %>%
      mutate(x = ifelse(((row_number() %% 2) == 0), "end", "start")) %>%
      mutate(grp = ifelse(x == "start", row_number(), lag(row_number()))) %>%
      pivot_wider(id_cols= grp, names_from = x, values_from=Id) %>%
      select(-grp) %>%
      rbind(individual_ids) %>%
      unique()

    for(i in 1:nrow(id_grps)){
      min_id <- id_grps$start[i]
      max_id <- id_grps$end[i]
      break_list_start <- seq(min_id, max_id, by=1000)
      break_list_end <- seq(min_id+999, max_id+999, by=1000)
      if(i == 1){
        id_section_table <- data.frame(start=break_list_start, end=break_list_end, min=min_id, max=max_id) %>%
          mutate_all(as.numeric) %>%
          rowwise() %>%
          mutate(end = ifelse(end > max, max, end)) %>%
          ungroup() %>%
          mutate(filter_url_conv = paste0("((",id_vars," ge ", start, ") and (",id_vars," le ", end,"))"))
      }
      if(i != 1){
        id_section_table <- id_section_table %>%
          bind_rows(data.frame(start=break_list_start, end=break_list_end, min=min_id, max=max_id) %>%
                      mutate_all(as.numeric) %>%
                      rowwise() %>%
                      mutate(end = ifelse(end > max, max, end)) %>%
                      ungroup() %>%
                      mutate(filter_url_conv = paste0("((",id_vars," ge ", start, ") and (",id_vars," le ", end,"))")))
      }
    }
    urls <- c()
    for(i in id_section_table$filter_url_conv){
      url <- paste0('https:/extranet.who.int/polis/api/v2/',
                    paste0(table_name, "?"),
                    '$filter=',
                    i,
                    '&token=',load_specs()$polis$token) %>%
        httr::modify_url()
      urls <- c(urls, url)
    }
  }
  options(scipen = prior_scipen)
  return(urls)
}


#' get table data for a single url request
#' @param url string of a single url
#' @param p used as iterator in multicore processing
get_table_data <- function(url, p){tryCatch({

  p()
  result <- call_url(url=url,
                     error_action = "RETURN NULL")

  if(is.null(result) == FALSE){
    response_data <- result %>%
      httr::content(type='text',encoding = 'UTF-8') %>%
      jsonlite::fromJSON() %>%
      {.$value} %>%
      as_tibble() %>%
      mutate_all(., as.character)
  }
  if(is.null(result) == TRUE){
    response_data <- NULL
  }
  response_data <- tibble(url = c(url), data = list(response_data))
  return(response_data)
}, error=function(e){
  response_data <- list(url = c(url), data = NULL)
  return(response_data)
})
}

#' multicore pull from API
#' @param urls array of URL strings
mc_api_pull <- function(urls){
  p <- progressor(steps = length(urls))
  future_map(urls,get_table_data, p = p) %>%
    rbind()
}

#' wrapper around multicore pull to produce progress bars
#' @param urls array of URL strings
pb_mc_api_pull <- function(urls){
  n_cores <- availableCores() - 1
  plan(multicore, workers = n_cores, gc = T)

  with_progress({
    result <- mc_api_pull(urls)
  })

  failed_urls <- c()

  #extract data from result and combine into result_df
  for(i in 1:length(result)){
    data <- result[[i]]$data[[1]]
    if(i == 1){
      result_df <- data
    }
    if(i != 1){
      result_df <- result_df %>%
        bind_rows(data)
    }
    #If the url failed, extract the url from result and add to the failed_urls list
    if(is.null(data)){
      failed_urls <- c(failed_urls, result[[i]]$url[1])
    }
  }
  #Combine the full dataset and failed_urls list and return
  result <- list(result_df, failed_urls)
  return(result)
  stopCluster(n_cores)
}

#retry all urls that failed when calling a url array
handle_failed_urls <- function(failed_urls,
                               failed_url_filename,
                               query_output,
                               retry = TRUE,
                               save = TRUE){
  if(length(failed_urls) > 0){
    if(retry == TRUE){
      retry_query_output_list <- pb_mc_api_pull(failed_urls)
      retry_query_output <- retry_query_output_list[[1]]
      if(is.null(retry_query_output)){
        retry_query_output <- data.frame(matrix(nrow=0, ncol=0))
      }
      failed_urls <- retry_query_output_list[[2]]
      query_output <- query_output %>%
        bind_rows(retry_query_output)
    }
    if(save == TRUE){
      write_rds(failed_urls, failed_url_filename)
    }
  }
  return(query_output)
}

#Join the previously cached dataset for a table to the newly pulled dataset
append_and_save <- function(query_output = query_output,
                            id_vars = load_query_parameters()$id_vars, #id_vars is a vector of data element names that, combined, uniquely identifies a row in the table
                            table_name = load_query_parameters()$table_name,
                            full_idvars_output = full_idvars_output){

  id_vars <- as.vector(id_vars)

  #If the newly pulled dataset has any data, then read in the old file, remove rows from the old file that are in the new file, then bind the new file and old file
  if(!is.null(query_output) & nrow(query_output) > 0 & file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
    old_polis <- readRDS(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))  %>%
      mutate_all(.,as.character) %>%
      #remove records that are in new file
      anti_join(query_output, by=id_vars)
    #remove records that are no longer in the POLIS table from old_polis

    new_query_output <- query_output %>%
      bind_rows(old_polis)

    #remove records that are no longer in the POLIS table from query_output
    if(nrow(full_idvars_output) > 0){
      deleted_obs <- new_query_output %>%
        select(id_vars) %>%
        anti_join(full_idvars_output, by=id_vars)
      new_query_output <- new_query_output %>%
        anti_join(deleted_obs, by=id_vars)
      print(paste0("Check for deleted records found and removed ", nrow(deleted_obs), " records from the previously saved table."))
    }
    #remove exact duplicate rows
    new_query_output <- new_query_output %>%
      unique()

    #save to file
    write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
    return(new_query_output)

    #If the overall number of rows in the table is not equal to old and new combined, then stop and flag for investigation
    #NOTE: instead of flagging, this could just trigger a re-pull of the full dataset
    if(table_count2 != nrow(old_polis) + nrow(query_output)){
      warning("Table is incomplete: check id_vars and field_name")
    }
  }
  if(!is.null(query_output) & nrow(query_output) > 0 & !file.exists(file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))){
    #check that the combined total row number matches POLIS table row number before appending
    #Get full table size for comparison to what was pulled via API, saved as "table_count2"
    my_url2 <-  paste0('https:/extranet.who.int/polis/api/v2/',
                       paste0(table_name, "?"),
                       "$inlinecount=allpages&$top=0",
                       '&token=',load_specs()$polis$token) %>%
      httr::modify_url()

    #The below while() loop runs my_url2 through the API until it succeeds or up to 10 times. If 10 try limit is reached, then the process is halted.
    result2 <- call_url(url=my_url2,
                        error_action = "STOP")

    result_content2 <- httr::content(result2, type='text',encoding = 'UTF-8') %>% jsonlite::fromJSON()
    table_count2 <- as.numeric(result_content2$odata.count)

    #If the overall number of rows in the table is equal to the rows in the old dataset (with new rows removed) + the rows in the new dataset, then combine the two and save
    # if(table_count2 == nrow(query_output)){
    new_query_output <- query_output
    #save to file
    write_rds(new_query_output, file.path(load_specs()$polis_data_folder, paste0(table_name, ".rds")))
    return(new_query_output)
    # }
    if(table_count2 != nrow(query_output)){
      warning("Table is incomplete: check id_vars and field_name")
    }
  }
}

#combine calling urls and processing output into a single function
call_urls_combined <- function(urls,
                               type){
  if(type == "full"){print("Pulling all variables:")}
  if(type == "re-pull"){print("Metadata change detected: Re-pulling all variables:")}
  if(type == "id_filter"){print("Pulling ID variables:")}
  if(type == "id_only"){print("Checking for deleted Ids in the full table:")}
  query_start_time <- Sys.time()
  query_output <- data.frame(matrix(nrow=0, ncol=0))
  query_output_list <- pb_mc_api_pull(urls)
  query_output <- query_output_list[[1]]
  if(is.null(query_output)){
    query_output <- data.frame(matrix(nrow=0, ncol=0))
  }
  failed_urls <- query_output_list[[2]]
  if(type == "full"){
    failed_url_filename <- file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name,"_failed_urls.rds"))
  }
  if(type == "id_filter"){
    failed_url_filename <- file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name,"_id_filter_failed_urls.rds"))
  }
  if(type == "id_only"){
    failed_url_filename <- file.path(load_specs()$polis_data_folder, paste0(load_query_parameters()$table_name,"_id_only_failed_urls.rds"))
  }
  query_output <- handle_failed_urls(failed_urls,
                                     failed_url_filename,
                                     query_output,
                                     retry = TRUE,
                                     save = TRUE)
  query_stop_time <- Sys.time()
  query_time <- round(difftime(query_stop_time, query_start_time, units="auto"),0)
  if(type == "full" & !is.null(query_output)){
    print(paste0("Downloaded ", nrow(query_output)," rows from ",load_query_parameters()$table_name_descriptive," Table in ", query_time[[1]], " ", units(query_time),"."))
  }
  if(type == "re-pull" & !is.null(query_output)){
    print(paste0("Metadata or field_name changed from cached version: Re-downloaded ", nrow(query_output)," rows from ",load_query_parameters()$table_name_descriptive," Table in ", query_time[[1]], " ", units(query_time),"."))
  }
  return(query_output)
}

#' Load Authorizations and Local Config
#' @param folder Folder pathway where POLIS data will be saved
#' @return auth/config object
load_specs <- function(folder = Sys.getenv("polis_data_folder")){
  specs <- read_yaml(file.path(folder,'cache_dir','specs.yaml'))
  return(specs)
}

#' Load Query Parameters
#' @param folder Folder pathway where POLIS data are saved
#' @return Parameters for an individual POLIS table query
load_query_parameters <- function(folder = Sys.getenv("polis_data_folder")){
  query_parameters <- read_yaml(file.path(folder,'cache_dir','query_parameters.yaml'))
  return(query_parameters)
}

#' Load a dataframe of query parameters corresponding to each API-accessible POLIS table
#' @return Dataframe of default parameters, where each row can be used as query parameters for a POLIS table
load_defaults <- function(){
  defaults <- as.data.frame(bind_rows(
    c(table_name_descriptive = "Activity", table_name = "Activity", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Case", table_name = "Case", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Environmental Sample", table_name = "EnvSample", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Geography", table_name = "Geography", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP 0 dose percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP 1 to 2 dose percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_1_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP 3+ doses percentage for 6M-59M", table_name = "IndicatorValue('AFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: AFP cases", table_name = "IndicatorValue('AFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Circulating VDPV case count (all Serotypes)", table_name = "IndicatorValue('cVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Circulating VDPV cace count (all serotypes) - Reporting", table_name = "IndicatorValue('cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Combined Surveillance Indicators category", table_name = "IndicatorValue('SURVINDCAT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Environmental sample circulating VDPV", table_name = "IndicatorValue('ENV_CVDPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Environmental samples count", table_name = "IndicatorValue('ENV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Environmental Wild samples", table_name = "IndicatorValue('ENV_WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Non polio AFP cases (under 15Y)", table_name = "IndicatorValue('NPAFP_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending excluded)", table_name = "IndicatorValue('NPAFP_RATE_NOPENDING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Non polio AFP rate (Pending included)", table_name = "IndicatorValue('NPAFP_RATE')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: NPAFP 0-2 dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_0_to_2')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: NPAFP 0 dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_0')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: NPAFP 3+ dose percentage for 6M-59M", table_name = "IndicatorValue('NPAFP_DOSE_3PLUS')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of 60-day follow-up cases with inadequate stool", table_name = "IndicatorValue('FUP_INSA_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of cases not classified", table_name = "IndicatorValue('UNCLASS_CASES_PERCENT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of cases w/ adeq stools specimens (condition+timeliness)", table_name = "IndicatorValue('NPAFP_SA_WithStoolCond')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Percent of cases with two specimens within 14 days of onset", table_name = "IndicatorValue('NPAFP_SA')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Reported circulating VDPV environmental samples count (all serotypes)", table_name = "IndicatorValue('ENV_cVDPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Reported Wild environmental samples count", table_name = "IndicatorValue('ENV_WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA bOPV campaigns", table_name = "IndicatorValue('SIA_BOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA mOPV campaigns", table_name = "IndicatorValue('SIA_MOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA planned since last case", table_name = "IndicatorValue('SIA_LASTCASE_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: SIA tOPV campaigns", table_name = "IndicatorValue('SIA_TOPV')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Total SIA campaigns", table_name = "IndicatorValue('SIA_OPVTOT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Wild poliovirus case count", table_name = "IndicatorValue('WPV_COUNT')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    # c(table_name_descriptive = "Indicator: Wild poliovirus case count - Reporting", table_name = "IndicatorValue('WPV_COUNT_REPORTING')", field_name = "LastCalculationDateTime", id_vars ="RowID", download_size = 1000),
    c(table_name_descriptive = "Lab Specimen", table_name = "LabSpecimen", field_name = "LastUpdateDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "LQAS", table_name = "Lqas", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Population", table_name = "Population", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityCategories", table_name = "RefData('ActivityCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityDeletionReason", table_name = "RefData('ActivityDeletionReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityPhases", table_name = "RefData('ActivityPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityPriorities", table_name = "RefData('ActivityPriorities')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityStatuses", table_name = "RefData('ActivityStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ActivityTypes", table_name = "RefData('ActivityTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AdditionalInterventions", table_name = "RefData('AdditionalInterventions')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AgeGroups", table_name = "RefData('AgeGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AllocationStatuses", table_name = "RefData('AllocationStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: AwardTypes", table_name = "RefData('AwardTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CalendarTypes", table_name = "RefData('CalendarTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CaseClassification", table_name = "RefData('CaseClassification')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Categories", table_name = "RefData('Categories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CategoryValues", table_name = "RefData('CategoryValues')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CellCultureResultsLRarm", table_name = "RefData('CellCultureResultsLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CellCultureResultsRLRarm", table_name = "RefData('CellCultureResultsRLRarm')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Channels", table_name = "RefData('Channels')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContactIsolatedVdpv", table_name = "RefData('ContactIsolatedVdpv')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContactIsolatedWild", table_name = "RefData('ContactIsolatedWild')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContributionFlexibility", table_name = "RefData('ContributionFlexibility')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ContributionStatuses", table_name = "RefData('ContributionStatuses')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: CostCenters", table_name = "RefData('CostCenters')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Currencies", table_name = "RefData('Currencies')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Datasets", table_name = "RefData('Datasets')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DateTag", table_name = "RefData('DateTag')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DelayReason", table_name = "RefData('DelayReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DiagnosisFinal", table_name = "RefData('DiagnosisFinal')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: DocumentCategories", table_name = "RefData('DocumentCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: EmergenceGroups", table_name = "RefData('EmergenceGroups')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FinalCellCultureResultAfp", table_name = "RefData('FinalCellCultureResultAfp')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FinalCellCultureResultLab", table_name = "RefData('FinalCellCultureResultLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FinalITDResults", table_name = "RefData('FinalITDResults')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FollowupFindings", table_name = "RefData('FollowupFindings')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: FundingPhases", table_name = "RefData('FundingPhases')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Genders", table_name = "RefData('Genders')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Genotypes", table_name = "RefData('Genotypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Geolevels", table_name = "RefData('Geolevels')", field_name = "None", id_vars ="Id", download_size = 1000),
    # ## c(table_name_descriptive = "Reference Data: ImportationEvents", table_name = "RefData('ImportationEvents')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed because API documentation states there is no data for this table
    # c(table_name_descriptive = "Reference Data: IndependentMonitoringReasons", table_name = "RefData('IndependentMonitoringReasons')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: IndependentMonitoringSources", table_name = "RefData('IndependentMonitoringSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: IndicatorCategories", table_name = "RefData('IndicatorCategories')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: IntratypeLab", table_name = "RefData('IntratypeLab')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Intratypes", table_name = "RefData('Intratypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: LQASClassifications", table_name = "RefData('LQASClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: LQASDenominators", table_name = "RefData('LQASDenominators')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: LQASThresholds", table_name = "RefData('LQASThresholds')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Methodologies", table_name = "RefData('Methodologies')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Objectives", table_name = "RefData('Objectives')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Outbreaks", table_name = "RefData('Outbreaks')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ParalysisSite", table_name = "RefData('ParalysisSite')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PartialInclusionReason", table_name = "RefData('PartialInclusionReason')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PopulationSources", table_name = "RefData('PopulationSources')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: PosNeg", table_name = "RefData('PosNeg')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: PreviousNumberOfDoses", table_name = "RefData('PreviousNumberOfDoses')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ProgrammeAreas", table_name = "RefData('ProgrammeAreas')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ReasonMissed", table_name = "RefData('ReasonMissed')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: ReasonVaccineRefused", table_name = "RefData('ReasonVaccineRefused')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Reports", table_name = "RefData('Reports')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ResultsElisa", table_name = "RefData('ResultsElisa')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ResultsPCR", table_name = "RefData('ResultsPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: ResultsRRTPCR", table_name = "RefData('ResultsRRTPCR')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: RiskLevel", table_name = "RefData('RiskLevel')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SampleCondition", table_name = "RefData('SampleCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: Sectors", table_name = "RefData('Sectors')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SecurityStatus", table_name = "RefData('SecurityStatus')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SourceOfIsolate", table_name = "RefData('SourceOfIsolate')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SpecimenSampleType", table_name = "RefData('SpecimenSampleType')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SpecimenSource", table_name = "RefData('SpecimenSource')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: StoolCondition", table_name = "RefData('StoolCondition')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: SurveillanceType", table_name = "RefData('SurveillanceType')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: UpdateType", table_name = "RefData('UpdateType')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: Vdpv2Clusters", table_name = "RefData('Vdpv2Clusters')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for Vdpv2Clusters reference data'
    # c(table_name_descriptive = "Reference Data: VdpvClassifications", table_name = "RefData('VdpvClassifications')", field_name = "None", id_vars ="Id", download_size = 1000),
    ## c(table_name_descriptive = "Reference Data: VdpvSources", table_name = "RefData('VdpvSources')", field_name = "None", id_vars ="Id", download_size = 1000), #Removed for now, since API documentation states 'No data for VdpvSources reference data'
    # c(table_name_descriptive = "Reference Data: VirusTypes", table_name = "RefData('VirusTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: VirusWildType", table_name = "RefData('VirusWildTypes')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Data: YesNo", table_name = "RefData('YesNo')", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Sub Activity", table_name = "SubActivity", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Synonym", table_name = "Synonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Virus", table_name = "Virus", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Historized Geoplace Names", table_name = "HistorizedGeoplaceNames", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Historized Synonym", table_name = "HistorizedSynonym", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "IM LQAS Last Two Rounds", table_name = "ImLqasLastTwoRounds", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Enviro Sample Sites", table_name = "EnviroSampleSites", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "IM LQAS Since 3 Years Done Planned SIAs", table_name = "ImLqasSince3YearsDonePlannedSIAs", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "RefCluster", table_name = "RefCluster", field_name = "UpdatedDate", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Reference Age Group Data", table_name = "RefAgeGroupData('AgeGroups')", field_name = "None", id_vars ="None", download_size = 1000),
    # c(table_name_descriptive = "PONS Store GR Environmental Samples", table_name = "PonsStoreGREnvironmentalSamples", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "PONS Store GR Cases", table_name = "PonsStoreGRCases", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "Outbreak SIAs", table_name = "OutbreakSias", field_name = "None", id_vars ="Id", download_size = 1000),
    # c(table_name_descriptive = "OprSopTracker", table_name = "OprSopTracker", field_name = "None", id_vars ="Id", download_size = 1000),
    c(table_name_descriptive = "Independent Monitoring", table_name = "Im", field_name = "None", id_vars ="Id", download_size = 1000)
  ))
  return(defaults)
}

#' Call URL
#'
#' Call an individual URL until it succeeds or reaches a call limit
#'
#' @param url A single URL to be called
#' @param call_limit Number of times to re-try URL if call fails
#' @param timeout_secs Time to wait from call to response before determining that call has failed
#' @return The response from the URL
call_url <- function(url,
                     call_limit = 10,
                     timeout_secs = 150,
                     error_action = "STOP"){
  status_code <- "x"
  i <- 1
  while(status_code != "200" & i < call_limit){
    response <- NULL
    response <- httr::GET(url, timeout(timeout_secs))
    if(is.null(response) == FALSE){
      status_code <- as.character(response$status_code)
    }
    i <- i+1
    if(i == call_limit){
      if(error_action == "STOP"){
        stop("Query halted. Repeated API call failure.")
      }
      if(error_action == "RETURN NULL"){
        response <- NULL
      }
    }
    if(status_code != "200"){Sys.sleep(10)}
  }
  rm(status_code)
  return(response)
}


load_GetPOLIS("C:/Users/ynm2/Desktop/gitrepos/")
init_polis_data_struc(folder = "C:/Users/ynm2/Desktop/POLIS_data",
                      token = "BRfIZj%2fI9B3MwdWKtLzG%2bkpEHdJA31u5cB2TjsCFZDdMZqsUPNrgiKBhPv3CeYRg4wrJKTv6MP9UidsGE9iIDmaOs%2bGZU3CP5ZjZnaBNbS0uiHWWhK8Now3%2bAYfjxkuU1fLiC2ypS6m8Jy1vxWZlskiPyk6S9IV2ZFOFYkKXMIw%3d")
