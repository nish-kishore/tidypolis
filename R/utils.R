#' @description Manage input
#' @param request str:
#' @param error_resp str:
#' @param vals array:
#' @param max_i int:
#' @returns val from `vals` or stops output
request_input <- function(
    request,
    vals,
    error_resp = "Valid input not chosen",
    max_i = 3
){

  message(request)

  i <- 1

  vals_str <- paste0("Please choose one of the following [",
                     paste0(as.character(vals), collapse = "/"),
                     "]:  \n")

  val <- readline(prompt = vals_str)

  while(!val %in% vals){
    val <- readline(prompt = vals_str)
    i <- i + 1
    if(i == max_i){
      stop(error_resp)
    }
  }

  return(val)

}


#' @description Update the POLIS cache directory
#' @param cache_file str: location of cache file
#' @param table str: table to be updated
#' @param nrow double: nrow of table to be updated
#' @returns Return true if cache updated
update_polis_cache <- function(
    cache_file = Sys.getenv("POLIS_CACHE_FILE"),
    .table,
    .nrow,
    .update_val
){

  readr::read_rds(cache_file) |>
    dplyr::mutate(
      nrow = ifelse(table == .table, .nrow, nrow),
      last_sync = ifelse(table == .table, lubridate::as_datetime(Sys.time()), last_sync),
      last_user = ifelse(table == .table, Sys.getenv("USERNAME"), last_user),
      polis_update_value = ifelse(table == .table, lubridate::as_datetime(.update_val), polis_update_value),
      last_sync = lubridate::as_datetime(last_sync),
      polis_update_value = lubridate::as_datetime(polis_update_value)
    ) |>
    readr::write_rds(cache_file)

}


#' @description Update the POLIS log
#' @param log_file str: location of cache file
#' @param .time dttm: time of update
#' @param .user double: user who conducted the action
#' @param .event str: event to be logged
#' @returns Return true if cache updated
update_polis_log <- function(
    log_file = Sys.getenv("POLIS_LOG_FILE"),
    .time = Sys.time(),
    .user = Sys.getenv("USERNAME"),
    .event
){

  readr::read_rds(log_file) |>
    tibble::add_row(
      time = .time,
      user = .user,
      event = .event
    ) |>
    readr::write_rds(log_file)

}


#' @description Pull cache data for a particular talbe
#' @param cache_file str: location of cache file
#' @param table str: table to be loaded
#' @returns Return tibble with table information
get_polis_cache <- function(
    cache_file = Sys.getenv("POLIS_CACHE_FILE"),
    .table
){
  cache <- readr::read_rds(cache_file)

  if(.table %in% dplyr::pull(cache, table)){
    cache |>
      dplyr::filter(table == .table)
  }else{
    cli::cli_alert_warning(paste0("No entry found in the cache table for: ", .table))
  }


}


#' @description Get POLIS table Data
#' @param api_key API Key
#' @param .table Table value to retrieve
#' @param .last_update Time of last update
#' @param
#' @returns Tibble with reference data
get_table_data <- function(
    api_key = Sys.getenv("POLIS_API_Key"),
    .table
    ){

  base_url <- "https://extranet.who.int/polis/api/v2/"
  table_data <- get_polis_cache(.table = .table)
  table_url <- paste0(base_url, table_data$endpoint)

  cli::cli_h1(paste0("Downloading POLIS Data for: ", table_data$table))

  #If never downloaded before
  if(is.na(table_data$last_sync) & !is.na(table_data$polis_id)){
    cli::cli_alert_info(paste0(table_data$endpoint, " has not been downloaded before...checking size..."))
    table_size <- get_table_size(.table = table_data$table)
    cli::cli_alert_info(paste0("Getting ready to download ", table_size, " new rows of data!"))

    if(table_data$table %in% c("human_specimen", "environmental_sample", "activity", "sub_activity", "lqas")){
      urls <- create_table_urls(url = table_url, table_size = table_size, type = "lab")
    }else{
      urls <- create_table_urls(url = table_url, table_size = table_size, type = "full")
    }

    cli::cli_process_start("Downloading data")
    out <- call_urls(urls)
    update_polis_log(.event = paste0("Downloaded ", table_size, "rows of ", table_data$table, " data"))
    cli::cli_process_done()

    #update cache information
    cli::cli_process_start("Updating cache")
    update_polis_cache(
      cache_file = Sys.getenv("POLIS_CACHE_FILE"),
      .table = .table,
      .nrow = nrow(out),
      .update_val = max(lubridate::as_datetime(dplyr::pull(out[table_data$polis_update_id])))
      )
    cli::cli_process_done()

    cli::cli_process_start("Writing data cache")
    readr::write_rds(out, file = paste0(Sys.getenv("POLIS_DATA_CACHE"),"/",table_data$table,".rds"))
    update_polis_log(.event = paste0(table_data$table, " data saved locally"))
    cli::cli_process_done()

    gc()

  }else{

    if(!is.na(table_data$last_sync)){

      #pull updated data
      #create new table url

      time_modifier <- paste0(
        "&$filter=",
        table_data$polis_update_id,
        " gt DateTime'",
        sub(" ", "T", as.character(table_data$polis_update_value)),
        "'"
      )

      time_modifier <- gsub(" ", "+", time_modifier, )

      table_size <- get_table_size(.table = table_data$table, extra_filter = time_modifier)

      cli::cli_alert_success(paste0(table_data$table, ": ", table_size, " new or updated records identified!"))
      update_polis_log(.event = paste0(table_data$table, ": ", table_size, " new or updated records identified!"))

      if(table_size > 0){
        table_url <- paste0(
          table_url,
          "?$filter=",
          table_data$polis_update_id,
          " gt DateTime'",
          sub(" ", "T", as.character(table_data$polis_update_value)),
          "'"
        )

        table_url <- gsub(" ", "+", table_url, )

        if(table_data$table %in% c("human_specimen", "environmental_sample", "activity", "sub_activity", "lqas")){
          urls <- create_table_urls(url = table_url, table_size = table_size, type = "lab-partial")
        }else{
          urls <- create_table_urls(url = table_url, table_size = table_size, type = "partial")
        }

        cli::cli_process_start("Downloading data")
        out <- call_urls(urls)
        update_polis_log(.event = paste0("Downloaded ", table_size, "rows of ", table_data$table, " data"))
        cli::cli_process_done()

        #check ids and make list of ids to be deleted
        cli::cli_process_start("Getting table Ids")
        ids <- get_table_ids(.table = table_data$table, .id = table_data$polis_id)
        cli::cli_process_done()

        #load in cache
        cli::cli_process_start("Loading existing cache")
        old_cache <- readr::read_rds(paste0(Sys.getenv("POLIS_DATA_CACHE"),"/",table_data$table,".rds"))
        cli::cli_process_done()
        old_cache_n <- nrow(old_cache)
        new_data_ids_in_old_cache <- sum(dplyr::pull(out[table_data$polis_id]) %in% dplyr::pull(old_cache[table_data$polis_id]))
        new_data_ids <- table_size-new_data_ids_in_old_cache
        deleted_ids <- dplyr::pull(old_cache[table_data$polis_id])[!dplyr::pull(old_cache[table_data$polis_id]) %in% ids]
        #old_data_ids_in_new <- dplyr::pull(old_cache[table_data$polis_id])[dplyr::pull(old_cache[table_data$polis_id]) %in% dplyr::pull(out[table_data$polis_id])]

        cli::cli_h3(paste0("'",table_data$table,"'", " table data"))
        cli::cli_bullets(
          c(
            "*" = paste0(table_size, " new rows of data downloaded"),
            "*" = paste0(old_cache_n, " rows of data available in old cache"),
            "*" = paste0(new_data_ids, " new ", table_data$polis_id, "s identified"),
            "*" = paste0(new_data_ids_in_old_cache, " rows of data being updated"),
            "*" = paste0(length(deleted_ids), " rows of data were deleted")
          )
        )

        update_polis_log(.event = paste0(table_data$table, " - update - ",
                                         table_size, " new rows of data downloaded; ",
                                         old_cache_n, " rows of data available in old cache; ",
                                         new_data_ids, " new ", table_data$polis_id, "s identified; ",
                                         new_data_ids_in_old_cache, " rows of data being updated;",
                                         paste0(length(deleted_ids), " rows of data were deleted - "),
                                         paste0(deleted_ids, collapse = ", ")))

        #update cache
        old_cache <- old_cache |> dplyr::filter(!get(table_data$polis_id) %in% dplyr::pull(out[table_data$polis_id]))
        old_cache <- bind_and_reconcile(old_cache, out)

        #write cache

        cli::cli_process_start("Updating cache log")
        update_polis_cache(
          cache_file = Sys.getenv("POLIS_CACHE_FILE"),
          .table = .table,
          .nrow = nrow(old_cache),
          .update_val = max(lubridate::as_datetime(dplyr::pull(out[table_data$polis_update_id])))
        )
        cli::cli_process_done()

        cli::cli_process_start("Writing data cache")
        readr::write_rds(out, file = paste0(Sys.getenv("POLIS_DATA_CACHE"),"/",table_data$table,".rds"))
        update_polis_log(.event = paste0(table_data$table, " data saved locally"))
        cli::cli_process_done()

        #garbage clean
        gc()

      }

    }

  }

}


#' Get table size from POLIS
#' @param .table str: Table to be downloaded
#' @param api_key str: API Key
#' @param cache_file str: Cache file location
get_table_size <- function(
    .table,
    api_key = Sys.getenv("POLIS_API_KEY"),
    cache_file = Sys.getenv("POLIS_CACHE_FILE"),
    extra_filter = ""
){

  table_data <- get_polis_cache(.table = .table)

  # disable SSL Mode
  httr::set_config(httr::config(ssl_verifypeer = 0L))

  # Variables: URL, Token, Filters, ...
  polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

  api_url <- paste0(polis_api_root_url, table_data$endpoint, "?$inlinecount=allpages&$top=0", extra_filter)

  #response <- httr::GET(url=api_url, httr::add_headers("authorization-token" = api_key))

  response <- httr::RETRY(
    verb = "GET",
    url = api_url,
    config = httr::add_headers("authorization-token" = api_key),
    times = 10,
    pause_min = 2,
    quiet = TRUE,
    terminate_on_success = TRUE
  )

  out <- jsonlite::fromJSON(rawToChar(response$content))

  tibble::as_tibble(out$value)


  table_size <- response |>
    httr::content(type='text',encoding = 'UTF-8') |>
    jsonlite::fromJSON()

  table_size <- as.integer(table_size$odata.count)

  return(table_size)

}

#' Create table URLs
#'
#' @description create urls from table size and base url
#' @url str: base url to be queried
#' @table_size int: integer of download
#' @type str: "full" or "partial"
#' @returns array of urls
create_table_urls <- function(
    url,
    table_size,
    type
){

  prior_scipen <- getOption("scipen")
  options(scipen = 999)

  if(sum(type %in% c("full", "partial", "lab", "lab-partial")) > 0){

    if(type == "full"){
      urls <- paste0(url, "?$top=2000&$skip=",as.character(seq(0,as.numeric(table_size), by = 2000)))
    }

    if(type == "partial"){
      urls <- paste0(url, "&$top=2000&$skip=",seq(0,as.numeric(table_size), by = 2000))
    }

    if(type == "lab"){
      urls <- paste0(url, "?$top=1000&$skip=",as.character(seq(0,as.numeric(table_size), by = 1000)))
    }

    if(type == "lab-partial"){
      urls <- paste0(url, "&$top=1000&$skip=",seq(0,as.numeric(table_size), by = 1000))
    }

  }
  return(urls)

}


#' Call multiple URLs
#' @description Call multiple URLs
#' @param urls
#' @return tibble with all data
call_urls <- function(urls){

  doFuture::registerDoFuture() ## tell foreach to use futures
  future::plan(future::multisession) ## parallelize over a local PSOCK cluster
  options(doFuture.rng.onMisuse = "ignore")
  xs <- 1:length(urls)

  progressr::handlers("cli")

  progressr::with_progress({
    p <- progressr::progressor(along = xs)
    y <- foreach::`%dopar%`(foreach::foreach(x = xs), {
      # signal a progression update
      p()
      # jitter the parallel calls to not overwhelm the server
      Sys.sleep(1 + rpois(1, 10)/100)
      call_single_url(urls[x])
    })
  })

  y <- dplyr::bind_rows(y)
  gc()
  return(y)

}


#' Call single URL
#' @description Call a return the formatted output frome one URL
#' @param url
#' @param api_key
#' @return tibble
call_single_url <- function(
    url,
    api_key = Sys.getenv("POLIS_API_KEY")
    ){
  # disable SSL Mode
  httr::set_config(httr::config(ssl_verifypeer = 0L))

  #response <- httr::GET(url=url, httr::add_headers("authorization-token" = api_key))

  response <- httr::RETRY(
    verb = "GET",
    url = url,
    config = httr::add_headers("authorization-token" = api_key),
    times = 10,
    quiet = TRUE,
    terminate_on_success = TRUE
  )

  out <- jsonlite::fromJSON(rawToChar(response$content))

  tibble::as_tibble(out$value)

  #Sys.sleep(1.25)

}

#' Get Ids
#'
#' @description return Ids availalbe in table
#' @param table str: table
#' @param id str: id variable
#' @param api_key str: POLIS API Key
#' @return character array of ids
get_table_ids <- function(.table, .id, api_key = Sys.getenv("POLIS_API_KEY")){

  cli::cli_process_start(paste0("Downloading ", .table, " table IDs"))

  table_data <- get_polis_cache(.table = .table)

  # disable SSL Mode
  httr::set_config(httr::config(ssl_verifypeer = 0L))

  # Variables: URL, Token, Filters, ...
  polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

  api_url <- paste0(polis_api_root_url, table_data$endpoint, "?$select=", table_data$polis_id)

  table_size <- get_table_size(.table = "case")

  if(table_data$table %in% c("human_specimen", "environmental_sample", "activity", "sub_activity", "lqas")){
    urls <- create_table_urls(url = api_url, table_size = table_size, type = "lab-partial")
  }else{
    urls <- create_table_urls(url = api_url, table_size = table_size, type = "partial")
  }

  ids <- call_urls(urls) |>
    dplyr::pull(table_data$polis_id)

  gc()

  cli::cli_process_done()

  return(ids)

}

#' Manager function to get and update POLIS data
#'
get_polis_data <- function(){

  tables <- c("virus", "case", "human_specimen", "environmental_sample",
              "activity", "sub_activity", "lqas", "im")

  sapply(tables, function(x) get_table_data(.table = x))

}


# old_data <- x
# new_data <- tibble::as_tibble(lapply(old_data, as.character))
#' Reconcile classes and bind two tibbles
#'
#' @param new tibble: Tibble to be converted and bound
#' @param old tibble: Tibble to be referenced
#' @returns tibble: bound tibble
bind_and_reconcile <- function(new_data, old_data){

  old_names <- names(old_data)
  classes_old <- sapply(old_data, class)
  new_data <- as.data.frame(new_data)
  old_data <- as.data.frame(old_data)

  for(name in old_names){

    class(new_data[,name]) <- classes_old[name][[1]]

  }

  new_data <- tibble::as_tibble(new_data)
  old_data <- tibble::as_tibble(old_data)

  return(dplyr::bind_rows(old_data, new_data))

}
