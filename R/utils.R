#### POLIS Interactions ####

#' Request data from single table
#'
#' @description Get POLIS table Data
#' @import cli lubridate dplyr readr
#' @param api_key API Key
#' @param .table Table value to retrieve
#' @returns Tibble with reference data
get_table_data <- function(api_key = Sys.getenv("POLIS_API_Key"),
                           .table) {
  base_url <- "https://extranet.who.int/polis/api/v2/"
  table_data <- get_polis_cache(.table = .table)
  table_url <- paste0(base_url, table_data$endpoint)

  #check if ID API works for key files
  api_url <-
    paste0(base_url,
           table_data$endpoint,
           "?$select=",
           table_data$polis_id)

  if (table_data$table %in% c("human_specimen",
                              "environmental_sample",
                              "activity",
                              "sub_activity",
                              "lqas",
                              "pop")) {
    urls <-
      create_table_urls(url = api_url,
                        table_size = 3000,
                        type = "lab-partial")
  } else{
    urls <-
      create_table_urls(url = api_url,
                        table_size = 3000,
                        type = "partial")
  }

  id_return <- tryCatch(
    call_single_url(urls[1], times = 1),
    error = function(cond) {
      return("Error")
    }
  )

  id_error <- is.character(id_return)

  rm(urls)
  rm(api_url)

  cli::cli_h1(paste0("Downloading POLIS Data for: ", table_data$table))

  #If never downloaded before or if ID API doesn't work
  if ((is.na(table_data$last_sync) &
       !is.na(table_data$polis_id)) |
      id_error | is.na(table_data$polis_update_id)) {
    if (id_error) {
      cli::cli_alert_info(
        paste0(
          table_data$endpoint,
          " has been downloaded before but the ID API is not functional, downloading all data...checking size..."
        )
      )
    } else{
      if (is.na(table_data$polis_update_id)) {
        cli::cli_alert_info(
          paste0(
            table_data$endpoint,
            " does not have a unique timestamps for the update, the entire table must be downloaded..."
          )
        )
      } else{
        cli::cli_alert_info(
          paste0(
            table_data$endpoint,
            " has not been downloaded before...checking size..."
          )
        )
      }
    }
    table_size <- get_table_size(.table = table_data$table)
    cli::cli_alert_info(paste0("Getting ready to download ", table_size, " new rows of data!"))

    if (table_data$table %in% c("human_specimen",
                                "environmental_sample",
                                "activity",
                                "sub_activity",
                                "lqas",
                                "pop")) {
      urls <-
        create_table_urls(url = table_url,
                          table_size = table_size,
                          type = "lab")
    } else{
      urls <-
        create_table_urls(url = table_url,
                          table_size = table_size,
                          type = "full")
    }

    cli::cli_process_start("Downloading data")
    out <- call_urls(urls)
    update_polis_log(.event = paste0(
      "Downloaded ",
      table_size,
      " rows of ",
      table_data$table,
      " data"
    ),
    .event_type = "INFO")
    cli::cli_process_done()

    #update cache information
    cli::cli_process_start("Updating cache")
    if (is.na(table_data$polis_update_id)) {
      update_polis_cache(
        cache_file = Sys.getenv("POLIS_CACHE_FILE"),
        .table = .table,
        .nrow = nrow(out),
        .update_val = NA
      )
    } else{
      update_polis_cache(
        cache_file = Sys.getenv("POLIS_CACHE_FILE"),
        .table = .table,
        .nrow = nrow(out),
        .update_val = max(lubridate::as_datetime(dplyr::pull(out[table_data$polis_update_id])), na.rm = T)
      )
    }

    cli::cli_process_done()

    cli::cli_process_start("Writing data cache")
    tidypolis_io(obj = out, io = "write", file_path = paste0(
      Sys.getenv("POLIS_DATA_CACHE"),
      "/",
      table_data$table,
      ".rds"
    ))
    update_polis_log(.event = paste0(table_data$table, " data saved locally"),
                     .event_type = "PROCESS")
    cli::cli_process_done()

    gc()

  } else{
    if (!is.na(table_data$last_sync)) {
      #pull updated data
      #create new table url

      time_modifier <- paste0(
        "&$filter=",
        table_data$polis_update_id,
        " gt DateTime'",
        sub(" ", "T", as.character(table_data$polis_update_value)),
        "'"
      )

      time_modifier <- gsub(" ", "+", time_modifier,)

      table_size <-
        get_table_size(.table = table_data$table, extra_filter = time_modifier)

      cli::cli_alert_success(paste0(
        table_data$table,
        ": ",
        table_size,
        " new or updated records identified!"
      ))
      update_polis_log(
        .event = paste0(
          table_data$table,
          ": ",
          table_size,
          " new or updated records identified!"
        ),
        .event_type = "INFO"
      )

      if (table_size > 0) {
        table_url <- paste0(
          table_url,
          "?$filter=",
          table_data$polis_update_id,
          " gt DateTime'",
          sub(" ", "T", as.character(table_data$polis_update_value)),
          "'"
        )

        table_url <- gsub(" ", "+", table_url,)

        if (table_data$table %in% c(
          "human_specimen",
          "environmental_sample",
          "activity",
          "sub_activity",
          "lqas"
        )) {
          urls <-
            create_table_urls(url = table_url,
                              table_size = table_size,
                              type = "lab-partial")
        } else{
          urls <-
            create_table_urls(url = table_url,
                              table_size = table_size,
                              type = "partial")
        }

        cli::cli_process_start("Downloading data")
        out <- call_urls(urls)
        update_polis_log(.event = paste0(
          "Downloaded ",
          table_size,
          " rows of ",
          table_data$table,
          " data"
        ),
        .event_type = "INFO")
        cli::cli_process_done()

        #check ids and make list of ids to be deleted
        cli::cli_process_start("Getting table Ids")
        ids <-
          get_table_ids(.table = table_data$table, .id = table_data$polis_id)
        cli::cli_process_done()

        #load in cache
        cli::cli_process_start("Loading existing cache")
        old_cache <-
          tidypolis_io(io = "read", file_path = paste0(
            Sys.getenv("POLIS_DATA_CACHE"),
            "/",
            table_data$table,
            ".rds"
          ))
        cli::cli_process_done()
        old_cache_n <- nrow(old_cache)
        new_data_ids_in_old_cache <-
          sum(dplyr::pull(out[table_data$polis_id]) %in% dplyr::pull(old_cache[table_data$polis_id]))
        new_data_ids <- table_size - new_data_ids_in_old_cache
        deleted_ids <-
          dplyr::pull(old_cache[table_data$polis_id])[!dplyr::pull(old_cache[table_data$polis_id]) %in% ids]
        #old_data_ids_in_new <- dplyr::pull(old_cache[table_data$polis_id])[dplyr::pull(old_cache[table_data$polis_id]) %in% dplyr::pull(out[table_data$polis_id])]

        cli::cli_h3(paste0("'", table_data$table, "'", " table data"))
        cli::cli_bullets(c(
          "*" = paste0(table_size, " new rows of data downloaded"),
          "*" = paste0(old_cache_n, " rows of data available in old cache"),
          "*" = paste0(
            new_data_ids,
            " new ",
            table_data$polis_id,
            "s identified"
          ),
          "*" = paste0(new_data_ids_in_old_cache, " rows of data being updated"),
          "*" = paste0(length(deleted_ids), " rows of data were deleted")
        ))

        update_polis_log(
          .event = paste0(
            table_data$table,
            " - update - ",
            table_size,
            " new rows of data downloaded; ",
            old_cache_n,
            " rows of data available in old cache; ",
            new_data_ids,
            " new ",
            table_data$polis_id,
            "s identified; ",
            new_data_ids_in_old_cache,
            " rows of data being updated;",
            paste0(length(deleted_ids), " rows of data were deleted - "),
            paste0(deleted_ids, collapse = ", ")
          ),
          .event_type = "INFO"
        )

        #update cache
        old_cache <- old_cache |>
          dplyr::filter(!get(table_data$polis_id) %in% dplyr::pull(out[table_data$polis_id]))
        old_cache <-
          bind_and_reconcile(new_data = out, old_data = old_cache)

        #delete data that no longer exists in POLIS
        old_cache <- old_cache |>
          dplyr::filter(get(table_data$polis_id) %in% ids)

        #check for missed IDs, if IDs missed then redownload full table
        #create ids table in order to filter
        cli::cli_process_start("Checking for missed records in download")
        ids_table <- as.data.frame(ids)
        missed.id <- ids_table |>
          dplyr::filter(!ids %in% dplyr::pull(old_cache[table_data$polis_id]))
        cli::cli_process_done()

        #if there are missed IDs, clear old cache and re-download full table
        if(nrow(missed.id) > 0){

          cli::cli_alert_info(
            paste0(
              table_data$endpoint,
              " has been downloaded before but ",
              nrow(missed.id),
              " record(s) missing, downloading all data...checking size..."
            )
          )

          table_size <- get_table_size(.table = table_data$table)
          cli::cli_alert_info(paste0("Getting ready to download ", table_size, " new rows of data!"))

          table_url <- paste0(base_url, table_data$endpoint)

          if (table_data$table %in% c("human_specimen",
                                      "environmental_sample",
                                      "activity",
                                      "sub_activity",
                                      "lqas",
                                      "pop")) {
            urls <-
              create_table_urls(url = table_url,
                                table_size = table_size,
                                type = "lab")
          }else{
            urls <-
              create_table_urls(url = table_url,
                                table_size = table_size,
                                type = "full")
          }

          cli::cli_process_start("Downloading data")
          out <- call_urls(urls)
          update_polis_log(.event = paste0(
            "Downloaded ",
            table_size,
            " rows of ",
            table_data$table,
            " data"
          ),
          .event_type = "INFO")
          cli::cli_process_done()

          #update cache information
          cli::cli_process_start("Updating cache")
          if (is.na(table_data$polis_update_id)) {
            update_polis_cache(
              cache_file = Sys.getenv("POLIS_CACHE_FILE"),
              .table = .table,
              .nrow = nrow(out),
              .update_val = NA
            )
          } else{
            update_polis_cache(
              cache_file = Sys.getenv("POLIS_CACHE_FILE"),
              .table = .table,
              .nrow = nrow(out),
              .update_val = max(lubridate::as_datetime(dplyr::pull(out[table_data$polis_update_id])), na.rm = T)
            )
          }

          cli::cli_process_done()

          cli::cli_process_start("Writing data cache")
          tidypolis_io(obj = out, io = "write", file_path = paste0(
            Sys.getenv("POLIS_DATA_CACHE"),
            "/",
            table_data$table,
            ".rds"
          ))
          update_polis_log(.event = paste0(table_data$table, " data saved locally"),
                           .event_type = "PROCESS")
          cli::cli_process_done()

          gc()

          cli::cli_process_done()

        }else{

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
          tidypolis_io(obj = old_cache, io = "write",
                       file_path = paste0(
                         Sys.getenv("POLIS_DATA_CACHE"),
                         "/",
                         table_data$table,
                         ".rds"
                       ))
          update_polis_log(.event = paste0(table_data$table, " data saved locally"),
                           .event_type = "PROCESS")
          cli::cli_process_done()

          #garbage clean
          gc()

        }




      }

    }

  }

}

#' Get table size from POLIS
#'
#' @import httr jsonlite
#' @param .table str: Table to be downloaded
#' @param api_key str: API Key
#' @param cache_file str: Cache file location
#' @param extra_filter str: additional filtering parameters
#' @export
get_table_size <- function(.table,
                           api_key = Sys.getenv("POLIS_API_KEY"),
                           cache_file = Sys.getenv("POLIS_CACHE_FILE"),
                           extra_filter = "") {
  table_data <- get_polis_cache(.table = .table)

  # disable SSL Mode
  httr::set_config(httr::config(ssl_verifypeer = 0L))

  # Variables: URL, Token, Filters, ...
  polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

  api_url <-
    paste0(
      polis_api_root_url,
      table_data$endpoint,
      "?$inlinecount=allpages&$top=0",
      extra_filter
    )

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

  #tibble::as_tibble(out$value)


  table_size <- response |>
    httr::content(type = 'text', encoding = 'UTF-8') |>
    jsonlite::fromJSON()

  table_size <- as.integer(table_size$odata.count)

  return(table_size)

}

#' Get Ids
#'
#' @description return Ids availalbe in table
#' @import cli dplyr httr
#' @param .table str: table
#' @param .id str: id variable
#' @param api_key str: POLIS API Key
#' @return character array of ids
get_table_ids <-
  function(.table, .id, api_key = Sys.getenv("POLIS_API_KEY")) {
    cli::cli_process_start(paste0("Downloading ", .table, " table IDs"))

    table_data <- get_polis_cache(.table = .table)

    # disable SSL Mode
    httr::set_config(httr::config(ssl_verifypeer = 0L))

    # Variables: URL, Token, Filters, ...
    polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

    api_url <-
      paste0(polis_api_root_url,
             table_data$endpoint,
             "?$select=",
             table_data$polis_id)

    table_size <- get_table_size(.table = .table)

    if (table_data$table %in% c("human_specimen",
                                "environmental_sample",
                                "activity",
                                "sub_activity",
                                "lqas")) {
      urls <-
        create_table_urls(url = api_url,
                          table_size = table_size,
                          type = "lab-partial")
    } else{
      urls <-
        create_table_urls(url = api_url,
                          table_size = table_size,
                          type = "partial")
    }

    ids <- call_urls(urls) |>
      dplyr::pull(table_data$polis_id)

    gc()

    cli::cli_process_done()

    return(ids)

  }

#### POLIS API ####

#' Test out if POLIS key is valid
#'
#' @description Test POLIS API Key
#' @import httr
#' @param key str: POLIS API Key
#' @returns boolean
#' @export
test_polis_key <- function(key) {
  # Variables: URL, Token, Filters, ...
  polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

  api_url <- paste0(polis_api_root_url, "$metadata")

  # connect to the API and Get data
  get_result <-
    httr::GET(api_url, httr::add_headers("authorization-token" = key))

  # Display the status which should be 200 (OK)
  return(httr::status_code(get_result) == 200)

}

#' Call multiple URLs
#'
#' @description Call multiple URLs
#' @import dplyr foreach future doFuture
#' @importFrom progressr progressor
#' @importFrom progressr with_progress
#' @importFrom progressr handlers
#' @param urls array of url strings
#' @return tibble with all data
call_urls <- function(urls) {
  doFuture::registerDoFuture() ## tell foreach to use futures
  future::plan(future::multisession) ## parallelize over a local PSOCK cluster
  options(doFuture.rng.onMisuse = "ignore")
  xs <- 1:length(urls)

  progressr::handlers("cli")

  progressr::with_progress({
    p <- progressr::progressor(along = xs)
    y <-
      foreach::`%dopar%`(foreach::foreach(
        x = xs,
        .packages = c("tidypolis", "tibble", "jsonlite", "httr")
      ), {
        # signal a progression update
        p()
        # jitter the parallel calls to not overwhelm the server
        #Sys.sleep(1 + stats::rpois(1, 10)/100)
        call_single_url(urls[x])
      })
  })

  y <- dplyr::bind_rows(y)
  gc()
  return(y)

}


#' Call single URL
#' @description Call a return the formatted output frome one URL
#' @import tibble jsonlite httr
#' @param url str: single url
#' @param api_key str: validated API key
#' @param times int: number of times to attempt connection with API
#' @export
#' @return tibble
call_single_url <- function(url,
                            api_key = Sys.getenv("POLIS_API_KEY"),
                            times = 10) {
  # disable SSL Mode
  httr::set_config(httr::config(ssl_verifypeer = 0L))

  #response <- httr::GET(url=url, httr::add_headers("authorization-token" = api_key))

  response <- httr::RETRY(
    verb = "GET",
    url = url,
    config = httr::add_headers("authorization-token" = api_key),
    times = times,
    quiet = TRUE,
    terminate_on_success = TRUE
  )

  out <- jsonlite::fromJSON(rawToChar(response$content))

  tibble::as_tibble(out$value)

  #Sys.sleep(1.25)

}

#' Run single table diagnostic
#'
#' @description Run single table diagnostic
#' @import httr tibble
#' @param .table str: table name
#' @param key str: POLIS API Key
#' @returns tibble with diagnostic data
run_single_table_diagnostic <-
  function(.table, key =  Sys.getenv("POLIS_API_Key")) {
    base_url <- "https://extranet.who.int/polis/api/v2/"
    table_data <- get_polis_cache(.table = .table)
    table_url <- paste0(base_url, table_data$endpoint)
    table_size <- get_table_size(.table = .table)

    if (table_data$table %in% c("human_specimen",
                                "environmental_sample",
                                "activity",
                                "sub_activity",
                                "lqas")) {
      urls <-
        create_table_urls(url = table_url,
                          table_size = table_size,
                          type = "lab")
    } else{
      urls <-
        create_table_urls(url = table_url,
                          table_size = table_size,
                          type = "full")
    }

    data_url <- urls[1]


    # disable SSL Mode
    httr::set_config(httr::config(ssl_verifypeer = 0L))

    # Variables: URL, Token, Filters, ...
    polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

    api_url <-
      paste0(polis_api_root_url,
             table_data$endpoint,
             "?$select=",
             table_data$polis_id)

    if (table_data$table %in% c("human_specimen",
                                "environmental_sample",
                                "activity",
                                "sub_activity",
                                "lqas")) {
      urls <-
        create_table_urls(url = api_url,
                          table_size = table_size,
                          type = "lab-partial")
    } else{
      urls <-
        create_table_urls(url = api_url,
                          table_size = table_size,
                          type = "partial")
    }

    id_url <- urls[1]

    tick <- Sys.time()
    data_return <- tryCatch(
      call_single_url(data_url, times = 1),
      error = function(cond) {
        return("Error")
      }
    )
    tock <- Sys.time()
    data_time <- tock - tick

    tick <- Sys.time()
    id_return <- tryCatch(
      call_single_url(id_url, times = 1),
      error = function(cond) {
        return("Error")
      }
    )
    tock <- Sys.time()
    id_time <- tock - tick

    return(
      tibble::tibble(
        "table" = .table,
        "data" = ifelse(is.data.frame(data_return), "Success", "Error"),
        "data_time" = data_time,
        "id" = ifelse(is.data.frame(id_return), "Success", "Error"),
        "id_time" = id_time
      )
    )


  }


#### Logging ####

#' Update local POLIS interaction log
#'
#' @description Update the POLIS log
#' @import readr tibble
#' @param log_file str: location of cache file
#' @param .time dttm: time of update
#' @param .user double: user who conducted the action
#' @param .event_type str: INIT, START, PROCESS, INFO, ERROR, ALERT, END
#' @param .event str: event to be logged
#' @returns Return true if cache updated
update_polis_log <- function(log_file = Sys.getenv("POLIS_LOG_FILE"),
                             .time = Sys.time(),
                             .user = Sys.getenv("USERNAME"),
                             .event_type = "INIT",
                             .event){

  log_file_path <- log_file
  log_file <- tidypolis_io(io = "read", file_path = log_file_path)

  log_names <- log_file |>
    names()

  if(!"event_type" %in% log_names){

    log_file |>
      cbind(event_type = NA) |>
      tibble::add_row(time = .time,
                      user = .user,
                      event_type = "INFO",
                      event = "Updating log format") |>
      tibble::add_row(time = .time,
                      user = .user,
                      event_type = .event_type,
                      event = .event) |>
      tidypolis_io(io = "write", file_path = log_file_path)

  }else{

  log_file |>
    tibble::add_row(time = .time,
                    user = .user,
                    event_type = .event_type,
                    event = .event) |>
    tidypolis_io(io = "write", file_path = log_file_path)


    }
}

#### Local Cache ####

#' Load local POLIS cache
#'
#' @description Pull cache data for a particular table
#' @import readr cli dplyr
#' @param cache_file str: location of cache file
#' @param .table str: table to be loaded
#' @returns Return tibble with table information
get_polis_cache <- function(cache_file = Sys.getenv("POLIS_CACHE_FILE"),
                            .table) {
  cache <- tidypolis_io(io = "read", file_path = cache_file)

  if (.table %in% dplyr::pull(cache, table)) {
    cache |>
      dplyr::filter(table == .table)
  } else{
    cli::cli_alert_warning(paste0("No entry found in the cache table for: ", .table))
  }


}


#' Update local POLIS cache
#'
#' @description Update the POLIS cache directory
#' @import readr dplyr lubridate
#' @param cache_file str: location of cache file
#' @param .table str: table to be updated
#' @param .nrow double: nrow of table to be updated
#' @param .update_val str: value to update data
#' @returns Return true if cache updated
update_polis_cache <- function(cache_file = Sys.getenv("POLIS_CACHE_FILE"),
                               .table,
                               .nrow,
                               .update_val) {
  tidypolis_io(io = "read", file_path = cache_file) |>
    dplyr::mutate(
      nrow = ifelse(table == .table, .nrow, nrow),
      last_sync = ifelse(
        table == .table,
        lubridate::as_datetime(Sys.time()),
        last_sync
      ),
      last_user = ifelse(table == .table, Sys.getenv("USERNAME"), last_user),
      polis_update_value = ifelse(
        table == .table,
        lubridate::as_datetime(.update_val),
        polis_update_value
      ),
      last_sync = lubridate::as_datetime(last_sync),
      polis_update_value = lubridate::as_datetime(polis_update_value)
    ) |>
    tidypolis_io(io = "write", file_path = cache_file)

}


#### Misc ####

#' Modified readlines function to simplify user interface
#'
#' @description Manage input
#' @param request str:
#' @param error_resp str:
#' @param vals array:
#' @param max_i int:
#' @returns val from `vals` or stops output
request_input <- function(request,
                          vals,
                          error_resp = "Valid input not chosen",
                          max_i = 3) {
  message(request)

  i <- 1

  vals_str <- paste0("Please choose one of the following [",
                     paste0(as.character(vals), collapse = "/"),
                     "]:  \n")

  val <- readline(prompt = vals_str)

  while (!val %in% vals) {
    val <- readline(prompt = vals_str)
    i <- i + 1
    if (i == max_i) {
      stop(error_resp)
    }
  }

  return(val)

}






#' Create table URLs
#'
#' @description create urls from table size and base url
#' @param url str: base url to be queried
#' @param table_size int: integer of download
#' @param type str: "full" or "partial"
#' @returns array of urls
create_table_urls <- function(url,
                              table_size,
                              type) {
  prior_scipen <- getOption("scipen")
  options(scipen = 999)

  if (sum(type %in% c("full", "partial", "lab", "lab-partial")) > 0) {
    if (type == "full") {
      urls <-
        paste0(url, "?$top=2000&$skip=", as.character(seq(0, as.numeric(table_size), by = 2000)))
    }

    if (type == "partial") {
      urls <-
        paste0(url, "&$top=2000&$skip=", seq(0, as.numeric(table_size), by = 2000))
    }

    if (type == "lab") {
      urls <-
        paste0(url, "?$top=1000&$skip=", as.character(seq(0, as.numeric(table_size), by = 1000)))
    }

    if (type == "lab-partial") {
      urls <-
        paste0(url, "&$top=1000&$skip=", seq(0, as.numeric(table_size), by = 1000))
    }

  }
  return(urls)

}


#' Reconcile classes and bind two tibbles
#'
#' @param new_data tibble: Tibble to be converted and bound
#' @import tibble dplyr
#' @param old_data tibble: Tibble to be referenced
#' @returns tibble: bound tibble
bind_and_reconcile <- function(new_data, old_data) {
  old_names <- names(old_data)
  classes_old <- sapply(old_data, class)
  new_data <- as.data.frame(new_data)
  old_data <- as.data.frame(old_data)

  for (name in old_names) {
    class(new_data[, name]) <- classes_old[name][[1]]

  }

  new_data <- tibble::as_tibble(new_data)
  old_data <- tibble::as_tibble(old_data)

  return(dplyr::bind_rows(old_data, new_data))

}


#' Set up local credentials file
#'
#' @description Create creds file
#' @import yaml
#' @param polis_data_folder str: location of POLIS data folder
#' @returns boolean for folder creation
create_cred_file <- function(polis_data_folder) {
  list("polis_api_key" = "",
       "polis_data_folder" = "") |>
    yaml::write_yaml(file = file.path(polis_data_folder, "creds.yaml"))
}

#' Rename variables via crosswalk
#'
#' @description Rename variables in tibble using crosswalk data
#' @import dplyr
#' @param api_data tibble: data pulled from api
#' @param crosswalk tibble: crosswalk file loaded through rio
#' @param table_name str: name of table to be crosswalked
#' @returns tibble: renamed table
rename_via_crosswalk <- function(api_data,
                                 crosswalk,
                                 table_name) {
  crosswalk_sub1 <- crosswalk |>
    dplyr::filter(Table == !!table_name) |>
    dplyr::filter(!is.na(API_Name))
  for (i in 1:nrow(crosswalk_sub1)) {
    api_name <- crosswalk_sub1$API_Name[i]
    web_name <- crosswalk_sub1$Web_Name[i]
    if (!is.na(web_name) & web_name != "") {
      api_data <- api_data |>
        dplyr::rename({{web_name}}:={{api_name}})

    }
  }
  return(api_data)
}

#' Remove empty columns
#'
#' @description Utility function to return tibble without empty columns
#' @param dataframe tibble: df
#' @import dplyr cli
#' @returns tibble: without any empty columns
remove_empty_columns <- function(dataframe) {

  original_df <- dataframe

  dataframe <- dataframe |>
    mutate(across(everything(), as.character))

  empty_cols <- colnames(dataframe)[colSums(is.na(dataframe) | dataframe == "") == nrow(dataframe)]

  if(length(empty_cols) > 0 ){
    cli::cli_alert_info(paste0("The following variables had empty columns: ", paste0(empty_cols, collapse = ", ")))
  }else{
    cli::cli_alert_info("No empty columns were detected")
  }
  return(
    original_df |>
      dplyr::select(-empty_cols)
  )

}


#' Get crosswalk data
#'
#' @description
#' Get all data from crosswalk location
#' @param file_loc str: location of crosswalk file
#' @import dplyr cli
#' @return tibble: crosswalk data
get_crosswalk_data <- function(
    file_loc = "Data/misc/crosswalk.rds"
  ){
  cli::cli_process_start("Import crosswalk")
  crosswalk <-
    sirfunctions::edav_io(io = "read", file_loc = file_loc) |>
    #TrendID removed from export
    dplyr::filter(!API_Name %in% c("Admin0TrendId", "Admin0Iso2Code"))
  cli::cli_process_done()
  return(crosswalk)
}

#' Check if sets of columns are the same and stop if not
#'
#' @description
#' Check if sets of names are the same and return stop error if not
#' @import cli
#' @param old str/array: array of character column names
#' @param new str/array: array of character colunm names
#' @returns str: Stop error or continue
f.compare.dataframe.cols <- function(old, new) {
  cli::cli_process_start("Checking column names")
  if (length(setdiff(colnames(new), colnames(old))) >= 1 | length(setdiff(colnames(old), colnames(new))) >= 1) {
    cli::cli_alert_danger(text = "WARNING there is a new or removed column in the data. Please investigate this new column")

    new.var <- setdiff(names(new), names(old))
    new.var <- ifelse(length(new.var) == 0, "NULL", new.var)
    lost.var <- setdiff(names(old), names(new))
    lost.var <- ifelse(length(lost.var) == 0, "NULL", lost.var)

    update_polis_log(.event = paste0("New Var: ", new.var, ", Removed Var: ", lost.var),
                     .event_type = "ALERT")


  } else {cli::cli_alert_info("No differences found in column names")}
}

#' Compare downloaded data
#' @description Compared downloade data
#' @import dplyr purrr
#' @param old.download tibble
#' @param new.download tibble
#' @returns tibble: variables that are new or unaccounted for
f.download.compare.01 <- function(old.download, new.download) {

  # Create dataframe from old download
  old.01 <- old.download |>
    dplyr::mutate_all( ~ as.character(.)) |>
    dplyr::mutate_all( ~ stringr::str_trim(., side = "both")) |>
    dplyr::mutate_all(~ dplyr::na_if(., "")) |>
    # below would strip data by column find distinct values by column
    # then rbind. So variables become rows. Makes it easier to read
    purrr::map_df(~ (data.frame(old.distinct.01 = n_distinct(.x))),
           .id = "variable"
    )

  # Create dataframe from combining old and new data
  combine.01 <- dplyr::bind_rows(old.download, new.download) |>
    dplyr::mutate_all(as.character) |>
    dplyr::mutate_all(~ stringr::str_trim(., side = "both")) |>
    dplyr::mutate_all(~ dplyr::na_if(., "")) |>
    # below would strip data by column find distinct values by column
    # then rbind. So variables become rows. Makes it easier to read
    purrr::map_df(~ (data.frame(combine.distinct.01 = dplyr::n_distinct(.x))),
           .id = "variable"
    )

  # This would give the total variables including new
  # and number of distinct values in each download
  compare.01 <- dplyr::full_join(combine.01, old.01, by = "variable")

  rm(combine.01, old.01)

  # Now compare across the two downloads keeping
  # only new variables or variables in which distinct values have changed

  new.var.or.new.distinct.char.01 <- compare.01 |>
    dplyr::mutate(diff.distinct.01 = combine.distinct.01 - old.distinct.01) |>
    dplyr::filter(diff.distinct.01 > 0 | is.na(old.distinct.01))

  rm(compare.01)

  return(new.var.or.new.distinct.char.01)
}

#' THIS FUNCTION WOULD LIST OUT THE DISTINCT VALUES BY VARIABLE
#' ALL VALUES FOR NEW VARIABLE AND NEW VALUES FOR EXISTING
#' @description List out distinct values that are not the same by variable
#' @import dplyr purrr purrr
#' @param df.from.f.download.compare.01 tibble: output from f.download.compare.01
#' @param old.download tibble
#' @param new.download tibble
#' @returns tibble of variables to compare
f.download.compare.02 <- function(df.from.f.download.compare.01, old.download, new.download) {

  # dataframe of new values of existing variables
  new.distinct <- df.from.f.download.compare.01 |>
    dplyr::filter(diff.distinct.01 > 0)

  # dataframe of new variables
  new.var.01 <- df.from.f.download.compare.01 |>
    dplyr::filter(is.na(old.distinct.01))

  x <- new.distinct |>
    dplyr::pull(variable)

  y <- new.var.01 |>
    dplyr::pull(variable)

  old.download <- old.download  |>
    dplyr::mutate_all(as.character)

  new.download <- new.download |>
    dplyr::mutate_all(as.character)

  # identify the names of the variables which were
  # existing before but have new values. Use the names as indicator list to
  # strip old and combined data by column. Do merge by those two columns
  # and make a list of dataframes. The size of list is equal to number of variables
  # with new distinct values

  new.distinct.value.01 <- purrr::map(x,
                                      ~ dplyr::anti_join(
                                        (
                                          dplyr::bind_rows(old.download, new.download) |> dplyr::select(x) |> dplyr::distinct()
                                        ),
                                        (old.download |> dplyr::select(x) |> dplyr::distinct())
                                      ) |> dplyr::mutate(id = dplyr::row_number()))

  # Make a dataframe from list of dataframe
  new.distinct.value.01.df <- purrr::reduce(new.distinct.value.01, full_join)

  # Identify distinct values of new variable
  new.distinct.var.01 <- dplyr::bind_rows(old.download, new.download) |>
    dplyr::select(y) |>
    dplyr::distinct() |>
    dplyr::mutate(id = dplyr::row_number())

  # final dataframe with all levels of new variable and new distinct values of existing
  # variable
  final.df.01 <- dplyr::full_join(new.distinct.value.01.df, new.distinct.var.01) |>
    dplyr::select(-id)

  return(final.df.01)
}

#' Sample points for missing lat/lon
#' @description Create random samples of points for missing GPS data
#' @import dplyr sf tidyr tibble
#' @param df01 tibble: table of afp data
#' @param global.dist.01 sf: spatial file of all locations
#' @returns tibble with lat/lon for all unsampled locations
f.pre.stsample.01 <- function(df01, global.dist.01) {

  df01.noshape <- dplyr::anti_join(df01, global.dist.01, by=c("Admin2GUID"="GUID"))

  df01.shape <- dplyr::right_join(global.dist.01, df01 |> dplyr::filter(!is.na(Admin2GUID)), by=c("GUID"="Admin2GUID"))

  df01.shape$empty.01 <- sf::st_is_empty(df01.shape)
  df01.shape <- dplyr::filter(df01.shape, !empty.01)

  df02.ref <- df01.shape |>
    tibble::as_tibble() |>
    dplyr::group_by(GUID) |>
    dplyr::summarise(nperarm = dplyr::n()) |>
    dplyr::arrange(GUID) |>
    dplyr::mutate(id = dplyr::row_number()) |>
    dplyr::ungroup()

  df02 <- global.dist.01 |>
    dplyr::select(GUID) |>
    dplyr::filter(GUID %in% df02.ref$GUID) |>
    dplyr::left_join(df02.ref, by = "GUID")

  cli::cli_process_start("Placing random points")
 # pt01 <- suppressMessages(sf::st_sample(df02, df02$nperarm,
  #                                       exact = T, progress = T))

  pt01 <- lapply(1:nrow(df02), function(x){

    tryCatch(
      expr = {suppressMessages(sf::st_sample(df02[x,], pull(df02[x,], "nperarm"),
                                            exact = T)) |> st_as_sf()},
      error = function(e) {
        guid = df02[x, ]$GUID[1]
        ctry_prov_dist_name = global.dist.01 |> filter(GUID == guid) |> select(ADM0_NAME, ADM1_NAME, ADM2_NAME)
        cli::cli_alert_warning(paste0("Fixing errors for:\n",
                                      "Country: ", ctry_prov_dist_name$ADM0_NAME,"\n",
                                      "Province: ", ctry_prov_dist_name$ADM1_NAME, "\n",
                                      "District: ", ctry_prov_dist_name$ADM2_NAME))

        suppressWarnings(
          {
          sf_use_s2(F)
          int <- df02[x,] |> st_centroid(of_largest_polygon = T)
          sf_use_s2(T)

          st_buffer(int, dist = 3000) |>
                   st_sample(slice(df02, x) |>
                               pull(nperarm)) |>
                   st_as_sf()
          }
        )

      }
    )

  }) |>
    bind_rows()

  cli::cli_process_done()

  pt01_sf <- pt01

  pt01_joined <- dplyr::bind_cols(
    pt01_sf,
    df02 |>
      tibble::as_tibble() |>
      dplyr::select(GUID, nperarm) |>
      tidyr::uncount(nperarm)
  ) |>
    dplyr::left_join(tibble::as_tibble(df02) |> dplyr::select(-SHAPE), by = "GUID")



  #pt01_joined <- sf::st_join(pt01_sf, df02)

  pt01_joined <- dplyr::bind_cols(
    tibble::as_tibble(pt01_joined),
    sf::st_coordinates(pt01_joined) |>
      tibble::as_tibble() |>
      dplyr::rename("lon" = "X", "lat" = "Y")
  )

  df03 <- pt01_joined |>
    tibble::as_tibble() |>
    dplyr::select(-nperarm, -id) |>
    dplyr::group_by(GUID)|>
    dplyr::arrange(GUID, .by_group = TRUE) |>
    dplyr::mutate(id = dplyr::row_number()) |>
    as.data.frame()

  df04 <- df01.shape |>
    dplyr::group_by(GUID)|>
    dplyr::arrange(GUID, .by_group = TRUE) |>
    dplyr::mutate(id = dplyr::row_number())

  df05 <- dplyr::full_join(df04, df03, by = c("GUID", "id"))

  sf::st_geometry(df05) <- NULL

  df06 <- dplyr::bind_rows(df05, df01.noshape)

  return(df06)
}


#' Function for data qa check in AFP line list cleaning
#' @description function creates a new variable when combined with a mutate statement in R code
#' @import dplyr
#' @param date1 date 1 is the date to be checked against date2
#' @param date2 date 2 is the date of onset such that the date should be after onset
#' @returns quality controlled date variable
f.datecheck.onset <- function(date1, date2) {
  date1.qa <-
    case_when(
      is.na(date1) == T ~ "date missing",
      is.na(date2) == T ~ "date onset missing",
      abs(date1 - date2) > 365 ~ "data entry error",
      date1 < date2 ~ "date before onset",
      (date1 - date2) <= 365 ~ "useable date"
    )


  return(date1.qa)
}

#' Compare meta data outputs between two datasets
#' @description compare meta data outputs between two datasets
#' @import dplyr
#' @param new_table_metadata tibble
#' @param old_table_metadata tibble
#' @param table str: "AFP", "Other Surv", "SIA", "ES", "POS"
#' @returns meta data comparisons
f.compare.metadata <- function(new_table_metadata, old_table_metadata, table){
  #compare to old metadata
  compare_metadata <- new_table_metadata |>
    dplyr::full_join(old_table_metadata, by=c("var_name"))

  new_vars <- (compare_metadata |>
                 dplyr::filter(is.na(var_class.y)))$var_name

  if(length(new_vars) != 0){
    new_vars <- new_vars
    warning(print("There are new variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'new_vars'"))

    update_polis_log(.event = paste0(table, " - ", "New Var(s): ", paste(new_vars, collapse = ", ")),
                     .event_type = "ALERT")

  }

  lost_vars <- (compare_metadata |>
                  filter(is.na(var_class.x)))$var_name

  if(length(lost_vars) != 0){
    lost_vars <- lost_vars
    warning(print("There are missing variables in the POLIS table\ncompared to when it was last retrieved\nReview in 'lost_vars'"))

    update_polis_log(.event = paste0(table, " - ", "Lost Var(s): ", paste(lost_vars, collapse = ", ")),
                     .event_type = "ALERT")

  }

  if(length(new_vars) == 0 & length(lost_vars) == 0){
    update_polis_log(.event = paste0(table, " - ", "No new or lost variables"),
                     .event_type = "INFO")
  }

  class_changed_vars <- compare_metadata |>
    dplyr::filter(!(var_name %in% lost_vars) &
             !(var_name %in% new_vars) &
             (var_class.x != var_class.y &
                !is.null(var_class.x) & !is.null(var_class.y))) |>
    dplyr::select(-c(categorical_response_set.x, categorical_response_set.y)) |>
    dplyr::rename(old_var_class = var_class.y,
           new_var_class = var_class.x)

  if(nrow(class_changed_vars) != 0){
    class_changed_vars <- class_changed_vars
    warning(print("There are variables in the POLIS table with different classes\ncompared to when it was last retrieved\nReview in 'class_changed_vars'"))

    update_polis_log(.event = paste0(table, " - ", "Variables changed class: ", class_changed_vars))
  }

  #Check for new responses in categorical variables (excluding new variables and class changed variables that have been previously shown)
  new_response <- compare_metadata |>
    dplyr::filter(!(var_name %in% lost_vars) &
             !(var_name %in% new_vars) &
             !(var_name %in% class_changed_vars$var_name) &
             as.character(categorical_response_set.x) != "NULL" &
             as.character(categorical_response_set.y) != "NULL") |>
    dplyr::rowwise() |>
    dplyr::mutate(same = toString(intersect(categorical_response_set.x, categorical_response_set.y)),
           in_old_not_new = toString(setdiff(categorical_response_set.y, categorical_response_set.x)),
           in_new_not_old = toString(setdiff(categorical_response_set.x, categorical_response_set.y))) |>
    dplyr::filter(in_new_not_old != "") |>
    dplyr::rename(old_categorical_response_set = categorical_response_set.x,
           new_categorical_response_set = categorical_response_set.y) |>
    dplyr::select(var_name, old_categorical_response_set, new_categorical_response_set, same, in_old_not_new, in_new_not_old)

  if(nrow(new_response) != 0){
    new_response <- new_response
    warning(print("There are categorical responses in the new table\nthat were not seen when it was last retrieved\nReview in 'new_response'"))
  }

  change_summary <- list(new_response = new_response, class_changed_vars = class_changed_vars, lost_vars = lost_vars, new_vars = new_vars)
  return(change_summary)
}

#' Summarize metadata from a tibble
#' @description Summarize metadata from tibble
#' @import skimr tidyselect
#' @param dataframe tibble
#' @param categorical_max int: maximum number of categories considered
#' @returns tibble: metadata
f.summarise.metadata <- function(dataframe, categorical_max = 10){
  #ungroup dataframe
  dataframe <- dataframe |>
    dplyr::ungroup()

  #summarise var names and classes
  var_name_class <- skimr::skim(dataframe) |>
    dplyr::select(skim_type, skim_variable, character.n_unique)

  var_name_class <- tibble::tibble(
    "var_name" = var_name_class$skim_variable,
    "var_class" = var_name_class$skim_type,
    "character.n_unique" = var_name_class$character.n_unique
  )

  #categorical sets: for categorical variables with <= n unique values, get a list of unique values
  categorical_vars <- dataframe |>
    dplyr::select(var_name_class$var_name[var_name_class$character.n_unique <= categorical_max  & !is.na(var_name_class$character.n_unique)]) |>
    tidyr::pivot_longer(cols=tidyselect::everything(), names_to="var_name", values_to = "response") |>
    unique() |>
    tidyr::pivot_wider(names_from=var_name, values_from=response) |>
    tidyr::pivot_longer(cols=tidyselect::everything(), names_to="var_name", values_to="categorical_response_set")

  #Combine var names/classes/categorical-sets into a 'metadata table'
  table_metadata <- var_name_class |>
    dplyr::select(-character.n_unique) |>
    dplyr::left_join(categorical_vars, by=c("var_name"))

  return(table_metadata)
}

#' Function to get cached env site data
#' @description Function to get cached env site data
#' @import readr
#' @returns tibble: env site list
get_env_site_data <- function(){
  envSiteYearList <- sirfunctions::edav_io(io = "read", file_loc = "Data/misc/env_sites.rds")
  return(envSiteYearList)
}


#' Function to read and report on latest log file entries
#'
#' @description Read log entries from the latest download and preprocessing run, create report to send to team
#' @import dplyr readr sirfunctions
#' @param log_file str: location of POLIS log file
#' @param polis_data_folder str: location of the POLIS data folder

log_report <- function(log_file = Sys.getenv("POLIS_LOG_FILE"),
                       polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")){

  log <- tidypolis_io(io = "read", file_path = log_file)

  last_start <- log |>
    dplyr::filter(event_type == "START") |>
    dplyr::arrange(desc(time)) |>
    dplyr::slice(1) |>
    dplyr::pull(time)

  last_end <- log |>
    dplyr::filter(event_type == "END") |>
    dplyr::arrange(desc(time)) |>
    dplyr::slice(1) |>
    dplyr::pull(time)

  latest_run <- log |>
    dplyr::filter(time >= last_start & time <= last_end)

  report_info <- latest_run |>
    dplyr::filter(event_type == "INFO" & !grepl("records identified!", event)) |>
    dplyr::mutate(event = ifelse(grepl(" - update -", event), sub("deleted.*", "deleted", event), event))|>
    dplyr::pull(event) |>
    sapply(function(x){paste0(x, "; ")}, USE.NAMES = F) |>
    paste0(collapse = "") %>%
    {paste0(.)}

  report_alert <- latest_run |>
    dplyr::filter(event_type == "ALERT") |>
    dplyr::pull(event) |>
    sapply(function(x){paste0(x, "; ")}, USE.NAMES = F) |>
    paste0(collapse = "") %>%
    {paste0(.)}


  #csv files to attach to report
  changed.virus.type <- tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/Core_Ready_Files/Changed_virustype_virusTableData.csv"))
  change.virus.class <- tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/Core_Ready_Files/virus_class_changed_date.csv"))
  new.virus.records <- tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/Core_Ready_Files/in_new_not_old_virusTableData.csv"))

  file1 <- tempfile(fileext = ".csv")
  file2 <- tempfile(fileext = ".csv")
  file3 <- tempfile(fileext = ".csv")

  readr::write_csv(x = changed.virus.type, file = file1)
  readr::write_csv(x = change.virus.class, file = file2)
  readr::write_csv(x = new.virus.records, file = file3)

  #coms section
  sirfunctions::send_teams_message(msg = paste0("New CORE data files info: ", report_info))
  sirfunctions::send_teams_message(msg = paste0("New CORE data files alerts: ", report_alert))
  sirfunctions::send_teams_message(msg = "Attached CSVs contain information on new/changed virus records", attach = c(file1, file2, file3))

}


#' function to archive log report
#'
#' @description
#' Function to read in log file and archive entries older than 3 months
#'
#' @import dplyr readr
#' @param log_file str: location of POLIS log file
#' @param polis_data_folder str: location of the POLIS data folder

archive_log <- function(log_file = Sys.getenv("POLIS_LOG_FILE"),
                        polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")){

  #create log archive

  archive.path <- file.path(polis_data_folder, "Log_Archive")

  flag.log.exists <- tidypolis_io(io = "exists.dir", file_path = archive.path)

  if(!flag.log.exists){
    tidypolis_io(io = "create", file_path = file.path(polis_data_folder, "Log_Archive"))
  }

  log <- tidypolis_io(io = "read", file_path = log_file)

  log.time.to.arch <- log |>
    dplyr::filter(event_type == "END") |>
    dplyr::arrange(desc(time)) |>
    dplyr::slice(10) |>
    dplyr::pull(time)

  log.to.arch <- log |>
    dplyr::filter(time <= log.time.to.arch)

  log.current <- log |>
    dplyr::filter(time > log.time.to.arch)

  #check existence of archived log and either create or rbind to it
  flag.log.exists <- tidypolis_io(io = "exists.file", file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds"))

  ifelse(!flag.log.exists,
         tidypolis_io(io = "write", obj = log.to.arch, file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds")),
         tidypolis_io(io = "read", file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds")) |>
           dplyr::bind_rows(log.to.arch) |>
           tidypolis_io(io = "write", file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds"))
         )
}

#' function to remove original character formatted date vars from data tables
#'
#' @description
#' remove original date variables from POLIS tables
#' @import dplyr
#' @param type str: the table on which to remove original date vars, "AFP", "ES", "POS"
#' @param df tibble: the dataframe from which to remove character formatted dates
#' @param polis_data_folder str:  location of user's polis data folder
#' @return outputs a saved reference table of original date vars and a smaller
#' core ready file without character dates
remove_character_dates <- function(type,
                                   df,
                                   polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")){

  if(type %in% c("AFP", "POS")){
    df.01 <- df |>
      dplyr::select(epid, (dplyr::contains("date") & dplyr::where(is.character)))

    df.02 <- df |>
      dplyr::select(!(dplyr::contains("date") & dplyr::where(is.character)))
  }

  if(type == "ES"){
    df.01 <- df |>
      dplyr::select(env.sample.manual.edit.id, env.sample.id, (dplyr::contains("date") & dplyr::where(is.character)))

    df.01.fixed <- df.01 |>
      dplyr::mutate(dplyr::across(dplyr::contains("date") & dplyr::where(is.character),
                    ~lubridate::parse_date_time(., c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S"))))

    df.02 <- df |>
      dplyr::select(env.sample.manual.edit.id, env.sample.id, !(dplyr::contains("date") & dplyr::where(is.character))) |>
      dplyr::left_join(df.01.fixed)

  }

  tidypolis_io(io = "write", obj = df.01, file_path = paste0(polis_data_folder, "/Core_Ready_Files/", type, "_orig_char_dates.rds"))

  return(df.02)
}


#Cluster Function
#this function identifies "cluster" or OBX response so we can identify rounds
#' @export
#' @import dplyr stats cluster
#' @param x df: data to be clustered
#' @param seed num
#' @param method str cluster method to use, can be "kmeans" or "mindate"
#' @param grouping_days int:
cluster_dates <- function(x,
                          seed = 1234,
                          method = "kmeans",
                          grouping_days = 365){

  if(method == "kmeans"){
    #prepare the data
    y <- x |>
      #select only dates
      dplyr::select(date = sub.activity.start.date) |>
      #calculate distance from minimum date
      dplyr::mutate(date = as.numeric(date - min(date))) |>
      #normalize values for clustering
      scale()

    set.seed(seed)
    #calculate the optimal number of clusters
    optim_k <- y %>%
      #calculate optimal number of clusters using the
      #gap statistic
      {cluster::clusGap(., FUN = stats::kmeans, nstart = 25, K.max = max(min(nrow(.)-1, nrow(.)/2), 2), B = 100)} %>%
      #extract gap statistic matrix
      {.$Tab[,"gap"]} %>%
      #calculate the max gap statistic, given the sparsity in the data
      #am not limiting to the first max SE method
      which.max()

    set.seed(seed)
    #calculate the clusters
    x$cluster <- stats::kmeans(y, optim_k)$cluster %>%
      #clusters don't always come out in the order we want them to
      #so here we convert them into factors, relevel and then extract
      #the numeric value to ensure that the cluster numbers are in order
      {factor(., levels = unique(.))} %>%
      as.numeric()

    #outputting the method used
    x$cluster_method <- method

    return(x)
  }else{
    if(method == "mindate"){
      x <- x |>
        dplyr::mutate(cluster = as.numeric(sub.activity.start.date - min(sub.activity.start.date)),
                      cluster = cut(cluster, seq(0, grouping_days*6, by = grouping_days), include.lowest = T),
                      cluster = as.numeric(cluster),
                      cluster_method = method)

      return(x)
    }
  }
}

#' @description
#' manager function to run the cluster_dates() function using helper function run_cluster_dates to cluster SIAs by type
#' @export
#' @import dplyr
#' @param df dataframe of SIAs to identify rounds by vaccine type
cluster_dates_for_sias <- function(sia){

  tick <- Sys.time()

  sia.01 <- sia |>
    dplyr::filter(yr.sia >= 2010)
  #get all vax types in SIA data
  vax.types <- sia |>
    dplyr::pull(vaccine.type) |>
    unique()

  for(i in vax.types) {
    sia.01 |>
      run_cluster_dates(min_obs = 4, type = i)
  }

  tock <- Sys.time()

  print(tock - tick)
}

#' @description
#' Wrapper around the cluster_dates function to do some error checking
#'
#' @export
#' @import dplyr readr
#' @param data df dataframe on which to run cluster dates function
#' @param min_obs int
#' @param type str vaccine type
run_cluster_dates <- function(data,
                              cache_folder = "Data/sia_cluster_cache",
                              min_obs = 4,
                              type){

  Sys.setenv(POLIS_EDAV_FLAG = T)
  #check which locations meet minimum obs requirements
  in_data <- data |>
    dplyr::filter(vaccine.type == type) |>
    dplyr::group_by(adm2guid) |>
    dplyr::summarize(count = n())

  #check if cache exists
  cache_exists <- tidypolis:::tidypolis_io(io = "exists.file", file_path = paste0(cache_folder, "/", type, "_cluster_cache.rds"))

  if(cache_exists){
    cache <- tidypolis:::tidypolis_io(io = "read", file_path = paste0(cache_folder, "/", type, "_cluster_cache.rds"))
    in_data <- setdiff(in_data, cache)

    print(paste0(nrow(in_data), " potentially new SIAs in [",type,"] found for clustering analysis"))

    #drop cache rows where the adm2guid is in in_data with a different count
    cache <- cache |>
      dplyr::filter(!(adm2guid %in% in_data$adm2guid))

    tidypolis:::tidypolis_io(obj = dplyr::bind_rows(in_data, cache), io = "write", file_path = paste0(cache_folder, "/", type,"_cluster_cache.rds"))
  }else{
    print(paste0("No cache found for [", type, "], creating cache and running clustering for ", nrow(in_data), " SIAs"))
    tidypolis:::tidypolis_io(obj = in_data, io = "write", file_path = paste0(cache_folder, "/", type,"_cluster_cache.rds"))
  }

  if(nrow(in_data) > 0){
    print("Clustering new SIA data")
    in_data <- in_data |>
      dplyr::filter(count >= min_obs)

    included <- data |>
      dplyr::filter(vaccine.type == type) |>
      dplyr::filter(adm2guid %in% in_data$adm2guid)

    #observations which didn't meet the minimum requirement
    dropped <- setdiff(dplyr::filter(data, vaccine.type == type), included)

    #for data with at least a minimum number of observations
    out <- dplyr::ungroup(included) |>
      dplyr::group_by(adm2guid) |>
      dplyr::group_split() |>
      #apply function to each subset
      lapply(cluster_dates) |>
      #bind output back together
      dplyr::bind_rows()

    #error checking for situations where no data < min_obs
    if(nrow(dropped) > 0){
      #for data with low obs
      out2 <- dplyr::ungroup(dropped) |>
        dplyr::group_by(adm2guid) |>
        dplyr::group_split() |>
        lapply(function(x) cluster_dates(x, method = "mindate")) |>
        dplyr::bind_rows()
    }


    #error catching the return
    if(nrow(dropped) > 0){

      out <- bind_rows(out, out2)
    }

    #data cache
    data_cache_exists <- tidypolis:::tidypolis_io(io = "exists.file", file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))

    if(data_cache_exists){
      data_cache <- tidypolis:::tidypolis_io(io = "read", file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))

      out <- filter(data_cache, !sia.sub.activity.code %in% unique(out$sia.sub.activity.code)) |>
        dplyr::bind_rows(out)
      # data_cache2 <- data_cache %>%
      #   anti_join(out, by=c("sia.sub.activity.code", "adm2guid"))
      # out <- data_cache2 %>%
      #   bind_rows(out)

      tidypolis:::tidypolis_io(obj = out, io = "write", file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))

    }else{
      print(paste0("No data cache found for [", type, "], creating data cache and saving clustering results for ", nrow(out), " SIAs"))
      tidypolis:::tidypolis_io(obj = out, io = "write", file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))
  }




  }else{
    print(paste0("No new SIA data found for [", type, "], loading cached data!"))
    out <- tidypolis:::tidypolis_io(io = "read", file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))
  }

  return(out)
}


#' @description
#' a function to assess key variable missingness
#'
#' @import dplyr
#' @param df tibble the datatable for which we want to check key variable missingness
#' @param type str "AFP", "ES", or "POS", type of dataset to check missingness
check_missingness <- function(data,
                              type) {

  afp.vars <- c("notification.date", "investigation.date",
                "stool.1.collection.date", "stool.2.collection.date",
                "date.notification.to.hq", "results.seq.date.to.program", "specimen.date",
                "case.date", "date.onset", "stool.date.sent.to.lab", "clinical.admitted.date",
                "followup.date", "stool.1.condition", "stool.2.condition", "age.months")

  if(type == "AFP") {

    missing_by_group <- data |>
      dplyr::select(yronset, place.admin.0, all_of(afp.vars)) |>
      dplyr::group_by(yronset, place.admin.0) |>
      summarise(dplyr::across(dplyr::everything(), ~ mean(is.na(.)) * 100)) |>
      dplyr::ungroup() |>
      dplyr::filter(dplyr::if_any(afp.vars, ~ . >= 10))

    tidypolis_io(io = "write", obj = missing_by_group, file_path = paste0(Sys.getenv("POLIS_DATA_CACHE"), "/Core_Ready_Files/afp_missingess.rds"))
  }

  if(type == "ES"){

    missing_by_group <- data |>
      dplyr::select(collect.yr, admin.0, who.region, collection.date) |>
      dplyr::group_by(collect.yr, admin.0) |>
      summarise(dplyr::across(dplyr::everything(), ~ mean(is.na(.)) * 100)) |>
      dplyr::ungroup() |>
      dplyr::filter(who.region >= 10 | collection.date >= 10)

    if(nrow(missing_by_group) > 0) {
      tidypolis_io(io = "write", obj = missing_by_group, file_path = paste0(Sys.getenv("POLIS_DATA_CACHE"), "/Core_Ready_Files/es_missingess.rds"))
    }

  }
}

#### Pre-processing ####


#' Preprocess data process data in the CDC style
#'
#' @description
#' Process POLIS data into analytic datasets needed for CDC
#' @import cli sirfunctions dplyr readr lubridate stringr rio tidyr openxlsx stringi purrr pbapply lwgeom
#' @param polis_data_folder str: location of the POLIS data folder, defaults to value stored from init_tidypolis
#' @return Outputs intermediary core ready files
preprocess_cdc <- function(polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")) {

  #Step 0 - create a CORE datafiles to combine folder and check for datasets before continuing with pre-p =========
  if(!tidypolis_io(io = "exists.dir", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
    tidypolis_io(io = "create", file_path = paste0(polis_data_folder, "/core_files_to_combine"))
  }
  #if on EDAV, create files to combine folder and write datasets into it
  missing_req_files <- c()
  if(Sys.getenv("POLIS_EDAV_FLAG")){
    if(!"afp_linelist_2001-01-01_2012-12-31.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      tidypolis_io(io = "read", file_path="Data/core_files_to_combine/afp_linelist_2001-01-01_2012-12-31.rds") |>
        tidypolis_io(io = "write", file_path = paste0(polis_data_folder, "/core_files_to_combine/afp_linelist_2001-01-01_2012-12-31.rds"))
    }
    if(!"afp_linelist_2013-01-01_2016-12-31.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      tidypolis_io(io = "read", file_path="Data/core_files_to_combine/afp_linelist_2013-01-01_2016-12-31.rds") |>
        tidypolis_io(io = "write", file_path = paste0(polis_data_folder, "/core_files_to_combine/afp_linelist_2013-01-01_2016-12-31.rds"))
    }
    if(!"afp_linelist_2017-01-01_2019-12-31.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      tidypolis_io(io = "read", file_path="Data/core_files_to_combine/afp_linelist_2017-01-01_2019-12-31.rds") |>
        tidypolis_io(io = "write", file_path = paste0(polis_data_folder, "/core_files_to_combine/afp_linelist_2017-01-01_2019-12-31.rds"))
    }
    if(!"other_surveillance_type_linelist_2016_2016.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      tidypolis_io(io = "read", file_path="Data/core_files_to_combine/other_surveillance_type_linelist_2016_2016.rds") |>
        tidypolis_io(io = "write", file_path = paste0(polis_data_folder, "/core_files_to_combine/other_surveillance_type_linelist_2016_2016.rds"))
    }
    if(!"other_surveillance_type_linelist_2017_2019.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      tidypolis_io(io = "read", file_path="Data/core_files_to_combine/other_surveillance_type_linelist_2017_2019.rds") |>
        tidypolis_io(io = "write", file_path = paste0(polis_data_folder, "/core_files_to_combine/other_surveillance_type_linelist_2017_2019.rds"))
    }
    if(!"sia_2000_2019.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      tidypolis_io(io = "read", file_path="Data/core_files_to_combine/sia_2000_2019.rds") |>
        tidypolis_io(io = "write", file_path = paste0(polis_data_folder, "/core_files_to_combine/sia_2000_2019.rds"))
    }


  }else{
    if(!"afp_linelist_2001-01-01_2012-12-31.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      missing_req_files <- append(missing_req_files, "afp_linelist_2001-01-01_2012-12-31.rds")
    }
    if(!"afp_linelist_2013-01-01_2016-12-31.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      missing_req_files <- append(missing_req_files, "afp_linelist_2013-01-01_2016-12-31.rds")
    }
    if(!"afp_linelist_2017-01-01_2019-12-31.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      missing_req_files <- append(missing_req_files, "afp_linelist_2017-01-01_2019-12-31.rds")
    }
    if(!"other_surveillance_type_linelist_2016_2016.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      missing_req_files <- append(missing_req_files, "other_surveillance_type_linelist_2016_2016.rds")
    }
    if(!"other_surveillance_type_linelist_2017_2019.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      missing_req_files <- append(missing_req_files, "other_surveillance_type_linelist_2017_2019.rds")
    }
    if(!"sia_2000_2019.rds" %in% tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/core_files_to_combine"))){
      missing_req_files <- append(missing_req_files, "sia_2000_2019.rds")
    }

  }

  if (length(missing_req_files) > 0) {
    cli::cli_alert_warning("Please request the following file(s) from the SIR team or if you have EDAV access move them into your core_files_to_combine folder:")
    for (i in missing_req_files) {
      cli::cli_alert_info(paste0(i, "\n"))
    }
    stop("Halting execution of preprocessing due to missing files.")
  }


  #Step 1 - Basic cleaning and crosswalk ======
  cli::cli_h1("Step 1/5: Basic cleaning and crosswalk across datasets")

  #update log for start of creation of CORE ready datasets
  update_polis_log(.event = "Beginning Preprocessing - Creation of CORE Ready Datasets",
                   .event_type = "START")

  #Read in the updated API datasets
  cli::cli_h2("Loading data")

  cli::cli_process_start("Case")
  api_case_2019_12_01_onward <-
    tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/case.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Environmental Samples")
  api_es_complete <-
    tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/environmental_sample.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Sub-activity")
  api_subactivity_complete <-
    tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/sub_activity.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Virus")
  api_virus_complete <-
    tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/virus.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Activity")
  api_activity_complete <-
    tidypolis_io(io = "read", file_path = paste0(polis_data_folder, "/activity.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()


  #Get geodatabase to auto-fill missing GUIDs
  cli::cli_process_start("Long district spatial file")
  long.global.dist.01 <-
    sirfunctions::load_clean_dist_sp(type = "long")
  cli::cli_process_done()


  cli::cli_h2("De-duplicating data")

  cli::cli_process_start("Sub-activity")
  api_subactivity_sub1 <- api_subactivity_complete |>
    dplyr::distinct()
  cli::cli_process_done()

  cli::cli_process_start("Activity")
  api_activity_sub1 <- api_activity_complete |>
    dplyr::filter(SIASubActivityCode %in% api_subactivity_sub1$SIASubActivityCode) |>
    dplyr::distinct()
  cli::cli_process_done()

  cli::cli_process_start("Environmental Samples")
  api_es_sub1 <- api_es_complete |>
    dplyr::distinct()
  cli::cli_process_done()

  cli::cli_process_start("Case")
  api_case_sub1 <- api_case_2019_12_01_onward |>
    dplyr::distinct()
  cli::cli_process_done()

  cli::cli_process_start("Virus")
  api_virus_sub1 <- api_virus_complete |>
    dplyr::distinct()
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_subactivity_complete", "api_activity_complete", "api_es_complete",
     "api_case_2019_12_01_onward", "api_virus_complete")
  gc()
  cli::cli_process_done()

  cli::cli_process_start("Processing sub-activity spatial data")
  api_subactivity_sub2 <- api_subactivity_sub1 |>
    dplyr::mutate(year = lubridate::year(DateFrom)) |>
    dplyr::left_join(
      long.global.dist.01 |>
        dplyr::select(
          active.year.01,
          ADM0_GUID,
          ADM1_NAME,
          ADM1_GUID,
          ADM2_NAME,
          GUID
        ) |>
        dplyr::mutate(
          ADM0_GUID = tolower(stringr::str_remove_all(ADM0_GUID, "\\{")),
          ADM0_GUID = stringr::str_remove_all(ADM0_GUID, "\\}"),
          ADM1_GUID = tolower(stringr::str_remove_all(ADM1_GUID, "\\{")),
          ADM1_GUID = stringr::str_remove_all(ADM1_GUID, "\\}"),
          GUID = tolower(stringr::str_remove_all(GUID, "\\{")),
          GUID = stringr::str_remove_all(GUID, "\\}")
        ),
      by = c(
        "year" = "active.year.01",
        "Admin0Guid" = "ADM0_GUID",
        "Admin1Name" = "ADM1_NAME",
        "Admin2Name" = "ADM2_NAME"
      ),
      relationship = "many-to-many"
    ) |>
    dplyr::mutate(
      Admin2Guid = dplyr::case_when(is.na(Admin2Guid) ~ GUID,
                                    TRUE ~ Admin2Guid),
      Admin1Guid = dplyr::case_when(is.na(Admin1Guid) ~ ADM1_GUID,
                                    TRUE ~ Admin1Guid)
    ) |>
    dplyr::select(-year,-ADM1_GUID,-GUID)
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_subactivity_sub1")
  gc()
  cli::cli_process_done()

  #Import the crosswalk file and use it to rename all data elements in the API-downloaded tables.

  cli::cli_h2("Crosswalk and rename variables")

  crosswalk <- get_crosswalk_data()

  cli::cli_process_start("Case")
  api_case_sub2 <- rename_via_crosswalk(api_data = api_case_sub1,
                                        crosswalk = crosswalk,
                                        table_name = "Case")
  cli::cli_process_done()

  cli::cli_process_start("Environmental Samples")
  api_es_sub2 <- rename_via_crosswalk(api_data = api_es_sub1,
                                      crosswalk = crosswalk,
                                      table_name = "EnvSample")
  cli::cli_process_done()

  cli::cli_process_start("Virus")
  api_virus_sub2 <- rename_via_crosswalk(api_data = api_virus_sub1,
                                         crosswalk = crosswalk,
                                         table_name = "Virus")
  cli::cli_process_done()

  cli::cli_process_start("Activity")
  api_activity_sub2 <-
    rename_via_crosswalk(api_data = api_activity_sub1,
                         crosswalk = crosswalk,
                         table_name = "Activity") |>
    dplyr::select(SIASubActivityCode, crosswalk$Web_Name[crosswalk$Table == "Activity"]) |>
    dplyr::select(-c("Admin 0 Id"))
  cli::cli_process_done()

  cli::cli_process_start("Sub-activity")
  api_subactivity_sub3 <-
    rename_via_crosswalk(api_data = api_subactivity_sub2,
                         crosswalk = crosswalk,
                         table_name = "SubActivity") |>
    dplyr::left_join(api_activity_sub2,
                     by = c("SIA Sub-Activity Code" = "SIASubActivityCode"))
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_activity_sub1", "api_case_sub1", "api_es_sub1", "api_subactivity_sub2",
     "api_virus_sub1")

  cli::cli_process_done()


  cli::cli_h2("Modifying and reconciling variable types")
  #    Modify individual variables in the API files to match the coding in the web-interface downloads,
  #    and retain only variables from the API files that are present in the web-interface downloads.
  cli::cli_process_start("Reconciling activity and subactivity variables")
  api_subactivity_sub4 <- api_subactivity_sub3 |>
    dplyr::mutate(
      `IM loaded` = dplyr::case_when(
        `IM loaded` == "TRUE" ~ "Yes",
        `IM loaded` == "FALSE" ~ "No",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate(
      `LQAS loaded` = dplyr::case_when(
        `LQAS loaded` == "TRUE" ~ "Yes",
        `LQAS loaded` == "FALSE" ~ "No",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate_at(c("Age Group %", "Area Targeted %", "Country Population %"),
                     ~ as.numeric(.) * 100) |>
    dplyr::mutate(`Area Population` = as.character(format(
      round(as.numeric(`Area Population`), 1),
      nsmall = 0,
      big.mark = ","
    ))) |>
    dplyr::mutate_at(
      c("Sub-Activity Initial Planned Date", "Last Updated Date"),
      ~ lubridate::as_datetime(.)
    ) |>
    dplyr::mutate(`Activity Comments` = "") |>
    dplyr::mutate(
      `Targeted Population` = as.character(`Targeted Population`),
      `Number of Doses` = as.character(`Number of Doses`)
    ) |>
    dplyr::mutate_at(
      c(
        "UNPD Country Population",
        "Immunized Population",
        "Admin 2 Targeted Population",
        "Admin 2 Immunized Population",
        "Admin2 children inaccessible",
        "Number of Doses Approved",
        "Children inaccessible",
        "Activity parent children inaccessible",
        "Admin 0 Id",
        "Admin 1 Id",
        "Admin 2 Id"
      ),
      as.numeric
    ) |>
    dplyr::select(c(crosswalk$Web_Name[crosswalk$Table %in% c("Activity", "SubActivity") &
                                         !is.na(crosswalk$Web_Name)],
                    crosswalk$API_Name[crosswalk$Table %in% c("SubActivity") &
                                         is.na(crosswalk$Web_Name) &
                                         crosswalk$API_Name != "ActivityAdminCoveragePercentage"]))
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_subactivity_sub3")
  gc()
  cli::cli_process_done()

  cli::cli_process_start("Cleaning IPV/OPV variables")
  api_case_sub3 <- api_case_sub2 |>
    dplyr::mutate(
      `total.number.of.ipv./.opv.doses` = NA_integer_,
      Doses =  as.numeric(Doses),
      `Virus Sequenced` = as.logical(`Virus Sequenced`),
      `Advanced Notification` = dplyr::case_when(
        `Advanced Notification` == TRUE ~ "Yes",
        `Advanced Notification` == FALSE ~ "No",
        TRUE ~ NA_character_
      ),
      `Dataset Lab` = dplyr::case_when(
        `Dataset Lab` == TRUE ~ "Yes",
        `Dataset Lab` == FALSE ~ "No",
        TRUE ~ NA_character_
      ),
      `Is Breakthrough` = dplyr::case_when(
        `Is Breakthrough` == TRUE ~ "Yes",
        `Is Breakthrough` == FALSE ~ "No",
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::select(c(crosswalk$Web_Name[crosswalk$Table %in% c("Case") &
                                         !is.na(crosswalk$Web_Name)],
                    crosswalk$API_Name[crosswalk$Table %in% c("Case") &
                                         is.na(crosswalk$Web_Name)]))
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_case_sub2")
  gc()
  cli::cli_process_done()

  cli::cli_process_start("Cleaning site and location variables")
  api_es_sub3 <- api_es_sub2 |>
    dplyr::mutate(Site = paste0(`Site Code`, " - ", `Site Name`),
                  `Country Iso2` = NA_character_) |>
    dplyr::mutate_at(
      c(
        "Vaccine 1",
        "Vaccine 2",
        "Vaccine 3",
        "Vdpv 1",
        "Vdpv 2",
        "Vdpv 3",
        "Wild 1",
        "Wild 2",
        "Wild 3",
        "nVaccine 2",
        "nVDPV 2",
        "PV 1",
        "PV 2",
        "PV 3",
        "Is Suspected",
        "NPEV"
      ),
      ~ dplyr::case_when(. == "TRUE" ~ "Yes",
                  . == "FALSE" ~ "No",
                  TRUE ~ NA_character_)
    ) |>

    dplyr::mutate_at(
      c(
        "Is Breakthrough",
        "Under Process",
        "Advanced Notification",
        "Mixture"
      ),
      ~ dplyr::case_when(. == "TRUE" ~ "Yes",
                  . == "FALSE" ~ "No",
                  TRUE ~ NA_character_)
    ) |>
    dplyr::mutate_at(
      c(
        "Date Final Culture Result",
        "Date Final Combined Result",
        "Date Final Results Reported",
        "Date Isol Sent Seq2",
        "Date Isol Rec Seq2",
        "Date Final Seq Result",
        "Date Res Sent Out VDPV2",
        "Date Res Sent Out VACCINE2",
        "Date Isol Rec Seq1",
        "Collection Date",
        "Date F1 ref ITD",
        "Date F2 ref ITD",
        "Date F3 ref ITD",
        "Date F4 ref ITD",
        "Date F5 ref ITD",
        "Date F6 ref ITD",
        "Date Notification To HQ",
        "Date received in lab",
        "Date Shipped To Ref Lab",
        "Publish Date",
        "Uploaded Date"
      ),
      ~ as.character(format(as.Date(., format = "%Y-%m-%d"), format = "%d/%m/%Y"))
    ) |>
    dplyr::mutate_at(c("Created Date",
                "Updated Date"), ~ as.character(format(as.Date(., format =
                                                                 "%Y-%m-%d"), format = "%d-%m-%Y"))) |>
    dplyr::mutate(`Sample Id` = as.character(`Sample Id`)) |>
    dplyr::select(c(crosswalk$Web_Name[crosswalk$Table %in% c("EnvSample") &
                                  !is.na(crosswalk$Web_Name)],
             crosswalk$API_Name[crosswalk$Table %in% c("EnvSample") &
                                  is.na(crosswalk$Web_Name)]))
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_es_sub2")
  gc()
  cli::cli_process_done()


  cli::cli_process_start("Cleaning irregular location names")
  api_virus_sub3 <- api_virus_sub2 |>
    dplyr::mutate(location = paste(
      toupper(Admin0OfficialName),
      toupper(Admin1OfficialName),
      toupper(Admin2OfficialName),
      sep = ", "
    )) |>
    dplyr::mutate(location = stringr::str_squish(stringr::str_replace_all(location, ", NA,", ", "))) |>
    dplyr::mutate(location = dplyr::case_when(
      stringr::str_sub(location,-4) == ", NA" ~ substr(location, 1, nchar(location) - 4),
      TRUE ~ location
    )) |>
    dplyr::mutate(location = stringr::str_replace_all(
      location,
      "ISLAMIC REPUBLIC OF IRAN",
      "IRAN (ISLAMIC REPUBLIC OF)"
    )) |>
    dplyr::mutate(
      location = stringr::str_replace_all(
        location,
        "UNITED KINGDOM OF GREAT BRITAIN AND NORTHERN IRELAND",
        "THE UNITED KINGDOM"
      )
    ) |>

    dplyr::mutate(`Virus Type(s)` = stringr::str_replace_all(`Virus Type(s)`, "NVACCINE", "nVACCINE")) |>

    dplyr::mutate(
      `Nt Changes` = dplyr::case_when(
        grepl("VDPV", `VirusTypeName`) &
          !is.na(`Nt Changes`) ~ `Nt Changes`,
        grepl("VACCINE", VirusTypeName) &
          !is.na(VaccineNtChangesFromSabin) ~ VaccineNtChangesFromSabin,
        TRUE ~ NA_character_
      )
    ) |>
    dplyr::mutate_at(c("Virus Date"), ~ lubridate::as_date(.)) |>
    dplyr::mutate_at(
      c("Is Breakthrough"),
      ~ dplyr::case_when(. == "TRUE" ~ "Yes",
                  . == "FALSE" ~ "No",
                  TRUE ~ NA_character_)
    ) |>
    dplyr::select(c(crosswalk$Web_Name[crosswalk$Table %in% c("Virus") &
                                  !is.na(crosswalk$Web_Name)],
             crosswalk$API_Name[crosswalk$Table %in% c("Virus") &
                                  is.na(crosswalk$Web_Name)])) |>
    dplyr::mutate(nt.changes.neighbor = dplyr::case_when(
      !is.na(VdpvNtChangesClosestMatch) ~ VdpvNtChangesClosestMatch,
      TRUE ~ NA_character_
    ))
  cli::cli_process_done()

  cli::cli_process_start("Clearing memory")
  rm("api_virus_sub2")
  gc()
  cli::cli_process_done()


  cli::cli_process_start("Removing empty columns")

  cli::cli_h3("Case")
  api_case_sub3 <- remove_empty_columns(api_case_sub3)

  cli::cli_h3("Sub-activity")
  api_subactivity_sub4 <- remove_empty_columns(api_subactivity_sub4)

  cli::cli_h3("ES")
  api_es_sub3 <- remove_empty_columns(api_es_sub3)

  #if nVaccine 2 is removed recreate with empty values so downstream code doesn't break

  es_names <- api_es_sub3 |>
    names()

  if(!"nVaccine 2" %in% es_names){
    api_es_sub3 <- api_es_sub3 |>
      cbind("nVaccine 2" = NA)
  }

  cli::cli_h3("Virus")
  api_virus_sub3 <- remove_empty_columns(api_virus_sub3)

  cli::cli_process_done()

  #13. Export csv files that match the web download, and create archive and change log
  cli::cli_h2("Creating change log and exporting data")

  cli::cli_process_start("Checking on requisite file structure")
  ts <- Sys.time()
  timestamp <- paste0(lubridate::date(ts),"_",lubridate::hour(ts),"-",lubridate::minute(ts),"-",round(lubridate::second(ts), 0))

  #create directory

  #files in directory
  c(
    file.path(polis_data_folder, "Core_Ready_Files"),
    file.path(polis_data_folder, "Core_Ready_Files", "Archive"),
    file.path(polis_data_folder, "Core_Ready_Files", "Archive", timestamp),
    file.path(polis_data_folder, "Core_Ready_Files", "Change Log"),
    file.path(polis_data_folder, "Core_Ready_Files", "Change Log", timestamp)
  ) |>
    sapply(function(x){
      if(!tidypolis_io(io = "exists.dir", file_path = x)){
        tidypolis_io(io = "create", file_path = x)
      }
    })

  cli::cli_process_done()
  #Get list of most recent files
  files_in_core_ready <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"))
  most_recent_files <- files_in_core_ready[grepl(".rds", files_in_core_ready)]
  most_recent_file_patterns <- c("Activity_Data", "EnvSamples", "Human_Detailed", "Viruses_Detailed")
  most_recent_files <- most_recent_files[grepl(paste(most_recent_file_patterns, collapse = "|"), most_recent_files)]

  if(length(most_recent_files)>0){

    for(i in 1:length(most_recent_files)){
      cli::cli_process_start(paste0("Processing data for: ", most_recent_files[i]))
      #compare current dataset to most recent and save summary to change_log
      old <- tidypolis_io(io = "read", file_path = file.path(polis_data_folder, "Core_Ready_Files", most_recent_files[i])) |>
        dplyr::mutate_all(as.character)

      if(grepl("EnvSamples", most_recent_files[i])){

        new <- api_es_sub3 |>
          dplyr::mutate(Id = `Env Sample Manual Edit Id`) |>
          dplyr::mutate_all(as.character)

        old <- old |>
          dplyr::mutate(Id = `Env Sample Manual Edit Id`)
      }

      if(grepl("Viruses", most_recent_files[i])){

        new <- api_virus_sub3 |>
          dplyr::mutate(Id = `Virus ID`) |>
          dplyr::mutate_all(as.character)

        old <- old |>
          dplyr::mutate(Id = `Virus ID`)

      }

      if(grepl("Human_Detailed", most_recent_files[i])){

        new <- api_case_sub3 |>
          dplyr::mutate(Id = `EPID`) |>
          dplyr::mutate_all(as.character)

        old <- old |>
          dplyr::mutate(Id = `EPID`)

      }

      if(grepl("Activity", most_recent_files[i])){

        new <- api_subactivity_sub4 |>
          dplyr::mutate(Id = paste0(`SIA Sub-Activity Code`, "_",`Admin 2 Guid`)) |>
          dplyr::mutate_all(as.character)

        old <- old |>
          dplyr::mutate(Id = paste0(`SIA Sub-Activity Code`, "_",`Admin 2 Guid`))

      }

      potential_duplicates_new <- new |>
        dplyr::group_by(Id) |>
        dplyr::summarise(count = n()) |>
        dplyr::ungroup() |>
        dplyr::filter(count >= 2)

      potential_duplicates_old <- old |>
        dplyr::group_by(Id) |>
        dplyr::summarise(count = n()) |>
        dplyr::ungroup() |>
        dplyr::filter(count >= 2)

      new <- new |>
        dplyr::filter(!(Id %in% potential_duplicates_new$Id) & !(Id %in% potential_duplicates_old$Id))

      old <- old |>
        dplyr::filter(!(Id %in% potential_duplicates_new$Id) & !(Id %in% potential_duplicates_old$Id))

      in_new_not_old <- new |>
        filter(!(Id %in% old$Id))

      in_old_not_new <- old |>
        filter(!(Id %in% new$Id))

      in_new_and_old_but_modified <- new |>
        dplyr::filter(Id %in% old$Id) |>
        dplyr::select(-c(setdiff(colnames(new), colnames(old))))

      in_new_and_old_but_modified <- setdiff(in_new_and_old_but_modified, old |>
                  dplyr::select(-c(setdiff(colnames(old), colnames(new)))))

      x <- old |>
        dplyr::filter(Id %in% new$Id) |>
        dplyr::select(-c(setdiff(colnames(old), colnames(new))))


      if(nrow(in_new_and_old_but_modified) >= 1){

      in_new_and_old_but_modified <- dplyr::inner_join(in_new_and_old_but_modified, setdiff(x, new |>
                               dplyr::select(-c(setdiff(colnames(new), colnames(old))))), by="Id") |>
        #wide_to_long
        tidyr::pivot_longer(cols=-Id) |>
        dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
        dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
        #long_to_wide
        tidyr::pivot_wider(names_from=source, values_from=value) |>
        dplyr::mutate(new = as.character(new), old = as.character(old))

        in_new_and_old_but_modified <- in_new_and_old_but_modified |>
          dplyr::filter(new != old)
      }

      cli::cli_process_done()
      cli::cli_process_start(paste0("Creating change log for: ", most_recent_files[i]))
      n_added = nrow(in_new_not_old)
      n_edited = length(unique(in_new_and_old_but_modified$Id))
      n_deleted = nrow(in_old_not_new)
      vars_added = setdiff(colnames(new), colnames(old))
      vars_dropped = setdiff(colnames(old), colnames(new))
      change_summary <- list(n_added = n_added,
                             n_edited = n_edited,
                             n_deleted = n_deleted,
                             vars_added = vars_added,
                             vars_dropped = vars_dropped,
                             obs_added = in_new_not_old,
                             obs_edited = in_new_and_old_but_modified,
                             obs_deleted = in_old_not_new)
      tidypolis_io(io = "write",
                   obj = change_summary,
                   file_path = file.path(polis_data_folder, "Core_Ready_Files", "Change Log", timestamp, paste0(substr(most_recent_files[i],1,nchar(most_recent_files[i])-4), ".rds")))
      #Move most recent to archive
      tidypolis_io(io = "read", file_path = (file.path(polis_data_folder, "Core_Ready_Files", most_recent_files[i]))) |>
        tidypolis_io(io = "write", file_path = file.path(polis_data_folder, "Core_Ready_Files", "Archive", timestamp, most_recent_files[i]))

      tidypolis_io(io = "delete", file_path = (file.path(polis_data_folder, "Core_Ready_Files", most_recent_files[i])))

      cli::cli_process_done()
    }
  }else{
    cli::cli_alert_info("No previous main Core Ready Files found, creating new files")
  }

  most_recent_files_01 <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"))
  most_recent_file_01_patterns <- c(".rds", ".csv", ".xlsx")
  most_recent_files_01 <- most_recent_files_01[grepl(paste(most_recent_file_01_patterns, collapse = "|"), most_recent_files_01)]

  if(length(most_recent_files_01) > 0){
    for(i in 1:length(most_recent_files_01)){
      cli::cli_process_start(paste0("Archiving Data for: ", most_recent_files_01[i]))
      #move file to archive
      tidypolis_io(io = "read", file_path = file.path(polis_data_folder, "Core_Ready_Files", most_recent_files_01[i])) |>
        tidypolis_io(io = "write", file_path = file.path(polis_data_folder, "Core_Ready_Files", "Archive", timestamp, most_recent_files_01[i]))
      #delete file
      tidypolis_io(io = "delete", file_path = file.path(polis_data_folder, "Core_Ready_Files", most_recent_files_01[i]))
      cli::cli_process_done()
    }
  }else{
    cli::cli_alert_info("No previous secondary Core Ready Files found, creating new files")
  }

  cli::cli_process_start("Writing all final Core Ready files")
  #Export files (as csv) to be used as pre-processing starting points
  tidypolis_io(obj = api_case_sub3, io = "write", file_path = file.path(polis_data_folder, "Core_Ready_Files", paste0("Human_Detailed_Dataset_",timestamp,"_from_01_Dec_2019_to_",format(ts, "%d_%b_%Y"),".rds")))
  tidypolis_io(obj = api_subactivity_sub4, io = "write", file_path = file.path(polis_data_folder, "Core_Ready_Files", paste0("Activity_Data_with_All_Sub-Activities_(1_district_per_row)_",timestamp,"_from_01_Jan_2020_to_",format(Sys.Date()+365/2, "%d_%b_%Y"),".rds")))
  tidypolis_io(obj = api_es_sub3, io = "write", file_path = file.path(polis_data_folder, "Core_Ready_Files", paste0("EnvSamples_Detailed_Dataset_",timestamp,"_from_01_Jan_2000_to_",format(ts, "%d_%b_%Y"),".rds")))
  tidypolis_io(obj = api_virus_sub3, io = "write", file.path(polis_data_folder, "Core_Ready_Files", paste0("Viruses_Detailed_Dataset_",timestamp,"_from_01_Dec_1999_to_",format(ts, "%d_%b_%Y"),".rds")))
  cli::cli_process_done()

  update_polis_log(.event = "CORE Ready files and change logs complete",
                   .event_type = "PROCESS")

  #14. Remove temporary files from working environment, and set scientific notation back to whatever it was originally
  cli::cli_process_start("Clearing memory from first step")
  rm("change_summary", "crosswalk", "in_new_and_old_but_modified", "in_new_not_old",
     "in_old_not_new", "new", "old", "potential_duplicates_new", "potential_duplicates_old",
     "x", "i", "n_added", "n_deleted", "n_edited", "vars_added", "vars_dropped",
     "api_activity_sub2", "api_case_sub3", "api_es_sub3", "api_virus_sub3",
     "api_subactivity_sub4", "most_recent_file_01_patterns", "most_recent_file_patterns")
  gc()
  cli::cli_process_done()

  #Step 2 - Creating AFP and EPI datasets ====
  update_polis_log(.event = "Creating AFP and Epi analytic datasets",
                   .event_type = "PROCESS")

  cli::cli_h1("Step 2/5 - Creating AFP and Epi analytic datasets")

  # Step 1: Read in "old" data file (System to find "Old" data file)
  latest_folder_in_archive <- tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/Core_Ready_Files/Archive")) |>
    dplyr::tibble()

  names(latest_folder_in_archive) <- c("name")

  latest_folder_in_archive <- latest_folder_in_archive |>
    dplyr::mutate(date_time = as_datetime(name))

  if(nrow(latest_folder_in_archive) > 0){

    cli::cli_alert_info("Previous archive found!")

    latest_folder_in_archive <- latest_folder_in_archive |>
      dplyr::filter(date_time == max(date_time)) |>
      dplyr::pull(name)
  }else{

    cli::cli_alert_info("No previous archive identified, will not perform any comparisons")

    latest_folder_in_archive <- timestamp

  }

  x <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files/Archive", latest_folder_in_archive), full_names = T)

  y <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"), full_names = T)

  cli::cli_process_start("Loading new human dataset")
  afp.raw.new <- tidypolis_io(io = "read", file_path = y[grepl("Human", y)]) |>
    dplyr::mutate_all(as.character) |>
    dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
    dplyr::mutate_all(list(~na_if(.,"")))
  cli::cli_process_done()

  names(afp.raw.new) <- stringr::str_to_lower(names(afp.raw.new))


  cli::cli_process_start("Managing memory load")
  gc()
  cli::cli_process_done()

  if(length(x)>0){

    cli::cli_alert_info("Old AFP dataset identified")

    cli::cli_process_start("Loading old human dataset")
    afp.raw.old <- tidypolis_io(io = "read", file_path = x[grepl("Human", x)]) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~na_if(.,"")))
    cli::cli_process_done()

    names(afp.raw.old) <- stringr::str_to_lower(names(afp.raw.old))

    # variables in old dataframe
    var.names <- afp.raw.old |>
      purrr::map_df(~ (data.frame(class = class(.x))),
                    .id = "variable"
      )

    var.names.01 <- var.names |>
      filter(variable != "virus.type(s)" &
               variable != "specdatereceivedbynatlab" &
               variable != "vdpv.classification(s)" &
               variable != "classification" &
               variable != "surveillance.type" &
               variable != "stool.1.condition" &
               variable != "stool.2.condition" &  # list of variables we want evaluated in 2nd QC function
               !(variable %in% c("afp.reporting.week", "last.updated.by", "total.number.of.ipv./.opv.doses", "npev", "nvaccine.1", "nvaccine.3", "nvdpv.1", "nvdpv.3", "exact.longitude", "exact.latitude", "place.nearest.facility", "source.ri.vaccination.information", "virus.genotypes",
                                 "event.name", "virus.origin", "event.direct.importation", "virus.sequenced", "virus.source.linked.to",
                                 "event.immediate.source", "virus.homology.percent", "virus.is.orphan", "event.comments", "wild.2",
                                 "nvdpv.2", "pv.1", "pv.2", "pv.3", 'pons_patient', 'pons_seqregion', 'pons_reference',
                                 'pons_referencename', 'pons_genotype2', 'pons_serotype2', 'pons_wildclustername')))

    var.list.01 <- as.character(var.names.01$variable)

    afp.raw.old.comp <- afp.raw.old |>
      dplyr::select(-dplyr::all_of(var.list.01))

    afp.raw.new.comp <- afp.raw.new |>
      dplyr::select(-dplyr::all_of(var.list.01))

    f.compare.dataframe.cols(afp.raw.old.comp, afp.raw.new.comp)

    new.var.afp.01 <- f.download.compare.01(afp.raw.old.comp, afp.raw.new.comp)

    new.df <- new.var.afp.01 |>
      dplyr::filter(is.na(old.distinct.01) | diff.distinct.01 >= 1)

    # Step 4: Apply compare variables function
    if (nrow(new.df) >= 1) {
      cli::cli_alert_danger("There is either a new variable in the AFP data or new value of an existing variable.
       Please run f.download.compare.02 to see what it is. New values of variables are present in log file.")

      afp.new.value <- f.download.compare.02(new.var.afp.01, afp.raw.old.comp, afp.raw.new.comp)

      update_polis_log(.event = sapply(names(afp.new.value), function(x) paste0("New Values in: ", x, " - ", paste0(unique(dplyr::pull(afp.new.value, x)), collapse = ", "))) |>
                         paste0(collapse = "; "),
                       .event_type = "ALERT")

    } else {
      cli::cli_alert_info("New AFP download is comparable to old AFP download")
    }

  }else{
    cli::cli_alert_info("No old AFP dataset identified")
  }

  cli::cli_process_start("Managing memory load")
  gc()
  cli::cli_process_done()

  #Find out if there are duplicate epids.
  cli::cli_process_start("Checking for duplicated EPIDs")
  afp.raw.dup <- afp.raw.new |>
    dplyr::mutate(dateonset = as.Date(date.onset, format="%Y-%m-%d"),
           yronset = lubridate::year(dateonset)) |>
    dplyr::select(epid, date.onset, yronset, place.admin.0, yronset, `virus.type(s)`, classification,
           stool.1.collection.date, stool.2.collection.date, stool.1.condition, stool.2.condition) |>
    dplyr::group_by(epid, date.onset, place.admin.0) |>
    dplyr::mutate(afp_dup =n()) |>
    dplyr::filter(afp_dup>=2) |>
    dplyr::select(epid, date.onset, place.admin.0, yronset, `virus.type(s)`, classification,
           stool.1.collection.date, stool.2.collection.date, stool.1.condition, stool.2.condition)

  # Script below will stop further execution if there is a duplicate epid
  if (nrow(afp.raw.dup) >= 1) {
    # Export duplicate afp cases in the CSV file:
    tidypolis_io(io = "write", obj = afp.raw.dup, paste0(polis_data_folder,"/Core_Ready_Files/", paste("duplicate_AFPcases_Polis",
                                                          min(afp.raw.dup$yronset, na.rm = T),
                                                          max(afp.raw.dup$yronset, na.rm = T),
                                                          sep = "_"), ".csv", sep = "")
    )
    cli::cli_alert_info("Duplicate AFP case. Check the data for duplicate records. If they are exact same, then contact Ashley")

    update_polis_log(.event = "Duplicate AFP cases output in duplicate_AFPcases_Polis within Core_Ready_files",
                     .event_type = "ALERT")

  } else {
    cli::cli_process_done(msg_done = "No duplicate EPIDs found, okay to proceed")
  }

  cli::cli_process_start("Fixing all dates from character to ymd format and fix character variables ")

  afp.raw.01 <- afp.raw.new |>
    dplyr::rename(poliovirustypes = `virus.type(s)`,
           classificationvdpv = `vdpv.classification(s)`,
           surveillancetypename = surveillance.type,
           whoregion = who.region,
           admin0guid = admin.0.guid,
           admin1guid = admin.1.guid,
           admin2guid = admin.2.guid) |>

    dplyr::mutate(
      dateonset = lubridate::ymd(as.Date(date.onset, "%Y-%m-%dT%H:%M:%S")),
      yronset = lubridate::year(dateonset)
    )  |>
    dplyr::mutate(
      datenotify = lubridate::ymd(as.Date(notification.date, "%Y-%m-%dT%H:%M:%S")),
      dateinvest = lubridate::ymd(as.Date(investigation.date, "%Y-%m-%dT%H:%M:%S")),
      datestool1 = lubridate::ymd(as.Date(stool.1.collection.date, "%Y-%m-%dT%H:%M:%S")),
      datestool2 = lubridate::ymd(as.Date(stool.2.collection.date, "%Y-%m-%dT%H:%M:%S")),
      datenotificationtohq = lubridate::ymd(as.Date(date.notification.to.hq, "%Y-%m-%dT%H:%M:%S")),
      results.seq.date.to.program = lubridate::ymd(as.Date(results.seq.date.to.program, "%Y-%m-%dT%H:%M:%S")),
      specimen.date = lubridate::ymd(as.Date(specimen.date, "%Y-%m-%dT%H:%M:%S")),
      casedate = lubridate::ymd(as.Date(case.date, "%Y-%m-%dT%H:%M:%S")),
      ontostool2 = as.numeric(datestool2 - dateonset),
      ontostool1 = as.numeric(datestool1 - dateonset),
      age.months = as.numeric(`calculated.age.(months)`),
      ontonot = as.numeric(datenotify - dateonset),
      ontoinvest = as.numeric(dateinvest - dateonset),
      nottoinvest = as.numeric(dateinvest - datenotify),
      investtostool1 = as.numeric(datestool1 - dateinvest),
      stool1tostool2 = as.numeric(datestool2 - datestool1),
      stooltolabdate = lubridate::ymd(as.Date(stool.date.sent.to.lab, "%Y-%m-%dT%H:%M:%S")),
      stooltoiclabdate = lubridate::ymd(as.Date(stool.date.sent.to.ic.lab, "%Y-%m-%dT%H:%M:%S")),
      clinicadmitdate = lubridate::ymd(as.Date(clinical.admitted.date, "%Y-%m-%dT%H:%M:%S")),
      followup.date = lubridate::ymd(as.Date(followup.date, "%Y-%m-%dT%H:%M:%S")),
      poliovirustypes = stringr::str_replace_all(poliovirustypes, "  ", " "))

  cli::cli_process_done()

  #write out epids with no dateonset
  tidypolis_io(io = "write", obj = afp.raw.01 |>
                 dplyr::select(epid, dateonset) |>
                 dplyr::filter(is.na(dateonset)),
               file_path = paste0(Sys.getenv("POLIS_DATA_CACHE"), "/Core_Ready_Files/afp_no_onset.csv"))

  cli::cli_process_start("Checking for missingness in key variables")
  check_missingness(data = afp.raw.01, type = "AFP")
  cli::cli_process_done("Check afp_missingess.rds for missing key varialbes")

  cli::cli_process_start("Classification of cases using lab data")
  # this uses the laboratory data "poliovirustypes" to assign virus type (WPV1 WPV3 and vdpv1,2,2)
  # vtype assigns the polioviruses found in stool excluding sabin

  endyr <- year(format(Sys.time()))
  startyr <- 2020

  afp.raw.01 <- afp.raw.01 |>
  dplyr::mutate(vtype = ifelse(stringr::str_detect(poliovirustypes, "WILD1"), "WILD 1", "none"),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "WILD2"), "WILD 2", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "WILD3"), "WILD 3", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "WILD1, WILD3"), "WILD1andWILD3", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV1"), "VDPV 1", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV2"), "VDPV 2", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV3"), "VDPV 3", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV1") &
                       stringr::str_detect(poliovirustypes, "VDPV2"), "VDPV1andVDPV2", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV 1") &
                       stringr::str_detect(poliovirustypes, "VDPV3"), "VDPV1andVDPV3", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV2") &
                       stringr::str_detect(poliovirustypes, "VDPV3"), "VDPV2andVDPV3", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "VDPV1") &
                       stringr::str_detect(poliovirustypes, "VDPV2")
                     & stringr::str_detect(poliovirustypes, "VDPV3"), "VDPV12and3", vtype),
      vtype = ifelse(stringr::str_detect(poliovirustypes, "WILD") & stringr::str_detect(poliovirustypes, "VDPV"),
                     paste0("CombinationWild1-", vtype), vtype
      ),

      # now using classificationvdpv to add in c, i, and a
      # this would make combination isolation of multiple vdpv types as c/a/i for the first
      # listed virus type. this is not accurate as we dont know which type is c/a/i

      classificationvdpv = ifelse(is.na(classificationvdpv), "", classificationvdpv),
      vtype = ifelse(classificationvdpv == "Ambiguous" & !is.na(vtype), paste0("a", vtype), vtype),
      vtype = ifelse(stringr::str_detect(classificationvdpv, "Circulating") & !is.na(vtype), paste0("c", vtype), vtype),
      vtype = ifelse(classificationvdpv == "Immune Deficient" & !is.na(vtype), paste0("i", vtype), vtype),

      # cleaning up cnone =none, and inone= none and may need additional fixes if more weird things happen

      vtype = ifelse(vtype == "cnone", "none", vtype),
      vtype = ifelse(vtype == "inone", "none", vtype),
      vtype = ifelse(vtype == "anone", "none", vtype),
      vtype = ifelse(vtype == "cCombinationWild1-VDPV 2", "CombinationWild1-cVDPV 2", vtype),
      vtype = ifelse(vtype == "cCombinationWild1-VDPV 3", "CombinationWild1-cVDPV 3", vtype),
      vtype = ifelse(vtype == "cCombinationWild1-VDPV 1", "CombinationWild1-cVDPV 1", vtype),
      vtype = ifelse(vtype == "cVDPV2andVDPV3", "cVDPV2andcVDPV3", vtype),

      # creating vtype.fixed that hard codes a fix for Congo 2010 WPV1 cases that were not tested by lab

      vtype.fixed = ifelse((classification == "Confirmed (wild)" & place.admin.0 == "CONGO" & yronset == "2010"),
                           "WILD 1", vtype
      ),

      # hard coding a fix for a Nigeria case that is WPV1 but missing from the lab data
      vtype.fixed = ifelse((is.na(vtype) == T & place.admin.0 == "NIGERIA" & yronset == "2011" & classification == "Confirmed (wild)"),
                           "WILD 1", vtype.fixed
      ),

      # NEW hard coding to deal with WPV1 cases from before 2010 that have no lab data assuming these are WPV 1

      vtype.fixed = ifelse((is.na(vtype) == T & yronset < "2010" & classification == "Confirmed (wild)"), "WILD 1", vtype.fixed),

      # note POLIS data undercounts the total WPV1 cases in 2011, in 2012 we have one extra WPV3 and one less WPV1

      #hard fix for Yemen case (YEM-SAD-2021-457-33) where classified as vdpv1andvdpv2 instead of cvdpv2
      vtype.fixed = ifelse(vtype == "cVDPV1andVDPV2" & stringr::str_detect(poliovirustypes, "cVDPV2"), "VDPV1andcVDPV2", vtype.fixed),

      #further fixing classification for cases with multiple vdpvs
      vtype.fixed = ifelse(vtype == "cVDPV1andVDPV2" & (stringr::str_detect(poliovirustypes, "cVDPV2") & stringr::str_detect(poliovirustypes, "cVDPV1")), "cVDPV1andcVDPV2", vtype.fixed),

      # now creating cdc.classification.all categories includes non-AFP, NPAFP, all vtypes and polio compatibles, pending, etc
      # CDC.classification is based first on lab, then on epi data. If the epi data and lab data disagree, we default to lab classification

      cdc.classification.all = vtype.fixed,
      cdc.classification.all = ifelse((vtype.fixed == "none" | is.na(vtype.fixed)) & classification == "Compatible",
                                      "COMPATIBLE", cdc.classification.all
      ),
      cdc.classification.all = ifelse((vtype.fixed == "none" | is.na(vtype.fixed)) & classification == "Discarded",
                                      "NPAFP", cdc.classification.all
      ),
      cdc.classification.all = ifelse((vtype.fixed == "none" | is.na(vtype.fixed)) & classification == "Not an AFP",
                                      "NOT-AFP", cdc.classification.all
      ),
      cdc.classification.all = ifelse((vtype.fixed == "none" | is.na(vtype.fixed)) & classification == "Pending",
                                      "PENDING", cdc.classification.all
      ),
      cdc.classification.all = ifelse((vtype.fixed == "none" | is.na(vtype.fixed)) & classification == "VAPP", "VAPP",
                                      cdc.classification.all
      ),
      cdc.classification.all = ifelse((vtype.fixed == "none" | is.na(vtype.fixed)) &
                                        (classification == "Not Applicable" | classification == "Others" | classification == "VDPV"),
                                      "UNKNOWN", cdc.classification.all
      ),

      cdc.classification.all = ifelse(
        final.cell.culture.result == "Not received in lab" &
          cdc.classification.all == "PENDING",
        "LAB PENDING",
        cdc.classification.all
      ),
      hot.case = ifelse(
        paralysis.asymmetric == "Yes" &
          paralysis.onset.fever == "Yes" &
          paralysis.rapid.progress == "Yes",
        1,
        0
      ),
      hot.case = ifelse(is.na(hot.case), 99, hot.case),

      # now dealing with Sabin viruses- creating indicators for detection of Sabin1, Sabin2 and Sabin3
      # note an AFP case can by NPAFP AND have Sabin virus isolated
      sabin1 = ifelse(stringr::str_detect(poliovirustypes, "VACCINE1"), 1, 0),
      sabin2 = ifelse(stringr::str_detect(poliovirustypes, "VACCINE2"), 1, 0),
      sabin3 = ifelse(stringr::str_detect(poliovirustypes, "VACCINE3"), 1, 0)
    ) |>
    dplyr::mutate(place.admin.0 = ifelse(stringr::str_detect(place.admin.0, "IVOIRE"),"COTE D IVOIRE",place.admin.0)) |>
    dplyr::distinct() |>
    dplyr::mutate(yronset=ifelse(is.na(yronset)==T, lubridate::year(datestool1), yronset),
           yronset = ifelse(is.na(datestool1) == T & is.na(yronset)==T, lubridate::year(datenotify), yronset)) |> #adding year onset correction for nonAFP
    dplyr::filter(dplyr::between(yronset, startyr, endyr))

  cli::cli_process_done()

  cli::cli_process_start("Verified that classifications in data line up with poliovirustypes and classification")
  #manual check for incorrect cdc.classifications
  #manually check to make sure the poliovirustypes and cdc.classification.all match up
  vtype.tocheck <- c("VDPV1andVDPV2", "VDPV1andVDPV3", "VDPV2andVDPV3", "VDPV12and3", "cVDPV1andVDPV2")
  virus.tocheck <- c("cVDPV1, VACCINE1, VDPV1")
  classification.tocheck <- c("Circulating, Pending")
  cdc.classification.tocheck <- c("none")
  tocheck <- afp.raw.01 |>
    dplyr::filter(vtype %in% vtype.tocheck |
             cdc.classification.all %in% cdc.classification.tocheck |
             poliovirustypes %in% virus.tocheck |
             classificationvdpv %in% classification.tocheck |
             is.na(cdc.classification.all)) |>
    dplyr::select(epid, poliovirustypes, classification, classificationvdpv, nt.changes, vtype, vtype.fixed, cdc.classification.all)

  if(nrow(tocheck |> dplyr::filter(cdc.classification.all == "none")) > 0){
    epids <- tocheck |> dplyr::filter(cdc.classification.all == "none") |> pull(epid)

    flagged_to_polis <- c("MOZ-TET-TSA-22-006")

    epids <- epids[!epids %in% flagged_to_polis]

    if(length(epids) > 0){
      cli::cli_alert_danger("There is a 'none' classification, flag for POLIS and get guidance on proper classification")

      update_polis_log(.event = "NONE classification found, must be manually addressed",
                       .event_type = "ERROR")

      stop()
    }
  }

  cli::cli_process_done()

  afp.raw.01 <- afp.raw.01 |>
    filter(cdc.classification.all != "none")

  cli::cli_process_start("Clearing memory")
  rm("vtype.tocheck", "virus.tocheck", "classification.tocheck", "cdc.classification.tocheck", "tocheck",
     "afp.raw.dup", "afp.raw.new", "afp.raw.new.comp", "afp.raw.old", "afp.raw.old.comp", "new.df",
     "new.var.afp.01", "var.names", "var.names.01", "var.list.01", "x", "y", "epids", "flagged_to_polis")
  gc()
  cli::cli_process_done()

  cli::cli_process_start("Checking GUIDs")


  afp.linelist.01 <- afp.raw.01 |>
    dplyr::mutate(
      Admin2GUID = paste0("{", toupper(admin2guid), "}"),
      Admin1GUID = paste0("{", toupper(admin1guid), "}"),
      Admin0GUID = paste0("{", toupper(admin0guid), "}")
    )


  shapes <- long.global.dist.01 |>
    tibble::as_tibble() |>
    dplyr::select(ADM0_GUID, ADM1_GUID, active.year.01) |>
    dplyr::distinct()

  shapenames <- long.global.dist.01 |>
    tibble::as_tibble() |>
    dplyr::filter(!(ADM0_NAME=="SUDAN" & yr.st == 2000 & active.year.01==2011)) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, ADM1_GUID, active.year.01) |>
    dplyr::distinct()

  # add dummy variable which will appear as missing if no match in area is found
  shapes$match <- 1
  shapenames$match01 <- 1



  # matching by guid and marking unmatched provinces

  afp.linelist.fixed <- dplyr::left_join(afp.linelist.01, shapes, by = c("Admin0GUID" = "ADM0_GUID", "Admin1GUID" = "ADM1_GUID", "yronset" = "active.year.01"))
  afp.linelist.fixed <- afp.linelist.fixed |>
    dplyr::mutate(wrongAdmin1GUID = ifelse(is.na(match) & !(is.na(admin1guid)), "yes", "no"))

  # matching by name and fixing incorrect Admin1 guids which can be matched by name

  afp.linelist.fixed <- afp.linelist.fixed |> dplyr::left_join(shapenames, by = c("place.admin.0" = "ADM0_NAME", "place.admin.1" = "ADM1_NAME", "yronset" = "active.year.01"),
                                                               relationship = "many-to-many")
  afp.linelist.fixed <- afp.linelist.fixed |>
    dplyr::mutate(Admin1GUID = ifelse(wrongAdmin1GUID == "yes" & !is.na(match01), ADM1_GUID, Admin1GUID))


  afp.linelist.fixed <- afp.linelist.fixed |> dplyr::select(-match, -match01)

  shapes <- long.global.dist.01 |>
    tibble::as_tibble() |>
    dplyr::select(ADM0_GUID, ADM1_GUID, GUID, active.year.01) |>
    dplyr::distinct()

  shapenames <- long.global.dist.01 |>
    tibble::as_tibble() |>
    dplyr::filter(!(ADM0_NAME=="SUDAN" & yr.st == 2000 & active.year.01==2011)) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME, GUID, active.year.01) |>
    dplyr::distinct()

  # add dummy variable which will appear as missing if no match in area is found
  shapes$match <- 1
  shapenames$match01 <- 1


  # matching by guid and marking districts which could not be matched
  afp.linelist.fixed.02 <- dplyr::left_join(afp.linelist.fixed, shapes, by = c("Admin0GUID" = "ADM0_GUID", "Admin1GUID" = "ADM1_GUID", "Admin2GUID" = "GUID", "yronset" = "active.year.01"))
  afp.linelist.fixed.02 <- afp.linelist.fixed.02 |>
    dplyr::mutate(wrongAdmin2GUID = ifelse(is.na(match) & !(is.na(admin2guid)), "yes", "no"))



  # matching by name and fixing incorrect Admin2 guids which can be matched by name

  afp.linelist.fixed.02 <- afp.linelist.fixed.02 |> dplyr::left_join(shapenames, by = c("place.admin.0" = "ADM0_NAME", "place.admin.1" = "ADM1_NAME", "place.admin.2" = "ADM2_NAME", "yronset" = "active.year.01"),
                                                                     relationship = "many-to-many")
  afp.linelist.fixed.02 <- afp.linelist.fixed.02 |>
    dplyr::mutate(Admin2GUID = ifelse(wrongAdmin2GUID == "yes" & !is.na(match01), GUID, Admin2GUID))


  afp.linelist.fixed.02 <- afp.linelist.fixed.02 |> dplyr::select(-match, -match01, -ADM1_GUID, -GUID)

  # Now afp.linelist.fixed.02 is identical to afp.linelist.01 but with incorrect guids fixed at
  # the district and province level. There are also two new columns added to indicate wether
  # the province level of district level guid was incorrect.

  #var names created from guid checking
  guid.check.vars <- c("Admin0GUID", "Admin1GUID", "Admin2GUID", "wrongAdmin1GUID", "wrongAdmin2GUID")

  # some info about number of errors
  #table(afp.linelist.fixed.02$wrongAdmin1GUID)
  #table(afp.linelist.fixed.02$wrongAdmin2GUID)

  issuesbyyear <- afp.linelist.fixed.02 |>
    dplyr::group_by(yronset) |>
    summarise(
      numNAprovince = sum(is.na(place.admin.1)),
      numNAdist = sum(is.na(place.admin.2)),
      wrongProvGUID = sum(wrongAdmin1GUID == "yes"),
      wrongdistGUID = sum(wrongAdmin2GUID == "yes"),
      wrongbothGUID = sum(wrongAdmin1GUID == "yes" & wrongAdmin2GUID == "yes")
    )


  issuesbyCtry <- afp.linelist.fixed.02 |>
    dplyr::filter(is.na(place.admin.1) | is.na(place.admin.2) | wrongAdmin1GUID == "yes" | wrongAdmin2GUID == "yes") |>
    dplyr::group_by(place.admin.0) |>
    dplyr::summarise(
      numNAprovince = sum(is.na(place.admin.1)),
      numNAdist = sum(is.na(place.admin.2)),
      wrongProvGUID = sum(wrongAdmin1GUID == "yes"),
      wrongdistGUID = sum(wrongAdmin2GUID == "yes"),
      wrongbothGUID = sum(wrongAdmin1GUID == "yes" & wrongAdmin2GUID == "yes")
    )



  rm("afp.linelist.01", "afp.linelist.fixed", "shapes", "shapenames")

  cli::cli_process_done()

  cli::cli_process_start("Fixing missing GPS locations (this may take a while)")

  afp.linelist.fixed.03 <- afp.linelist.fixed.02 |>
    dplyr::rename(polis.latitude = x,
           polis.longitude = y) |>
    dplyr::distinct()

  rm("afp.linelist.fixed.02")
  gc()


  global.dist.01 <- sirfunctions::load_clean_dist_sp()
  shape.file.names <- names(global.dist.01)
  shape.file.names <- shape.file.names[!shape.file.names %in% c("SHAPE")]
  col.afp.raw.01 <- colnames(afp.raw.01)
  rm("afp.raw.01")
  gc()
  # Function to create lat & long for AFP cases
  afp.linelist.fixed.04 <- f.pre.stsample.01(afp.linelist.fixed.03, global.dist.01)
  rm("afp.linelist.fixed.03")

  #vars created during stsample
  stsample.vars <- c("id", "empty.01", "x")

  cli::cli_process_done()

  cli::cli_process_start("Creating key AFP variables")

  afp.linelist.01 <- afp.linelist.fixed.04 |>
    dplyr::ungroup() |>
    dplyr::mutate(
      bad.onset = dplyr::case_when(
        is.na(dateonset) == T ~ "Missing",
        is.na(datenotify) == T | is.na(dateinvest) == T ~ "Invest or Notify date missing",
        ontonot < 0 & ontoinvest < 0 & is.na(dateonset) == F ~ "Onset after Notif and Invest",
        ontonot < 0 & is.na(dateonset) == F ~ "Onset after Notification",
        ontoinvest < 0 & is.na(dateonset) == F ~ "Onset after Investigation",
        ontonot >= 0 & ontoinvest >= 0 & is.na(dateonset) == F ~ "Good Date"
      ),

      # applying data qa check function which compares date to date of onset
      bad.notify = f.datecheck.onset(datenotify, dateonset),
      bad.invest = f.datecheck.onset(dateinvest, dateonset),
      bad.stool1 = f.datecheck.onset(datestool1, dateonset),
      bad.stool2 = f.datecheck.onset(datestool2, dateonset),
      bad.followup = f.datecheck.onset(followup.date, dateonset),

      # Step 5 Create stool timeliness variable
      # variables exclude bad dates as 77 or Unable to Assess
      # Missing dates treated as absence of collection of stool as there is no variable
      # that specifies stool was not collected

      timeliness.01 =
        dplyr::case_when(
          bad.stool1 == "data entry error" | bad.stool1 == "date before onset" | bad.stool1 == "date onset missing" ~ "Unable to Assess",
          bad.stool2 == "data entry error" | bad.stool2 == "date before onset" | bad.stool2 == "date onset missing" ~ "Unable to Assess",
          ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 & ontostool2 >= 1 & stool1tostool2 >= 1 ~ "Timely",
          is.na(datestool1) == T | is.na(datestool2) == T ~ "Not Timely",
          ontostool1 > 13 | ontostool1 < 0 | ontostool2 > 14 | ontostool2 < 1 | stool1tostool2 < 1 ~ "Not Timely"
        ),

      # Step 6 Create stool adequacy variable
      ## note this is creating three adequacy variables: one assigns missing as "Bad" quality, one excludes missing,
      ## and one assumes missing means "Good" quality (POLIS method)

      # this is assigning all missing as "Bad"
      adequacy.01 =
        dplyr::case_when(
          bad.stool1 == "data entry error" | bad.stool1 == "date before onset" | bad.stool1 == "date onset missing" ~ 77,
          bad.stool2 == "data entry error" | bad.stool2 == "date before onset" | bad.stool2 == "date onset missing" ~ 77,
          ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 & ontostool2 >= 1 & stool1tostool2 >= 1 & is.na(stool1tostool2) == F
          & stool.1.condition == "Good" & stool.2.condition == "Good" ~ 1,
          ontostool1 > 13 | ontostool1 < 0 | ontostool2 > 14 | ontostool2 < 1 | stool1tostool2 < 1 | is.na(stool1tostool2) == T
          | stool.1.condition != "Good" | stool.2.condition != "Good" | is.na(stool.1.condition) == T | is.na(stool.2.condition) == T ~ 0
        ),

      # this is assigning all missing as 99
      adequacy.02 =
        dplyr::case_when(
          bad.stool1 == "data entry error" | bad.stool1 == "date before onset" | bad.stool1 == "date onset missing" ~ 77,
          bad.stool2 == "data entry error" | bad.stool2 == "date before onset" | bad.stool2 == "date onset missing" ~ 77,
          ontostool1 > 13 | ontostool1 < 0 | is.na(stool1tostool2) == T |
            ontostool2 > 14 | ontostool2 < 1 | stool1tostool2 < 1
          | stool.1.condition == "Poor" | stool.2.condition == "Poor" ~ 0,
          is.na(stool.1.condition) == T | is.na(stool.2.condition) == T | stool.1.condition == "Unknown" | stool.2.condition == "Unknown" ~ 99,
          ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 & ontostool2 >= 1 & stool1tostool2 >= 1
          & stool.1.condition == "Good" & stool.2.condition == "Good" ~ 1
        ),

      # this is assuming samples missing condition are "Good" (method used by POLIS to deal with missing condition)
      adequacy.03 =
        dplyr::case_when(
          bad.stool1 == "data entry error" | bad.stool1 == "date before onset" | bad.stool1 == "date onset missing" ~ 77,
          bad.stool2 == "data entry error" | bad.stool2 == "date before onset" | bad.stool2 == "date onset missing" ~ 77,
          ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 & ontostool2 >= 1 & stool1tostool2 >= 1 & is.na(stool1tostool2) == F
          & (stool.1.condition =="Good" | is.na(stool.1.condition) | stool.1.condition == "Unknown") &
            (stool.2.condition =="Good" | is.na(stool.2.condition) | stool.2.condition == "Unknown") ~ 1,
          ontostool1 > 13 | ontostool1 < 0 | ontostool2 > 14 | ontostool2 < 1 | stool1tostool2 < 1 | is.na(stool1tostool2) == T
          | stool.1.condition == "Poor" | stool.2.condition == "Poor" ~ 0
        ),

      # above categorizes missing as 99 meaning that they would be a Yes but do NOT have stool condition
      # 77 means that the dates are usuable and can't assess


      # Step 7 create variables need for flags and 60 day follow-up variable
      # based on old CORE code, but can be combined with bad.invest etc. to limit to only those with interpetable dates

      stool2missing = ifelse(is.na(datestool2) & is.na(stool.2.condition), 1, 0),
      stool1missing = ifelse(is.na(datestool1) & is.na(stool.1.condition), 1, 0),
      stoolmissing = ifelse(stool1missing == 1 & stool2missing == 1, 1, 0),


      ontostool2.03 = NA,
      ontostool2.03 = ifelse(ontostool2 > 14, ">14", ontostool2.03),
      ontostool2.03 = ifelse(ontostool2 <= 14, "<=14", ontostool2.03),


      ontonot.60 = NA,
      ontonot.60 = ifelse(ontonot > 60, ">60", ontonot.60),
      ontonot.60 = ifelse(ontonot <= 60, "<=60", ontonot.60),


      ontonot.14 = NA,
      ontonot.14 = ifelse(ontonot > 14, ">14", ontonot.14),
      ontonot.14 = ifelse(ontonot <= 14, "<=14", ontonot.14),

      # 60 day followup
      need60day = ifelse(timeliness.01 == "Not Timely", 1, 0), # using timeliness only to pull people that need 60 day reviews
      got60day =
        dplyr::case_when(
          need60day == 1 & is.na(followup.date) == F ~ 1,
          need60day == 1 & is.na(followup.date) == T ~ 0,
          need60day == 0 ~ 99
        ),

      timeto60day = followup.date - dateonset,

      ontime.60day =
        dplyr::case_when(
          need60day == 0 ~ 99, # excluded timely cases
          need60day == 1 & timeto60day >= 60 & timeto60day <= 90 ~ 1,
          (need60day == 1 & timeto60day < 60 | timeto60day > 90 | is.na(timeto60day) == T) ~ 0
        ),

      adm0guid = paste("{", stringr::str_to_upper(admin0guid), "}", sep = ""),
      adm0guid = ifelse(adm0guid == "{}" | adm0guid == "{NA}", NA, adm0guid),
      adm1guid = paste("{", stringr::str_to_upper(admin1guid), "}", sep = ""),
      adm1guid = ifelse(adm1guid == "{}" | adm1guid == "{NA}", NA, adm1guid),
      adm2guid = paste("{", stringr::str_to_upper(admin2guid), "}", sep = ""),
      adm2guid = ifelse(adm2guid == "{}" | adm2guid == "{NA}", NA, adm2guid),
    ) |>

    # need to decide range for 60 day reviews completed

    # Step 9 select only variables needed for analyses

  # limited only to raw variables we still use (i,e geography) and created/cleaned variables
  # dropping clinical and RI variables we don't use
  # code below depends on data structure staying the same
  # update if data structure changes

  # renaming POLIS original variables to match the variables in afp line list for CORE analysis
  dplyr::rename(adequate.stool = stool.adequacy,
         datasetlab = dataset.lab,
         doses.total = doses,
         # totalnumberofdosesipvopv = `total.number.of.ipv./.opv.doses`,
         virus.cluster = `virus.cluster(s)`,
         emergence.group = `emergence.group(s)`
  ) |>
    dplyr::filter(!is.na(epid)) |>
    dplyr::select(-c(shape.file.names, guid.check.vars, stsample.vars))

  rm("afp.linelist.fixed.04")
  gc()
  cli::cli_process_done()

  cli::cli_process_start("Checking duplicated AFP EPIDs")

  afp.linelist.02 <- afp.linelist.01 |>
    dplyr::filter(surveillancetypename == "AFP") |>
    dplyr::distinct()# this gives us an AFP line list

  #identify duplicate EPIDs with differing place and onsets
  dup.epid <- afp.linelist.02 |>
    dplyr::group_by(epid) |>
    dplyr::mutate(dup_epid = dplyr::n()) |>
    dplyr::filter(dup_epid > 1) |>
    dplyr::ungroup() |>
    dplyr::select(epid, date.onset, yronset, diagnosis.final, classification, classificationvdpv,
           final.cell.culture.result, poliovirustypes, person.sex, place.admin.0,
           place.admin.1, place.admin.2) |>
    dplyr::distinct() |>
    dplyr::arrange(epid)

  tidypolis_io(obj = dup.epid, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/", paste("duplicate_AFP_epids_Polis",
                                                      min(dup.epid$yronset, na.rm = T),
                                                      max(dup.epid$yronset, na.rm = T),
                                                      sep = "_"), ".csv", sep = "")
  )

  # remove duplicates in afp linelist
  afp.linelist.02 <- afp.linelist.02[!duplicated(afp.linelist.02$epid), ]

  cli::cli_process_done()

  cli::cli_process_start("Checking Missing AFP EPIDs and GUIDs")

  not.afp <- afp.linelist.01 |>
    dplyr::filter(surveillancetypename != "AFP") # this gives us non-AFP line list

  # remove duplicates in non-afp linelist
  not.afp <- not.afp[!duplicated(not.afp$epid), ]

  unknown.afp <- afp.linelist.01 |>
    dplyr::filter(is.na(surveillancetypename) == T) # this gives us unknown surveillance type line list


  # Step 10: Create excel files showing AFP cases with unmatched guids by country and year
  afp.missing.01 <- afp.linelist.02 |>
    dplyr::filter(is.na(admin1guid) == T | is.na(admin2guid) == T) |>
    dplyr::group_by(place.admin.0, yronset) |>
    dplyr::summarise(case.miss.guid = dplyr::n()) |>
    tidyr::pivot_wider(names_from = yronset, values_from = case.miss.guid)

  afp.missing.02 <- afp.linelist.02 |>
    dplyr::filter(is.na(admin1guid) == T | is.na(admin2guid) == T) |>
    dplyr::select(polis.case.id, epid, place.admin.0, place.admin.1, place.admin.2,
           date.onset, yronset, adm0guid, adm1guid, admin2guid)

  tidypolis_io(obj = afp.missing.01, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                  paste("afp_missing_guid_count", min(afp.linelist.01$dateonset, na.rm = T), max(afp.linelist.01$dateonset, na.rm = T), sep = "_"),
                                  ".csv",
                                  sep = ""
  ))

  tidypolis_io(obj = afp.missing.02, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                  paste("afp_missing_guid_epids", min(afp.linelist.01$dateonset, na.rm = T), max(afp.linelist.01$dateonset, na.rm = T), sep = "_"),
                                  ".csv",
                                  sep = ""
  ))

  cli::cli_process_done()

  cli::cli_process_start("Comparing data with last AFP dataset")

  afp.linelist.02 <- afp.linelist.02 |>
    dplyr::mutate(polis.latitude = as.character(polis.latitude),
           polis.longitude = as.character(polis.longitude),
           doses.total = as.numeric(doses.total)
           # virus.sequenced = as.logical(virus.sequenced),
    )

  x <- tidypolis_io(io = "list", file_path = paste0(polis_data_folder, "/Core_Ready_Files/Archive/", latest_folder_in_archive), full_names = T)

  old.file <- x[grepl("afp_linelist_2020", x)]

  if(length(old.file) > 0){
    old <- tidypolis_io(io = "read", file_path = old.file) |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::mutate_all(as.character)

    old <- old |>
      dplyr::mutate(yronset = as.numeric(yronset)) |>
      dplyr::filter(dplyr::between(yronset, startyr, endyr)) |>
      dplyr::mutate_all(as.character)

    afp.linelist.02 <- afp.linelist.02 |>
      dplyr::ungroup() |>
      dplyr::select(-c(setdiff(setdiff(colnames(afp.linelist.02), col.afp.raw.01), colnames(old))))

    # Step 11 write R datafiles for use in analyses
    #Compare the final file to last week's final file to identify any differences in var_names, var_classes, or categorical responses
    new_table_metadata <- f.summarise.metadata(head(afp.linelist.02, 1000))
    old_table_metadata <- f.summarise.metadata(head(old, 1000))
    afp_metadata_comparison <- f.compare.metadata(new_table_metadata, old_table_metadata, "AFP")

    #compare obs
    new <- afp.linelist.02 |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::mutate_all(as.character)

    in_new_not_old <- new[!(new$epid %in% old$epid),]

    in_old_not_new <- old[!(old$epid %in% new$epid),]

    in_new_and_old_but_modified <- ungroup(new) |>
      dplyr::filter(epid %in% old$epid) |>
      dplyr::select(-c(setdiff(colnames(new), colnames(old))))


    in_new_and_old_but_modified <- setdiff(in_new_and_old_but_modified, old |>
                                             dplyr::select(-c(setdiff(colnames(old), colnames(new)))))

    if(nrow(in_new_and_old_but_modified) > 0){
      in_new_and_old_but_modified <- in_new_and_old_but_modified |>
        dplyr::inner_join(old |>
                            dplyr::filter(epid %in% in_new_and_old_but_modified$epid) |>
                            dplyr::select(-c(setdiff(colnames(old), colnames(new)))) |>
                            setdiff(new |>
                                      select(-c(setdiff(colnames(new), colnames(old))))), by="epid") |>
        #wide_to_long
        tidyr::pivot_longer(cols=-epid) |>
        dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
        dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
        #long_to_wide
        tidyr::pivot_wider(names_from=source, values_from=value) |>
        dplyr::filter(new != old & !name %in% c("lat", "lon"))


    }

    update_polis_log(.event = paste0("AFP New Records: ", nrow(in_new_not_old), "; ",
                                     "AFP Removed Records: ", nrow(in_old_not_new), "; ",
                                     "AFP Modified Records: ", length(unique(in_new_and_old_but_modified$epid))),
                     .event_type = "INFO")

  }

  tidypolis_io(obj = afp.linelist.02, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                   paste("afp_linelist", min(afp.linelist.02$dateonset, na.rm = T), max(afp.linelist.02$dateonset, na.rm = T), sep = "_"),
                                   ".rds",
                                   sep = ""
  ))


  cli::cli_process_done()

  cli::cli_process_start("Create AFP Lat/Lon list")

  # note above is keeping the cases with missing date of onset and creating a giant new data dataet

  # Step 12 write.csv for AFP cases for Lat and long

  afp.linelist.latlong <- afp.linelist.01 |>
    dplyr::ungroup() |>
    dplyr::select(epid, dateonset, place.admin.0, place.admin.1, place.admin.2, adm0guid, adm1guid, adm2guid, cdc.classification.all, lat, lon)

  tidypolis_io(obj = afp.linelist.latlong, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                        paste("afp_lat_long", min(afp.linelist.latlong$dateonset, na.rm = T), max(afp.linelist.latlong$dateonset, na.rm = T), sep = "_"),
                                        ".csv",
                                        sep = ""
  ))

  cli::cli_process_done()

  cli::cli_process_start("Comparing data with last non-AFP dataset")

  # Step 13 write.rds file for non AFP type cases
  not.afp.01 <- dplyr::bind_rows(not.afp, unknown.afp)
  not.afp.01 <- not.afp.01 |>
    dplyr::mutate(polis.latitude = as.character(polis.latitude),
           polis.longitude = as.character(polis.longitude),
           doses.total = as.numeric(doses.total)
    )

  x <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder,"Core_Ready_Files", "Archive", latest_folder_in_archive), full_names = T)

  old.file <- x[grepl("other_surveillance_type_linelist_2020", x)]

  if(length(old.file)>0){
    old <- tidypolis_io(io = "read", file_path = old.file) |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::mutate_all(as.character)

    not.afp.01 <- not.afp.01 |>
      dplyr::ungroup() |>
      dplyr::select(-c(setdiff(setdiff(colnames(not.afp.01), col.afp.raw.01), colnames(old))))


    #Compare the final file to last week's final file to identify any differences in var_names, var_classes, or categorical responses
    new_table_metadata <- f.summarise.metadata(not.afp.01)
    old_table_metadata <- f.summarise.metadata(old)
    not_afp_metadata_comparison <- f.compare.metadata(new_table_metadata, old_table_metadata, "Other Surv")
    #compare obs
    new <- not.afp.01 |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::mutate_all(as.character)

    in_new_not_old <- new |>
      dplyr::filter(!(epid %in% old$epid))

    in_old_not_new <- old |>
      dplyr::filter(!(epid %in% new$epid))

    in_new_and_old_but_modified <- new |>
      dplyr::filter(epid %in% old$epid) |>
      dplyr::select(-c(setdiff(colnames(new), colnames(old)))) |>
      setdiff(old |>
                dplyr::select(-c(setdiff(colnames(old), colnames(new))))) |>
      dplyr::inner_join(old |>
                          dplyr::filter(epid %in% new$epid) |>
                          dplyr::select(-c(setdiff(colnames(old), colnames(new)))) |>
                          dplyr::setdiff(new |>
                                           dplyr::select(-c(setdiff(colnames(new), colnames(old))))), by="epid") |>
      #wide_to_long
      tidyr::pivot_longer(cols=-epid) |>
      dplyr::mutate(source = ifelse(str_sub(name, -2) == ".x", "new", "old")) |>
      dplyr::mutate(name = str_sub(name, 1, -3)) |>
      #long_to_wide
      tidyr::pivot_wider(names_from=source, values_from=value) |>
      dplyr::filter(new != old & !name %in% c("lat", "lon"))

    update_polis_log(.event = paste0("Other Surveillance New Records: ", nrow(in_new_not_old), "; ",
                                     "Other Surveillance Removed Records: ", nrow(in_old_not_new), "; ",
                                     "Other Surveillance Modified Records: ", length(unique(in_new_and_old_but_modified$epid))),
                     .event_type = "INFO")

  }



  tidypolis_io(obj = not.afp.01, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                              paste("other_surveillance_type_linelist", min(not.afp.01$yronset, na.rm = T), max(not.afp.01$yronset, na.rm = T), sep = "_"),
                              ".rds",
                              sep = ""
  ))

  cli::cli_process_done()


  cli::cli_process_start("Generating AFP dataset")
  # AFP cases with EPIDs ("14070210003" "50023710003" "Per 011-21"  "53060210001") got removed from original POLIS download "Cases_30-04-2020_20-15-38_from_01_Jan_2000_to_31_Dec_2018.csv"

  #move newly created linelists to the "core datafiles to combine" folder within core 2.0

  # read AFP surveillance type linelist and combine to make one AFP-linlelist
  afp.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path=paste0(polis_data_folder, "/Core_Ready_Files"), full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(afp_linelist).*(.rds)$", name)) |>
    dplyr::pull(name)
  afp.files.02 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path=paste0(polis_data_folder, "/core_files_to_combine"), full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(afp_linelist).*(.rds)$", name)) |>
    dplyr::pull(name)
  afp.to.combine <- purrr::map_df(afp.files.02, ~tidypolis_io(io = "read", file_path = .x)) |>
    dplyr::mutate(stool1tostool2 = as.numeric(stool1tostool2),
                  datenotificationtohq =  lubridate::parse_date_time(datenotificationtohq, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")))
  afp.new <- purrr::map_df(afp.files.01, ~tidypolis_io(io = "read", file_path = .x))

  afp.clean.01 <- dplyr::bind_rows(afp.new, afp.to.combine)



  tidypolis_io(obj = afp.clean.01, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                paste("afp_linelist", min(afp.clean.01$dateonset, na.rm = T),
                                      max(afp.clean.01$dateonset, na.rm = T),
                                      sep = "_"
                                ),".rds",
                                sep = ""
  ))
  cli::cli_process_done()

  cli::cli_process_start("Creating light AFP dataset for WHO")
  #outputting lighter file for WHO
  afp.clean.light <- afp.clean.01 |>
    dplyr::filter(yronset >= 2019)

  tidypolis_io(obj = afp.clean.light, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                   paste("afp_linelist", min(afp.clean.light$dateonset, na.rm = T),
                                         max(afp.clean.light$dateonset, na.rm = T),
                                         sep = "_"
                                   ),".rds",
                                   sep = ""
  ))

  cli::cli_process_done()

  cli::cli_process_start("Creating non-AFP dataset")

  #other surveillance linelist combine
  non.afp.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path=paste0(polis_data_folder, "/Core_Ready_Files"), full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(other_surveillance_type_linelist).*(.rds)$", name)) |>
    dplyr::pull(name)
  non.afp.files.02 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path=paste0(polis_data_folder, "/core_files_to_combine"), full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(other_surveillance_type_linelist).*(.rds)$", name)) |>
    dplyr::pull(name)
  non.afp.to.combine <- purrr::map_df(non.afp.files.02, ~tidypolis_io(io = "read", file_path = .x)) |>
    dplyr::mutate(stool1tostool2 = as.numeric(stool1tostool2),
                  datenotificationtohq =  lubridate::parse_date_time(datenotificationtohq, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")))
  non.afp.new <- purrr::map_df(non.afp.files.01, ~tidypolis_io(io = "read", file_path = .x))

  non.afp.clean.01 <- dplyr::bind_rows(non.afp.new, non.afp.to.combine)


  tidypolis_io(obj = non.afp.clean.01, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                    paste("other_surveillance_type_linelist", min(non.afp.clean.01$yronset, na.rm = T),
                                          max(non.afp.clean.01$yronset, na.rm = T),
                                          sep = "_"
                                    ),".rds",
                                    sep = ""
  ))

  cli::cli_process_done()

  update_polis_log(.event = "AFP and Other Surveillance Linelists Finished",
                   .event_type = "PROCESS")

  cli::cli_process_start("Clearing memory")

  rm('afp.clean.01', 'afp.clean.light', 'afp.files.01', 'afp.files.02',
      'afp.linelist.01', 'afp.linelist.02', 'afp.linelist.latlong',
     'afp.missing.01', 'afp.missing.02', 'afp_metadata_comparison',
     'col.afp.raw.01', 'dup.epid', 'issuesbyCtry', 'issuesbyyear',
     'endyr', 'global.dist.01', 'in_new_and_old_but_modified',
     'in_new_not_old', 'in_old_not_new',
     'new', 'new_table_metadata', 'non.afp.clean.01', 'non.afp.files.01',
     'non.afp.files.02', 'non.afp.files.03',
     'not.afp', 'not.afp.01', 'not_afp_metadata_comparison', 'old',
     'old.file', 'old_table_metadata', 'startyr',
     'unknown.afp', 'x', "afp.new", "afp.to.combine", "non.afp.new",
     "non.afp.to.combine"
  )

  gc()

  cli::cli_process_done()

  #Step 3 - Creating SIA datasets ====
  update_polis_log(.event = "Creating SIA analytic datasets",
                   .event_type = "PROCESS")

  cli::cli_h1("Step 3/5 - Creating SIA analytic datasets")

  # Step 1: Read in "old" data file (System to find "Old" data file)
  x <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files/Archive", latest_folder_in_archive), full_names = T)

  y <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"), full_names = T)


  old.file <- x[grepl("Activity",x)]

  new.file <- y[grepl("Activity", y)]


  cli::cli_process_start("Loading new SIA data")
  # Step 1: Read in "new" data file
  # Newest downloaded activity file, will be .rds located in Core Ready Files
  sia.01.new <- tidypolis_io(io = "read", file_path = new.file) |>
    dplyr::mutate_all(as.character) |>
    dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
    dplyr::mutate_all(list(~dplyr::na_if(.,"")))

  # Make the names small case and remove space bar from names
  names(sia.01.new) <- stringr::str_to_lower(names(sia.01.new))
  cli::cli_process_done()

  # QC CHECK
  # This is for checking data across different download options

  if(length(x) > 0){

    cli::cli_process_start("Loading old SIA data")
    # Old pre-existing download
    # This is previous Activity .rds that was preprocessed last week, it has been moved to the archive, change archive subfolder and specify last weeks Activity .rds
    sia.01.old <- tidypolis_io(io = "read", file_path = old.file) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~dplyr::na_if(.,"")))

    names(sia.01.old) <- stringr::str_to_lower(names(sia.01.old))
    cli::cli_process_done()

    # Are there differences in the names of the columns between two downloads?
    f.compare.dataframe.cols(sia.01.old, sia.01.new)

    # If okay with above then proceed to next step
    # First determine the variables that would be excluded from the comparison
    # this includes variables such as EPID numbers which would invariably change in values

    var.list.01 <- c(
      "sia.code", "sia.sub-activity.code", "country", "country.iso3", "who.region", "ist", "activity.start.date", "activity.end.date",
      "last.updated.date", "sub-activity.start.date", "sub-activity.end.date", "admin1",
      "admin2", "delay.reason", "priority", "country.population.%", "unpd.country.population", "targeted.population",
      "immunized.population", "admin.coverage.%", "area.targeted.%", "area.population", "age.group.%", "wastage.factor",
      "number.of.doses", "number.of.doses.approved", "im.loaded", "lqas.loaded", "sub-activity.last.updated.date", "admin.2.targeted.population",
      "admin.2.immunized.population", "admin.2.accessibility.status", "admin.2.comments", "sub-activity.initial.planned.date",
      "activity.parent.children.inaccessible", "children.inaccessible", "admin2.children.inaccessible", "linked.outbreak(s)",
      "admin.0.guid", "admin.1.guid", "admin.2.guid"
    )

    cli::cli_process_start("Comparing downloaded variables")
    # Exclude the variables from
    sia.01.old.compare <- sia.01.old |>
      dplyr::select(-var.list.01)

    sia.01.new.compare <- sia.01.new |>
      dplyr::select(-var.list.01)

    new.var.sia.01 <- f.download.compare.01(sia.01.old.compare, sia.01.new.compare)

    new.df <- new.var.sia.01 |>
      dplyr::filter(is.na(old.distinct.01) | diff.distinct.01 >= 1) |>
      dplyr::filter(!variable %in% c("parentid", "id"))

    if (nrow(new.df) >= 1) {
      cli::cli_alert_danger("There is either a new variable in the SIA data or new value of an existing variable.
       Please run f.download.compare.02 to see what it is. Preprocessing can not continue until this is adressed.")

      sia.new.value <- f.download.compare.02(new.var.sia.01, sia.01.old.compare, sia.01.new.compare)

      update_polis_log(.event = sapply(names(sia.new.value), function(x) paste0("New values in: ", x, " - ", paste0(unique(dplyr::pull(sia.new.value, x)), collapse = ", "))) |>
                         paste0(collapse = "; "),
                       .event_type = "ALERT")

    } else {

      cli::cli_alert_info("New SIA download is comparable to old SIA download")

    }

  }else{
    cli::cli_alert_info("No old SIA data found, will not perform comparisons")
  }

  cli::cli_process_done()

  # Get the date from 'Mon-year' format of parent start date as a first day of the month.
  # this would insert first of month if date is missing from the activity date. This
  # leads to errors if one is trying to guess duration of activity. For negative duration ignore it.
  # Active year at country level is start yr of activity
  # active year at prov and district level is start yr of sub-activity

  cli::cli_process_start("Create CDC variables")
  endyr <- year(format(Sys.time()))
  startyr <- 2020
  sia.02 <- sia.01.new |>
    dplyr::rename(linked.obx=`linked.outbreak(s)`,
           sia.sub.activity.code = `sia.sub-activity.code`,
           sub.activity.end.date = `sub-activity.end.date`,
           sub.activity.start.date = `sub-activity.start.date`,
           sub.activity.last.updated.date = `sub-activity.last.updated.date`,
           sub.activity.initial.planned.date = `sub-activity.initial.planned.date`
           #`country.population.%` = `country.population.%`,
           #`area.targeted.%` = `area.targeted.%`,
           #`age.group.%`=`age.group.%`,
           #`admin.coverage.%` = `admin.coverage.%`
    ) |>
    dplyr::mutate(
      # Checked with Valentina at WHO. lqas and im loaded variables are not correctly populated in the POLIS. Valentina will fix this issue.
      #status = ifelse((im.loaded == "yes" & is.na(status == T)) | (lqas.loaded == "yes" & is.na(status == T)),
      #                "Done", status),
      status = tidyr::replace_na(status, "Missing"),
      vaccine.type = tidyr::replace_na(vaccine.type, "Missing")
    ) |>

    dplyr::mutate_at(
      dplyr::vars(sub.activity.end.date), ~ dplyr::na_if(., "undefined")
    ) |>
    dplyr::mutate_at(
      dplyr::vars(
        sub.activity.initial.planned.date, sub.activity.last.updated.date,
        activity.start.date, activity.end.date,
        sub.activity.start.date, sub.activity.end.date,
        last.updated.date
      ), ~ lubridate::parse_date_time(., c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S"))
    ) |>
    dplyr::mutate(
      yr.sia = lubridate::year(activity.start.date),
      yr.subsia = lubridate::year(sub.activity.start.date),
      month.sia = lubridate::month(activity.start.date),
      month.subsia = lubridate::month(activity.start.date),
      `admin.coverage.%` = as.numeric(`admin.coverage.%`)
    ) |>
    dplyr::rename(
      place.admin.0 = country,
      place.admin.1 = admin1,
      place.admin.2 = admin2
    ) |>
    dplyr::filter(dplyr::between(yr.sia, startyr, endyr)) |>
    # Pakistan does not match to shape file for 2008 and 2009 for
    # KP and FATA as name change. They were called NWFP then
    dplyr::mutate(
      place.admin.1 = dplyr::case_when(
        (yr.sia == 2009 | yr.sia == 2008) & (place.admin.1 == "KHYBER PAKHTOON" | place.admin.1 == "FATA") ~ "NWFP",
        TRUE ~ as.character(place.admin.1)
      ),
      place.admin.0 = ifelse(stringr::str_detect(place.admin.0, "IVOIRE"),"COTE D IVOIRE",place.admin.0)
    ) |>
    dplyr::mutate(
      adm2guid  = paste0("{", toupper(admin.2.guid), "}"),
      adm1guid  = paste0("{", toupper(admin.1.guid), "}"),
      adm0guid  = paste0("{", toupper(admin.0.guid), "}")
    ) |>
    dplyr::select(-admin.0.guid, -admin.1.guid, -admin.2.guid) |>
    dplyr::distinct()

  cli::cli_process_done()

  cli::cli_process_start("Checking GUIDs")
  # SIA file with GUID
  # SIAs match with GUIDs in shapes
  sia.03 <- sia.02 |>
    dplyr::left_join(long.global.dist.01 |> dplyr::select(GUID, ADM0_NAME, ADM1_NAME, ADM2_NAME, active.year.01), by=c("adm2guid"="GUID", "yr.sia"="active.year.01"))|>
    dplyr::mutate(no_match=ifelse(is.na(ADM2_NAME)==T, 1, 0))

  # SIAs did not match with GUIDs in shapes.
  tofix <- sia.03 |>
    dplyr::select(-ADM0_NAME, -ADM1_NAME, -ADM2_NAME) |>
    dplyr::filter(no_match==1) |>
    dplyr::left_join(long.global.dist.01 |> dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME, active.year.01, GUID),
              by = c("place.admin.0" = "ADM0_NAME", "place.admin.1" = "ADM1_NAME", "place.admin.2" = "ADM2_NAME", "yr.sia" = "active.year.01")) |>
    dplyr::mutate(missing.guid = ifelse(is.na(GUID)==T, 1, 0),
           adm2guid = ifelse(missing.guid==0, GUID, adm2guid)) |>
    dplyr::select(-GUID, -no_match)

  # Combine SIAs matched with prov, dist with shapes
  sia.04 <- sia.03 |>
    dplyr::filter(no_match==0)|>
    dplyr::bind_rows(tofix)|>
    dplyr::mutate(place.admin.1=ifelse(is.na(place.admin.1)==T, ADM1_NAME, place.admin.1),
           place.admin.2=ifelse(is.na(place.admin.2)==T, ADM2_NAME, place.admin.2)) |>
    dplyr::select(-no_match)

  # Next step is to remove duplicates:
  sia.05 <- dplyr::distinct(sia.04, adm2guid, sub.activity.start.date, vaccine.type, age.group, status, lqas.loaded, im.loaded, .keep_all= TRUE)

  cli::cli_process_done()

  cli::cli_process_start("Checking for duplicates")
  # Another step to check duplicates:
  savescipen <- getOption("scipen")
  options(scipen = 999)
  sia.06 <- sia.05 |>
    dplyr::mutate(sub.activity.start.date = lubridate::as_date(sub.activity.start.date)) |>
    dplyr::arrange(sub.activity.start.date) |>
    dplyr::group_by(adm2guid, vaccine.type) |>
    dplyr::mutate(camp.diff.days = as.numeric(sub.activity.start.date - dplyr::lag(sub.activity.start.date))) |>
    #this creates variable that is days from last campaign of that vaccine
    dplyr::mutate(dup=dplyr::case_when(camp.diff.days==0 & !is.na(place.admin.2)~ 1,
                         camp.diff.days>0 | is.na(place.admin.2)==T | is.na(camp.diff.days)==T ~ 0)) |>

    #manually removing extra duplicates
    dplyr::filter(sia.sub.activity.code!="PAK-2021-006-1") |>
    dplyr::filter(sia.sub.activity.code!="SOM-2000-002-2") |>
    dplyr::select(-missing.guid, -ADM0_NAME, -ADM1_NAME, -ADM2_NAME) |>
    dplyr::select(-c(camp.diff.days, dup)) |>
    #change class of vars to match old file
    dplyr::mutate(`country.population.%` = as.character(`country.population.%`),
           `area.targeted.%` = as.character(`area.targeted.%`),
           `age.group.%` = as.character(`age.group.%`),
           `wastage.factor` = as.character(`wastage.factor`),
           sub.activity.start.date = as.POSIXct(round(as.POSIXct(sub.activity.start.date)), format="%Y-%m-%d %H:%M:%S")
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate_at(c("admin.1.id",
                "admin.2.id",
                "unpd.country.population",
                "immunized.population",
                "targeted.population",
                "number.of.doses",
                "admin.2.targeted.population",
                "admin.2.immunized.population",
                "admin2.children.inaccessible",
                "number.of.doses.approved",
                "children.inaccessible",
                "activity.parent.children.inaccessible",
                "admin.0.id" ),
              as.numeric)
  options(scipen = savescipen)

  cli::cli_process_done()

  cli::cli_process_start("Checking metadata")
  # This is the final SIA file which would be used for analysis.
  #Compare the final file to last week's final file to identify any differences in var_names, var_classes, or categorical responses
  sia.06 <- sia.06 |>
    dplyr::select(-dplyr::starts_with("SHAPE"))

  old.file <- x[grepl("sia_2020", x)]

  if(length(old.file) > 0){

    new_table_metadata <- f.summarise.metadata(sia.06)
    old <- tidypolis_io(io = "read", file_path = old.file)
    old_table_metadata <- f.summarise.metadata(old)
    sia_metadata_comparison <- f.compare.metadata(new_table_metadata, old_table_metadata, "SIA")

    #check obs in new and old
    old <- old |>
      dplyr::mutate_all(as.character)
    new <- sia.06 |>
      dplyr::mutate_all(as.character)
    in_old_not_new <- old |>
      dplyr::anti_join(new, by=c("sia.sub.activity.code", "adm2guid"))
    in_new_not_old <- new |>
      dplyr::anti_join(old, by=c("sia.sub.activity.code", "adm2guid"))
    in_new_and_old_but_modified <- new |>
      dplyr::group_by(sia.sub.activity.code, adm2guid) |>
      dplyr::slice(1) |>
      dplyr::ungroup() |>
      dplyr::inner_join(old |>
                          dplyr::group_by(sia.sub.activity.code, adm2guid) |>
                          dplyr::slice(1) |>
                          dplyr::ungroup(), by=c("sia.sub.activity.code", "adm2guid")) |>
      dplyr::select(-c(setdiff(colnames(new), colnames(old)))) |>
      # setdiff(., old |>
      #           select(-c(setdiff(colnames(old), colnames(new))))) |>
      # #wide_to_long
      tidyr::pivot_longer(cols=-c("sia.sub.activity.code", "adm2guid")) |>
      dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
      dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
      #long_to_wide
      tidyr::pivot_wider(names_from=source, values_from=value) |>
      dplyr::filter(new != old)

    update_polis_log(.event = paste0("SIA New Records: ", nrow(in_new_not_old), "; ",
                                     "SIA Removed Records: ", nrow(in_old_not_new), "; ",
                                     "SIA Modified Records: ", length(unique(in_new_and_old_but_modified$sia.sub.activity.code))),
                     .event_type = "INFO")

  }else{
    cli::cli_alert_info("No old SIA file identified, will not perform comparisons")
  }

  cli::cli_process_done()

  cli::cli_process_start("Writing out SIA file")
  # Write final SIA file to RDS file
  sia.file.path <- paste(polis_data_folder, "/Core_Ready_Files/", sep = "")

  tidypolis_io(obj = sia.06, io = "write", file_path = paste(sia.file.path,
                          paste("sia", min(sia.06$yr.sia, na.rm = T), max(sia.06$yr.sia, na.rm = T), sep = "_"),
                          ".rds",
                          sep = ""
  ))
  cli::cli_process_done()

  #combine SIA pre-2020 with the current rds
  # read SIA and combine to make one SIA dataset

  sia.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path=paste0(polis_data_folder, "/Core_Ready_Files"), full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(sia).*(.rds)$", name)) |>
    dplyr::pull(name)
  sia.files.02 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path=paste0(polis_data_folder, "/core_files_to_combine"), full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(sia).*(.rds)$", name)) |>
    dplyr::pull(name)
  sia.to.combine <- purrr::map_df(sia.files.02, ~tidypolis_io(io = "read", file_path = .x)) |>
    dplyr::mutate(sub.activity.initial.planned.date = lubridate::parse_date_time(sub.activity.initial.planned.date, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),
                  last.updated.date = lubridate::parse_date_time(last.updated.date, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),
                  sub.activity.last.updated.date = as.Date(lubridate::parse_date_time(sub.activity.last.updated.date, c("dmY", "bY", "Ymd", "%d-%m-%Y %H:%M"))))
  sia.new <- purrr::map_df(sia.files.01, ~tidypolis_io(io = "read", file_path = .x))

  sia.clean.01 <- dplyr::bind_rows(sia.new, sia.to.combine) |>
    mutate(sub.activity.last.updated.date = as.Date(sub.activity.last.updated.date),
           last.updated.date = as.Date(last.updated.date)) |>
    dplyr::select(sia.code, sia.sub.activity.code, everything())

  cluster_dates_for_sias(sia.clean.01)

  tidypolis_io(obj = sia.clean.01, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                paste("sia", min(sia.clean.01$yr.sia, na.rm = T),
                                      max(sia.clean.01$yr.sia, na.rm = T),
                                      sep = "_"
                                ),".rds",
                                sep = ""
  ))

  cli::cli_process_start("Evaluating unmatched SIAs")

  # Identify the SIAs did not match to shape file.
  # Each SIA by district is a separate row

  dist.sia.mismatch.01 <- sia.05 |>
    dplyr::filter(missing.guid == 1)

  # Summary list of non-matching SIA to shape file
  # by country by year

  cty.yr.mismatch <- dist.sia.mismatch.01 |>
    dplyr::group_by(place.admin.0, yr.sia) |>
    dplyr::summarise(no.of.mismatch.sia = n())

  # excel file summarizing mismatch SIA by country

  tidypolis_io(obj = cty.yr.mismatch, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                   paste("ctry_sia_mismatch", min(cty.yr.mismatch$yr.sia, na.rm = T),
                                         max(cty.yr.mismatch$yr.sia, na.rm = T),
                                         sep = "_"
                                   ),
                                   ".csv",
                                   sep = ""
  ))

  cli::cli_process_done()
  # the curly brace below is closure of else statement. Do not delete

  update_polis_log(.event = "SIA Finished",
                   .event_type = "PROCESS")

  cli::cli_process_done("Clearing memory")
  rm(
    'cty.yr.mismatch',
    'dist.sia.mismatch.01', 'endyr',
    'in_new_and_old_but_modified', 'in_new_not_old',
    'in_old_not_new', 'new', 'new.df', 'new.file',
    'new.var.sia.01', 'new_table_metadata', 'old', 'old.file',
    'old_table_metadata', 'savescipen',
    'sia.01.new', 'sia.01.new.compare', 'sia.01.old',
    'sia.01.old.compare', 'sia.02', 'sia.03', 'sia.04', 'sia.05',
    'sia.06', 'sia.clean.01', 'sia.files.01', 'sia.files.02',
    'sia_metadata_comparison', 'sia.file.path',
    'startyr', 'tofix', 'var.list.01', "sia.new", "sia.to.combine"
  )

  cli::cli_process_done()
  #Step 4 - Creating ES datasets====
  update_polis_log(.event = "Creating ES analytic datasets",
                   .event_type = "PROCESS")

  cli::cli_h1("Step 4/5 - Creating ES analytic datasets")

  # Step 1: Read in "old" data file (System to find "Old" data file)
  old.file <- x[grepl("EnvSamples",x)]

  new.file <- y[grepl("EnvSamples", y)]

  es.01.new <- tidypolis_io(io = "read", file_path = new.file) |>
    dplyr::mutate_all(as.character) |>
    dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
    dplyr::mutate_all(list(~dplyr::na_if(.,"")))

  names(es.01.new) <- stringr::str_to_lower(names(es.01.new))

  if(length(old.file) > 0){
    es.01.old <- tidypolis_io(io = "read", file_path = old.file) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~dplyr::na_if(.,"")))

    # Modifing POLIS variable names to make them easier to work with
    names(es.01.old) <- stringr::str_to_lower(names(es.01.old))

    # Are there differences in the names of the columns between two downloads?
    f.compare.dataframe.cols(es.01.old, es.01.new)

    var.list.01 <- c(
      "env.sample.id", "env.sample.manual.edit.id", "sample.id", "worksheet.name", "labid",
      "site.comment", "y", "x", "collection.date", "npev", "under.process", "is.suspected","advanced.notification",
      "date.shipped.to.ref.lab", "region.id", "region.official.name", "admin.0.officialname", "admin.1.id",
      "admin.1.officialname", "admin.2.id", "admin.2.officialname", "updated.date", "publish.date", "uploaded.date",
      "uploaded.by",  "reporting.year", "date.notification.to.hq", "date.received.in.lab", "created.date", "date.f1.ref.itd",
      "date.f2.ref.itd", "date.f3.ref.itd","date.f4.ref.itd","date.f5.ref.itd", "date.f6.ref.itd"
    )


    ## Exclude the variables from
    es.02.old <- es.01.old |>
      dplyr::select(-dplyr::all_of(var.list.01))

    es.02.new <- es.01.new |>
      dplyr::select(-dplyr::all_of(var.list.01))

    new.var.es.01 <- f.download.compare.01(es.02.new, es.02.old)

    new.df <- new.var.es.01 |>
      dplyr::filter(is.na(old.distinct.01) | diff.distinct.01 >= 1) |>
      dplyr::filter(variable != "id")

    if (nrow(new.df) >= 1) {
      cli::cli_alert_danger("There is either a new variable in the ES data or new value of an existing variable.
          Please run f.download.compare.01 to see what it is. Preprocessing can not continue until this is adressed.")


      es.new.value <- f.download.compare.02(new.var.es.01 |> filter(!(is.na(old.distinct.01)) & variable != "id"), es.02.old, es.02.new)


      update_polis_log(.event = sapply(names(es.new.value), function(x) paste0("New Values in: ", x, " - ", paste0(unique(dplyr::pull(es.new.value, x)), collapse = ", "))) |>
                         paste0(collapse = "; "),
                       .event_type = "ALERT")

    }else{
      cli::cli_alert_info("No variable change errors")
    }

    remove("es.01.old", "es.02.old")
  }else{
    cli::cli_alert_info("No ES file found in archives")
  }


  # Data manipulation

  # Renaming and creating variables
  es.02 <- es.01.new |>
    dplyr::rename(
      province = admin.1.officialname,
      district = admin.2.officialname,
      ctry.guid = admin.0.guid,
      prov.guid = admin.1.guid,
      dist.guid = admin.2.guid,
      lat = y,
      lng = x,
      vdpv.classification.id = `vdpv.classification.id(s)`,
      vdpv.classification = `vdpv.classification(s)`,
      is.advanced.notification = advanced.notification,
      virus.cluster = `virus.cluster(s)`,
      emergence.group = `emergence.group(s)`,
      virus.type = `virus.type(s)`
    ) |>
    dplyr::mutate(
      ctry = admin.0,
      site = stringr::str_to_title(site.name),
      collect.date = lubridate::dmy(collection.date),
      collect.yr = lubridate::year(collect.date),
      sabin = dplyr::case_when(
        vaccine.1 == "Yes" | vaccine.2 == "Yes" | vaccine.3 == "Yes" ~ 1,
        vaccine.1 == "No" & vaccine.2 == "No" & vaccine.3 == "No" ~ 0
      ),
      vdpv = dplyr::case_when(
        vdpv.1 == "Yes" | vdpv.2 == "Yes" | vdpv.3 == "Yes" ~ 1,
        vdpv.1 == "No" & vdpv.2 == "No" & vdpv.3 == "No" ~ 0
      ),
      wpv = dplyr::case_when(
        wild.1 == "Yes" | wild.3 == "Yes" ~ 1,
        wild.1 == "No" & wild.3 == "No" ~ 0
      ),
      npev = dplyr::case_when(
        npev == "Yes" ~ 1,
        #npev == "No" | is.na(npev) ~ 0
        npev == "No" | npev=="" | is.na(npev) ~ 0
      ),
      nvaccine = dplyr::case_when(
        nvaccine.2 == "Yes"  ~ 1,
        nvaccine.2 == "No"   ~ 0,
      ),
      ev.detect = dplyr::case_when(who.region == "AFRO" & (
        dplyr::if_any( c("vaccine.1", "vaccine.2", "vaccine.3", "nvaccine.2",
                  "vdpv.1", "vdpv.2", "vdpv.3",
                  "wild.1", "wild.3"), ~stringr::str_detect(., "Yes")) |
          npev==1 |
          stringr::str_detect(virus.type, "WILD") |
          stringr::str_detect(virus.type, "VDPV") |
          stringr::str_detect(virus.type, "VACCINE") |
          stringr::str_detect(virus.type, "NPE") |
          stringr::str_detect(final.combined.rtpcr.results, "PV") |
          stringr::str_detect(final.combined.rtpcr.results, "NPE")) ~ 1,
        who.region != "AFRO" & (
          dplyr::if_any(c("vaccine.1", "vaccine.2", "vaccine.3",
                   "nvaccine.2", "vdpv.1", "vdpv.2", "vdpv.3",
                   "wild.1", "wild.3"), ~stringr::str_detect(., "Yes")) |
            (npev==1 & !is.na(npev))) ~ 1,
        TRUE ~ 0),
      ctry.guid = ifelse(is.na(ctry.guid) | ctry.guid == "", NA, paste("{", stringr::str_to_upper(ctry.guid), "}", sep = "")),
      prov.guid = ifelse(is.na(prov.guid) | prov.guid == "", NA, paste("{", stringr::str_to_upper(prov.guid), "}", sep = "")),
      dist.guid = ifelse(is.na(dist.guid) | dist.guid == "", NA, paste("{", stringr::str_to_upper(dist.guid), "}", sep = ""))
    ) |>
    dplyr::mutate_at(c("ctry", "province", "district"), list(~stringr::str_trim(stringr::str_to_upper(.), "both"))) |>
    dplyr::distinct()


  # Make sure 'env.sample.maual.edit.id' is unique for each ENV sample
  es.00 <- es.02[duplicated(es.02$env.sample.manual.edit.id), ]

  # Script below will stop further execution if there is a duplicate ENV sample manual id
  if (nrow(es.00) >= 1) {
    cli::cli_alert_danger("Duplicate ENV sample manual ids. Flag for POLIS. Output in duplicate_ES_sampleID_Polis.csv.")

    tidypolis_io(obj = es.00, io = "write", file_path =  paste0(polis_data_folder, "/Core_Ready_Files/duplicate_ES_sampleID_Polis.csv"))

  } else {
    cli::cli_alert_info("No duplicates identified")
  }


  # find out duplicate ES samples even though they have different 'env.sample.maual.edit.id'
  # from same site, same date, with same virus type

  es.dup.01 <- es.02 |>
    #dplyr::filter(sabin==1) |>
    dplyr::group_by(env.sample.id, virus.type, emergence.group, nt.changes, site.id, collection.date, collect.yr) |>
    dplyr::mutate(es.dups=dplyr::n()) |>
    dplyr::filter(es.dups >=2)|>
    dplyr::select(env.sample.manual.edit.id, env.sample.id, sample.id, site.id, site.code, site.name, sample.condition, collection.date, virus.type,
           nt.changes, emergence.group,ctry, collect.date, collect.yr, es.dups )

  # Script below will stop further execution if there is a duplicate ENV sabin sample from same site, same date, with same virus type
  if (nrow(es.dup.01) >= 1) {
    cli::cli_alert_warning("Duplicate ENV sample. Check the data for duplicate records. If they are the exact same, then contact Ashley")
    cli::cli_alert_warning("Writing out ES duplicates file, please check, continuing processing")
    es.dup.01 <- es.dup.01[order(es.dup.01$env.sample.id,es.dup.01$virus.type, es.dup.01$collect.yr),] |> dplyr::select(-es.dups)

    # Export duplicate viruses in the CSV file:
    tidypolis_io(obj = es.dup.01, io = "write", file_path = paste0(polis_data_folder, "/Core_Ready_Files/duplicate_ES_Polis.csv"))

  } else {
    cli::cli_alert_info("No duplicates identified")
  }

  remove("es.dup.01")

  cli::cli_process_start("Checking for missingness in key ES vars")
  check_missingness(data = es.02, type = "ES")
  cli::cli_process_done("Review missing vars in es_missingness.rds")

  cli::cli_process_start("Cleaning 'virus.type' and creating CDC variables")

  es.space.02 <- es.02 |>
    tidyr::separate_rows(virus.type, sep = ",") |>
    dplyr::mutate(
      virus.type = stringr::str_trim(virus.type, "both"),
      virus.type = ifelse(virus.type == "cVDPV1", "cVDPV 1", virus.type),
      virus.type = ifelse(virus.type == "cVDPV2", "cVDPV 2", virus.type),
      virus.type = ifelse(virus.type == "cVDPV3", "cVDPV 3", virus.type),
      virus.type = ifelse(virus.type == "WILD1", "WILD 1", virus.type),
      virus.type = ifelse(virus.type == "WILD3", "WILD 3", virus.type),
      virus.type = ifelse(virus.type == "VDPV1", "VDPV 1", virus.type),
      virus.type = ifelse(virus.type == "VDPV2", "VDPV 2", virus.type),
      virus.type = ifelse(virus.type == "VDPV3", "VDPV 3", virus.type),
      virus.type = ifelse(virus.type == "VACCINE1", "VACCINE 1", virus.type),
      virus.type = ifelse(virus.type == "VACCINE2", "VACCINE 2", virus.type),
      virus.type = ifelse(virus.type == "VACCINE3", "VACCINE 3", virus.type),
      virus.type = ifelse(virus.type == "aVDPV1", "aVDPV 1", virus.type),
      virus.type = ifelse(virus.type == "aVDPV2", "aVDPV 2", virus.type),
      virus.type = ifelse(virus.type == "aVDPV3", "aVDPV 3", virus.type)
    )


  es.space.03 <- es.space.02 |>
    dplyr::group_by(env.sample.manual.edit.id) |>
    dplyr::summarise(virus.type.01 = paste(virus.type, collapse = ", "))

  es.space.03$virus.type.01[es.space.03$virus.type.01=="NA"] <- NA

  es.02 <- dplyr::right_join(es.space.03, es.02, by= c("env.sample.manual.edit.id"="env.sample.manual.edit.id")) |>
    dplyr::select(-virus.type) |>
    dplyr::rename(virus.type=virus.type.01) |>
    dplyr::mutate(lat = as.numeric(lat),
           lng = as.numeric(lng))



  # Check if na in guid of es country, province or district
  na.es.01 <- es.02 |>
    dplyr::summarise_at(dplyr::vars(ctry.guid, prov.guid, dist.guid, lat, lng), list(~sum(is.na(.))))

  remove("es.space.02", "es.space.03", "es.01.new", "es.02.new")
  gc()
  cli::cli_process_done()
  # attach to shapefile

  # check and make sure there are no new site names:
  envSiteYearList <- get_env_site_data()
  envSiteYearList$site.name <- gsub("\n", " ", envSiteYearList$site.name)
  envSiteYearList$site.name <- toupper(envSiteYearList$site.name)

  es.02$site.name <- gsub("\r\n", " ", es.02$site.name)
  es.02$site.name <- toupper(es.02$site.name)
  es.02$site.name <- stringr::str_trim(es.02$site.name)

  envSiteYearList$site.name <- stringr::str_trim(envSiteYearList$site.name)

  newsites <- setdiff(es.02$site.name, envSiteYearList$site.name)

  # some new sites are just old sites with no lat or long
  truenewsites <- es.02 |>
    dplyr::filter(site.name %in% newsites) |>
    dplyr::filter(is.na(lat)) |>
    dplyr::select(admin.0, site.name, site.id, site.code, lat) |>
    unique()

  if (nrow(truenewsites) > 0) {
    Sys.setenv(POLIS_ENVstatus = "WARNING")
    cli::cli_alert_warning("WARNING, there are site names which can not be found in our database.
            Please run envshapecheck_02.R to make sure each site has a new GUID.")
  }else{
    Sys.setenv(POLIS_ENVstatus = "clear")
  }

  cli::cli_process_start("Creating ES CDC variables")

  #Rename es.02 as es.03 directly, until the above has been re-instated and issues with envshapecheck resolved. Change var names to match es.03 expected output
  es.03 <-  es.02 |>
    dplyr::rename(ADM0_NAME = admin.0,
           ADM1_NAME = admin.1,
           ADM2_NAME = admin.2) |>
    dplyr::mutate(GUID = dist.guid) |>
    dplyr::select(-c("district", "ctry", "province"))


  #fix CIV
  es.03 = es.03|>
    dplyr::mutate(ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"),"COTE D IVOIRE",ADM0_NAME))

  global.ctry.01 <- sirfunctions::load_clean_ctry_sp()

  sf::sf_use_s2(F)
  shape.name.01 <- global.ctry.01 |>
    dplyr::select(ISO_3_CODE, ADM0_NAME.rep = ADM0_NAME) |>
    dplyr::distinct(.keep_all = T)
  sf::sf_use_s2(T)

  savescipen <- getOption("scipen")
  options(scipen = 999)

  es.04 <- dplyr::left_join(es.03, shape.name.01, by = c("country.iso3" = "ISO_3_CODE"), relationship = "many-to-many") |>
    dplyr::mutate(ADM0_NAME = ifelse(is.na(ADM0_NAME), ADM0_NAME.rep, ADM0_NAME)) |>
    dplyr::select(-c("ADM0_NAME.rep", "Shape")) |>
    dplyr::mutate(lat = as.character(lat),
           lng = as.character(lng)) |>
    dplyr::mutate_at(c("admin.2.id", "region.id", "reporting.week", "reporting.year",
                "env.sample.manual.edit.id", "site.id", "sample.id", "admin.0.id", "admin.1.id"), ~as.numeric(.)) |>
    dplyr::distinct()

  es.05 <- remove_character_dates(type = "ES", df = es.04)

  options(scipen = savescipen)

  cli::cli_process_done()

  cli::cli_process_start("Checking metadata with previous data")

  # save data
  #Compare the final file to last week's final file to identify any differences in var_names, var_classes, or categorical responses
  old.es.file <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files/Archive", latest_folder_in_archive), full_names = T)

  old.es.file <- old.es.file[grepl("es_2001-01-08", old.es.file)]

  if(length(old.es.file) > 0){

    old.es <- tidypolis_io(io = "read", file_path = old.es.file)

    new_table_metadata <- f.summarise.metadata(es.05)
    old_table_metadata <- f.summarise.metadata(old.es)
    es_metadata_comparison <- f.compare.metadata(new_table_metadata, old_table_metadata, "ES")

    #compare obs
    new <- es.05 |>
      dplyr::mutate(env.sample.manual.edit.id = stringr::str_squish(env.sample.manual.edit.id)) |>
      dplyr::mutate_all(as.character)


    old <- old.es |>
      dplyr::mutate(env.sample.manual.edit.id = stringr::str_squish(env.sample.manual.edit.id)) |>
      dplyr::mutate_all(as.character)

    in_new_not_old <- new |>
      dplyr::filter(!(env.sample.manual.edit.id %in% old$env.sample.manual.edit.id))

    in_old_not_new <- old |>
      dplyr::filter(!(env.sample.manual.edit.id %in% new$env.sample.manual.edit.id))

    in_new_and_old_but_modified <- new |>
      dplyr::filter(env.sample.manual.edit.id %in% old$env.sample.manual.edit.id) |>
      dplyr::select(-c(setdiff(colnames(new), colnames(old)))) |>
      setdiff(old |>
                dplyr::select(-c(setdiff(colnames(old), colnames(new))))) |>
      dplyr::inner_join(old |>
                          dplyr::filter(env.sample.manual.edit.id %in% new$env.sample.manual.edit.id) |>
                          dplyr::select(-c(setdiff(colnames(old), colnames(new)))) |>
                          setdiff(new |>
                                    dplyr::select(-c(setdiff(colnames(new), colnames(old))))), by="env.sample.manual.edit.id") |>
      #wide_to_long
      tidyr::pivot_longer(cols=-env.sample.manual.edit.id) |>
      dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
      dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
      #long_to_wide
      tidyr::pivot_wider(names_from=source, values_from=value)

    if(nrow(in_new_and_old_but_modified) > 0){
      in_new_and_old_but_modified <- in_new_and_old_but_modified |>
        dplyr::mutate(new = as.character(new),
                      old = as.character(old)) |>
        dplyr::filter(new != old)
    }

    update_polis_log(.event = paste0("ES New Records: ", nrow(in_new_not_old), "; ",
                                     "ES Removed Records: ", nrow(in_old_not_new), "; ",
                                     "ES Modified Records: ", length(unique(in_new_and_old_but_modified$env.sample.manual.edit.id))),
                     .event_type = "INFO")


    cli::cli_process_done()
  }else{
    cli::cli_process_done()
    cli::cli_alert_info("No old ES file found")
  }


  cli::cli_process_start("Writing out ES datasets")
  tidypolis_io(obj = es.05, io = "write", file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                         paste("es", min(es.04$collect.date, na.rm = T), max(es.04$collect.date, na.rm = T), sep = "_"),
                         ".rds",
                         sep = ""
  ))

  cli::cli_process_done()

  update_polis_log(.event = paste0("ES finished"),
                   .event_type = "PROCESS")

  # remove unneeded data from workspace

  cli::cli_process_start("Clearing memory")

  rm(
    'envSiteYearList', 'es.00', 'es.02', 'es.03', 'es.04', 'es.05', 'es_metadata_comparison', 'global.ctry.01',
    'in_new_and_old_but_modified', 'in_new_not_old', 'in_old_not_new',
    'na.es.01', 'new', 'new.df', 'new.file', 'new.var.es.01', 'new_table_metadata', 'newsites',
    'old', 'old.es.file', 'old.file', 'old_table_metadata', 'savescipen',
    'shape.name.01', 'truenewsites', 'var.list.01', 'sia.new.value'
  )
  gc()
  cli::cli_process_done()

  #Step 5 - Creating Virus datasets ====
  update_polis_log(.event = "Creating Positives analytic datasets",
                   .event_type = "PROCESS")

  cli::cli_h1("Step 5/5 - Creating Virus datasets")

  cli::cli_process_start("Loading new and old virus data")
  endyr <- year(format(Sys.time()))
  startyr <- 2000


  # Step 1: Read in "old" data file (System to find "Old" data file)
  old.file <- x[grepl("Viruses_",x)]

  new.file <- y[grepl("Viruses_", y)]

  # Step 1: Read in VIRUS table data from POLIS
  virus.raw.new <- tidypolis_io(io = "read", file_path = new.file) |>
    dplyr::mutate_all(as.character) |>
    dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
    dplyr::mutate_all(list(~dplyr::na_if(.,"")))

  names(virus.raw.new) <- stringr::str_to_lower(names(virus.raw.new))

  if(length(old.file) > 0){
    # Step 2: Read in "old" data file
    virus.raw.old <- tidypolis_io(io = "read", file_path =old.file) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~dplyr::na_if(.,"")))


    names(virus.raw.old) <- stringr::str_to_lower(names(virus.raw.old))

    cli::cli_process_done()
    # variables in old dataframe

    var.names <- virus.raw.old |>
      purrr::map_df(~ (data.frame(class = class(.x))),
                    .id = "variable"
      )

    var.names.01 <- var.names |>
      dplyr::filter(variable != "virus.type(s)" & variable != "vdpv.classification(s)" & variable != "nt.changes" & variable != "emergence.group(s)" &
                      variable != "virus.cluster(s)" & variable != "surveillance.type" &
                      !(variable %in% c("exact.longitude", "exact.latitude", "pons.latitude", "pons.longitude", "pons.environment",
                                        "pons.seq.date", "pons.administration.type", "pons.spec.type", "location", "country.iso2"))) # list of variables we want evaluated in 2nd QC function


    var.list.01 <- as.character(var.names.01$variable)

    virus.raw.old.comp <- virus.raw.old |>
      dplyr::select(-c(dplyr::all_of(var.list.01)))

    virus.raw.new.comp <- virus.raw.new |>
      dplyr::select(-c(dplyr::all_of(var.list.01)))


    # Step 3: Apply compare dataframe function

    # Are there differences in the names of the columns between two downloads?
    f.compare.dataframe.cols(virus.raw.old.comp, virus.raw.new.comp)

    new.var.virus.01 <- f.download.compare.01(virus.raw.old.comp, virus.raw.new.comp)

    new.df <- new.var.virus.01 |>
      filter(is.na(old.distinct.01) | diff.distinct.01 >= 1)

    if (nrow(new.df) >= 1) {
      cli::cli_alert_danger("There is either a new variable in the AFP data or new value of an existing variable.
       Please run f.download.compare.02 to see what it is.")

      # Step 4: Apply compare variables function

      virus.new.value <- f.download.compare.02(new.var.virus.01, virus.raw.old.comp, virus.raw.new.comp)

      update_polis_log(.event = sapply(names(virus.new.value), function(x) paste0("New Values in: ", x, " - ", paste0(unique(dplyr::pull(virus.new.value, x)), collapse = ", "))) |>
                         paste0(collapse = "; "),
                       .event_type = "ALERT")

    } else {
      cli::cli_alert_info("New AFP download is comparable to old AFP download")
    }
  } else{
    cli::cli_process_done()

    cli::cli_alert_info("No previous Virus table identified")
  }



  # Step 5: check virus types and virus type names to ensure that novel derived viruses are properly
  # accounted for

  virus.types.names <- virus.raw.new |>
    dplyr::count(`virus.type(s)`, virustypename)

  # Step 6: fix all dates from character to ymd format and fix character variables

  cli::cli_process_start("Creating CDC variables")

  #read in list of novel emergences supplied by ORPG
  nopv.emrg <- sirfunctions::edav_io(io = "read", file_loc = "GID/PEB/SIR/Data/orpg/nopv_emg.table.rds", default_dir = NULL) |>
    dplyr::rename(emergencegroup = emergence_group,
                  vaccine.source = vaccine_source) |>
    dplyr::mutate(vaccine.source = dplyr::if_else(vaccine.source == "novel", "Novel", vaccine.source))


  virus.01 <- virus.raw.new |>
    dplyr::rename(virus.type = `virus.type(s)`,
           viruscluster = `virus.cluster(s)`,
           emergencegroup = `emergence.group(s)`,
           ntchanges = nt.changes,
           classificationvdpv =`vdpv.classification(s)`,
           whoregion=who.region
    ) |>
    dplyr::mutate(dateonset = lubridate::ymd(virus.date),
                  vdpvclassificationchangedate = lubridate::parse_date_time(vdpvclassificationchangedate, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),
           datasource='virustable',
           yronset= lubridate::year(dateonset))|>
    dplyr::mutate(
      surveillance.type = ifelse(surveillance.type == "Environmental", "ENV", surveillance.type),
      virus.type = stringr::str_trim(virus.type, "both"),
      virustypename = stringr::str_trim(virustypename, "both"),

      vaccine.source = ifelse(virus.type %in% c("VACCINE1", "VACCINE2", "VACCINE3", "VDPV1",
                                                "VDPV2", "VDPV3"), "Sabin", NA),
      vaccine.source = ifelse(virus.type %in% c("nVACCINE2")|emergencegroup %in% nopv.emrg$emergencegroup, "Novel", vaccine.source),

      virustypename = ifelse(virustypename == "aVDPV1", "aVDPV 1", virustypename),
      virustypename = ifelse(virustypename == "aVDPV2", "aVDPV 2", virustypename),
      virustypename = ifelse(virustypename == "aVDPV3", "aVDPV 3", virustypename),
      virustypename = ifelse(virustypename == "cVDPV1", "cVDPV 1", virustypename),
      virustypename = ifelse(virustypename == "cVDPV2", "cVDPV 2", virustypename),
      virustypename = ifelse(virustypename == "cVDPV3", "cVDPV 3", virustypename),
      virustypename = ifelse(virustypename == "iVDPV1", "iVDPV 1", virustypename),
      virustypename = ifelse(virustypename == "iVDPV2", "iVDPV 2", virustypename),
      virustypename = ifelse(virustypename == "iVDPV3", "iVDPV 3", virustypename),
      virustypename = ifelse(virustypename == "VACCINE1", "VACCINE 1", virustypename),
      virustypename = ifelse(virustypename == "VACCINE2", "VACCINE 2", virustypename),
      virustypename = ifelse(virustypename == "VACCINE3", "VACCINE 3", virustypename),
      virustypename = ifelse(virustypename == "VDPV1", "VDPV 1", virustypename),
      virustypename = ifelse(virustypename == "VDPV2", "VDPV 2", virustypename),
      virustypename = ifelse(virustypename == "VDPV3", "VDPV 3", virustypename),
      virustypename = ifelse(virustypename == "WILD1", "WILD 1", virustypename),
      virustypename = ifelse(virustypename == "WILD3", "WILD 3", virustypename),
      virustypename = ifelse(virustypename == "VACCINE2-n", "VACCINE 2-n", virustypename),

      virus.type = ifelse(virus.type == "nVACCINE2", "VACCINE2-n", virus.type),
      virus.type = ifelse(virus.type == "VACCINE1", "VACCINE 1", virus.type),
      virus.type = ifelse(virus.type == "VACCINE2", "VACCINE 2", virus.type),
      virus.type = ifelse(virus.type == "VACCINE3", "VACCINE 3", virus.type),
      virus.type = ifelse(virus.type == "VDPV1", "VDPV 1", virus.type),
      virus.type = ifelse(virus.type == "VDPV2", "VDPV 2", virus.type),
      virus.type = ifelse(virus.type == "VDPV3", "VDPV 3", virus.type),
      virus.type = ifelse(virus.type == "WILD1", "WILD 1", virus.type),
      virus.type = ifelse(virus.type == "WILD3", "WILD 3", virus.type),

      classificationvdpv = ifelse(is.na(classificationvdpv), "", classificationvdpv),

      adm0guid = dplyr::case_when(
        !is.na(admin.0.guid) ~ paste0("{", toupper(admin.0.guid), "}"),
        TRUE ~ as.character(admin.0.guid)
      ),
      adm1guid = dplyr::case_when(
        !is.na(admin.1.guid) ~ paste0("{", toupper(admin.1.guid), "}"),
        TRUE ~ as.character(admin.1.guid),
      ),
      adm2guid = dplyr::case_when(
        !is.na(admin.2.guid) ~ paste0("{", toupper(admin.2.guid), "}"),
        TRUE ~ as.character(admin.2.guid)
      )
    ) |>
    dplyr::rename(virustype=virus.type) |>
    dplyr::select(-admin.0.guid, -admin.1.guid, -admin.2.guid, -virus.date) |>
    #separate(location, c("place.admin.0", "place.admin.1", "place.admin.2"), ",") |>
    #fix CIV
    dplyr::mutate(place.admin.0 = ifelse(stringr::str_detect(place.admin.0, "IVOIRE"),"COTE D IVOIRE", place.admin.0))

  rm(nopv.emrg)

  cli::cli_process_done()
  #flag to identify vaccine with nt.changes >= 6
  vaccine.6.plus <- virus.01 |>
    dplyr::filter(virustypename %in% c("VACCINE 1", "VACCINE 2", "VACCINE 3", "VACCINE 2-n") &
             ntchanges >= 6) |>
    dplyr::select(epid, surveillance.type, virustype, virustypename, ntchanges, classificationvdpv, vaccine.source)

  if (nrow(vaccine.6.plus) >= 1) {
    cli::cli_alert_warning("There is a potentially misclassified virus based on ntchanges, check in POLIS and flag on message board. Writing out viruses to check.")
    tidypolis_io(obj = vaccine.6.plus, io = "write", file_path = paste0(polis_data_folder, "/Core_Ready_Files/virus_large_nt_changes.csv"))

    update_polis_log(.event = paste0("Vaccine viruses with 6+ NT changes, flag for POLIS"),
                     .event_type = "ALERT")

    } else {
    cli::cli_alert_info("Virus classifications are correct")
  }

  # Step 7: Check for duplicate viruses

  virus.dup.01 <- virus.01 |>
    dplyr::select(epid, epid.in.polis, pons.epid, virus.id, polis.case.id, env.sample.id, place.admin.0, surveillance.type, datasource, virustype, dateonset, yronset, ntchanges, emergencegroup) |>
    dplyr::group_by(epid, virustype, dateonset, yronset, ntchanges, emergencegroup) |>
    dplyr::mutate(virus_dup=dplyr::n()) |>
    dplyr::filter(virus_dup>=2)

  # Script below will stop further execution if there is a duplicate virus, with same onset date, virus type, emergence group, ntchanges
  if (nrow(virus.dup.01) >= 1) {
    cli::cli_alert_warning("Duplicate viruses in the virus table data. Check the data for duplicate records.
          If they are the exact same, then contact Ashley")
    virus.dup.01 <- virus.dup.01[order(virus.dup.01$surveillance.type,virus.dup.01$virustype, virus.dup.01$yronset),] |> select(-virus_dup)

    tidypolis_io(obj = virus.dup.01, io = "write", file_path = paste0(polis_data_folder, "/Core_Ready_Files/duplicate_viruses_Polis_virusTableData.csv"))

    update_polis_log(.event = paste0("Duplicate viruses available in duplicate_viruses_Polis_virusTableData.csv"),
                     .event_type = "ALERT")

  } else {
    cli::cli_alert_info("If no duplicates found, then proceed")
  }

  # Human virus dataset with sabin 2 and positive viruses only
  human.virus.01 <- virus.01 |>
    dplyr::filter(!surveillance.type=="ENV") |>
    dplyr::filter(!virustype %in% c("VACCINE 1", "VACCINE 3"))

  cli::cli_process_start("Clearing memory")
  rm(
    "new.df", "new.var.virus.01", "vaccine.6.plus", "var.names", "var.names.01",
    "virus.dup.01", "virus.raw.new", "virus.raw.new.comp", "virus.raw.old",
    "virus.raw.old.comp", "virus.types.names", "new.file", "old.file", "var.list.01", "x", "y"
  )
  gc()
  cli::cli_process_done()

  cli::cli_process_start("Processing and cleaning AFP/non-AFP files")
  afp.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"), full_names = T)) |>
    dplyr::mutate(short_name = stringr::str_replace(name, paste0(polis_data_folder, "/Core_Ready_Files/"), "")) |>
    dplyr::filter(grepl("^(afp_linelist_2001-01-01_2024).*(.rds)$", short_name)) |>
    dplyr::pull(name)
  afp.01 <- lapply(afp.files.01, function(x) tidypolis_io(io = "read", file_path = x)) |>
    dplyr::bind_rows() |>
    dplyr::ungroup() |>
    dplyr::distinct(.keep_all = T)|>
    dplyr::filter(dplyr::between(yronset, startyr, endyr))

  non.afp.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"), full_names = T)) |>
    dplyr::mutate(short_name = stringr::str_replace(name, paste0(polis_data_folder, "/Core_Ready_Files/"), "")) |>
    dplyr::filter(grepl("^(other_surveillance_type_linelist_2016_2024).*(.rds)$", short_name)) |>
    dplyr::pull(name)
  non.afp.01 <- purrr::map_df(non.afp.files.01, ~ tidypolis_io(io = "read", file_path = .x)) |>
    dplyr::ungroup() |>
    dplyr::distinct(.keep_all = T) |>
    dplyr::filter(dplyr::between(yronset, startyr, endyr))

  # deleting any duplicate records in afp.01.
  afp.01 <- afp.01[!duplicated(afp.01$epid), ] |>
    dplyr::select(epid, dateonset,  place.admin.0, place.admin.1, place.admin.2, admin0guid, yronset, admin1guid, admin2guid, cdc.classification.all,
           whoregion, nt.changes, emergence.group, virus.cluster, surveillancetypename, lat, lon, vtype.fixed, datenotificationtohq)

  # deleting any duplicate records in non.afp.files.01.
  non.afp.01 <- non.afp.01[!duplicated(non.afp.01$epid), ] |>
    dplyr::select(epid, dateonset,  place.admin.0, place.admin.1, place.admin.2, admin0guid, yronset, admin1guid, admin2guid, cdc.classification.all,
           whoregion, nt.changes, emergence.group, virus.cluster, surveillancetypename, lat, lon, vtype.fixed, datenotificationtohq)


  #Combine AFP and other surveillance type cases
  afp.02 <- rbind(afp.01, non.afp.01) |>
    dplyr::select(epid, lat, lon, datenotificationtohq) |>
    dplyr::mutate(datenotificationtohq = parse_date_time(datenotificationtohq, c("%Y-%m-%d", "%d/%m/%Y")))


  # Get geo cordinates from AFP linelist.
  human.virus.02 <- dplyr::right_join(afp.02, human.virus.01, by=c("epid"="epid"))|>
    dplyr::mutate_at(c("lat", "lon", "x", "y"), ~as.numeric(.)) |>
    dplyr::distinct() |>
    dplyr::mutate(latitude = ifelse(is.na(lat), y, lat),
           longitude = ifelse(is.na(lon), x, lon)) |>
    dplyr::select(-x, -y, -lat, -lon) |>
    dplyr::mutate(congo_wild1 = dplyr::case_when((place.admin.0 == "CONGO" & yronset == '2010' & virustypename == "WILD 1")~ 1))


  # Remove CONGO 2010 WILD 1 cases from VIRUS table
  human.virus.03 <- human.virus.02 |>
    dplyr::filter(is.na(congo_wild1)) |>
    dplyr::select(-congo_wild1)


  # Create dataset of AFP CONGO Wild 1 (441)cases from 2010 from AFP linelist
  congo.afp.wild1 <- afp.01 |>
    dplyr::filter(place.admin.0=="CONGO" & yronset =='2010' & vtype.fixed == "WILD 1") |>
    dplyr::mutate(virustypename = "WILD 1",
           latitude= as.numeric(lat),
           longitude= as.numeric(lon),
           adm0guid = dplyr::case_when(
             !is.na(admin0guid) ~ paste0("{", toupper(admin0guid), "}"),
             TRUE ~ as.character(admin0guid)
           ),
           adm1guid = dplyr::case_when(
             !is.na(admin1guid) ~ paste0("{", toupper(admin1guid), "}"),
             TRUE ~ as.character(admin1guid),
           ),
           adm2guid = dplyr::case_when(
             !is.na(admin2guid) ~ paste0("{", toupper(admin2guid), "}"),
             TRUE ~ as.character(admin2guid)
           ),
           datasource="afp_linelist"
    ) |>
    dplyr::select(epid, dateonset,  place.admin.0, place.admin.1, place.admin.2, adm0guid, yronset, adm1guid, adm2guid, cdc.classification.all,
           whoregion, nt.changes, emergence.group, virus.cluster, surveillancetypename, latitude, longitude, datasource, virustypename) |>
    dplyr::rename(virustype=cdc.classification.all, surveillance.type = surveillancetypename, viruscluster = virus.cluster,
           emergencegroup = emergence.group, ntchanges = nt.changes)


  # Adding Congo 2010 WILD 1 (441)cases in VIRUS table.
  human.virus.04 <- dplyr::bind_rows(congo.afp.wild1, human.virus.03)

  ### ENV data from virus table
  env.virus.01 <- virus.01 |>
    dplyr::filter(surveillance.type == "ENV" & !is.na(virustype) &
             !virustype %in% c("VACCINE 1", "VACCINE 3", "NPEV"))

  cli::cli_process_done()
  gc()
  cli::cli_process_start("Adding in ES data")

  # read in ES files from cleaned ENV linelist
  env.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"), full_names = T)) |>
    dplyr::mutate(short_name = stringr::str_replace(name, paste0(polis_data_folder, "/Core_Ready_Files/"), "")) |>
    dplyr::filter(grepl("^(es).*(.rds)$", short_name)) |>
    dplyr::pull(name)
  es.01 <- purrr::map_df(env.files.01, ~ tidypolis_io(io = "read", file_path = .x)) |>
    dplyr::ungroup() |>
    dplyr::distinct(.keep_all = T)

  # Make sure 'env.sample.maual.edit.id' is unique for each ENV sample
  es.00 <- es.01[duplicated(es.01$env.sample.manual.edit.id), ]


  es.02 <- es.01 |>
    dplyr::rename(
      place.admin.0 = ADM0_NAME,
      place.admin.1 = ADM1_NAME,
      place.admin.2 = ADM2_NAME,
      adm2guid = GUID,
      adm1guid = prov.guid,
      adm0guid = ctry.guid,
      dateonset = collection.date,
      yronset = collect.yr,
      whoregion = region.name
    ) |>
    dplyr::select(virus.type, dplyr::everything()) |>
    dplyr::mutate(
      dateonset = lubridate::parse_date_time(dateonset, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),
      datenotificationtohq = parse_date_time(date.notification.to.hq,
                                             c("%Y-%m-%d", "%d/%m/%Y")),
      virustype = stringr::str_replace_all(virus.type, "  ", " "),
      datasource = "es_linelist",
      lat = as.numeric(lat),
      lon = as.numeric(lng)
    ) |>
    tidyr::separate_rows(virus.type, sep = ", ")


  es.03 <- es.02 |>
    dplyr::select(env.sample.id, site.id, lat, lon, datenotificationtohq) |>
    dplyr::rename(epid = env.sample.id)


  # get goe coordinates from ES linelist.
  env.virus.02 <- dplyr::left_join(env.virus.01, es.03, by=c("epid"="epid"), relationship = "many-to-many")

  cli::cli_process_done()

  cli::cli_process_start("Removing duplicates from virus table")

  # remove duplicates in virus table
  env.virus.02 <- env.virus.02[!duplicated(env.virus.02$virus.id), ]

  env.virus.03 <- env.virus.02 |>
    dplyr::mutate_at(c("lat", "lon", "x", "y"), ~as.numeric(.)) |>
    dplyr::mutate(latitude = ifelse(is.na(lat), y, lat),
           longitude = ifelse(is.na(lon), x, lon)) |>
    dplyr::select(-x, -y, -lat, -lon)

  env.virus.04 <- env.virus.03 |>
    dplyr::mutate(place.admin.0 =  stringi::stri_trim(place.admin.0, "left"),
           place.admin.1 =  stringi::stri_trim(place.admin.1, "left"),
           place.admin.2 =  stringi::stri_trim(place.admin.2, "left"))

  human.virus.05 <- human.virus.04 |>
    dplyr::mutate(place.admin.0 =  stringi::stri_trim(place.admin.0, "left"),
           place.admin.1 =  stringi::stri_trim(place.admin.1, "left"),
           place.admin.2 =  stringi::stri_trim(place.admin.2, "left"))

  cli::cli_process_done()
  cli::cli_process_start("Creating final virus file")

  # Final virus file with AFP and ES
  afp.es.virus.01 <- dplyr::bind_rows(human.virus.05, env.virus.04) |>
    dplyr::rename(source = surveillance.type,
           measurement = virustypename,
           admin2guid = adm2guid) |>
    dplyr::mutate(cdc.classification.all = 'cdc.classification.all') |>
    #fix CIV
    dplyr::mutate(place.admin.0 = ifelse(stringr::str_detect(place.admin.0, "IVOIRE"),"COTE D IVOIRE", place.admin.0)) |>
    dplyr::mutate(latitude = as.character(latitude),
           longitude = as.character(longitude),
           env.sample.id = as.numeric(env.sample.id),
           polis.case.id = as.numeric(polis.case.id),
           vdpvclassificationchangedate = parse_date_time(vdpvclassificationchangedate, c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),
           report_date = case_when(
             measurement %in% c("cVDPV 1", "cVDPV 2", "cVDPV 3", "VDPV 1", "VDPV 2", "VDPV 3") ~ vdpvclassificationchangedate,
             measurement == "WILD 1" ~ datenotificationtohq))

  afp.es.virus.02 <- remove_character_dates(type = "POS", df = afp.es.virus.01)

  cli::cli_process_done()

  cli::cli_process_start("Checking for variables that don't match last weeks pull")

  #Compare the final file to last week's final file to identify any differences in var_names, var_classes, or categorical responses
  new_table_metadata <- f.summarise.metadata(afp.es.virus.02)

  x <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files/Archive", latest_folder_in_archive), full_names = T)

  y <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, "Core_Ready_Files"), full_names = T)

  old.file <- x[grepl("positives_2001-01-01", x)]

  if(length(old.file) > 0){

    old.es <- tidypolis_io(io = "read", file_path = old.file)

    old_table_metadata <- f.summarise.metadata(old.es)
    positives_metadata_comparison <- f.compare.metadata(new_table_metadata, old_table_metadata, "POS")

    new <- afp.es.virus.02 |>
      unique() |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::group_by(epid)|>
      dplyr::slice(1) |>
      dplyr::ungroup() |>
      dplyr::mutate_all(as.character)

    old <- old.es |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::group_by(epid)|>
      dplyr::slice(1) |>
      dplyr::ungroup() |>
      dplyr::mutate_all(as.character)


    in_new_not_old <- new |>
      dplyr::filter(!(epid %in% old$epid))

    in_old_not_new <- old |>
      dplyr::filter(!(epid %in% new$epid))

    in_new_and_old_but_modified <- new |>
      dplyr::filter(epid %in% old$epid) |>
      dplyr::select(-c(setdiff(colnames(new), colnames(old)))) |>
      setdiff(old |>
                dplyr::select(-c(setdiff(colnames(old), colnames(new))))) |>
      dplyr::inner_join(old |>
                          dplyr::filter(epid %in% new$epid) |>
                          dplyr::select(-c(setdiff(colnames(old), colnames(new)))) |>
                          dplyr::setdiff(new |>
                                           select(-c(setdiff(colnames(new), colnames(old))))), by="epid") |>
      #wide_to_long
      tidyr::pivot_longer(cols=-epid) |>
      dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
      dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
      #long_to_wide
      tidyr::pivot_wider(names_from=source, values_from=value)

    if(nrow(in_new_and_old_but_modified) > 0){
      in_new_and_old_but_modified <- in_new_and_old_but_modified |>
        dplyr::mutate(new = as.character(new)) |>
        dplyr::mutate(old = as.character(old)) |>
        dplyr::filter(new != old & !name %in% c("latitude", "longitude"))

      # list of records for which virus type name has changed from last week to this week.
      pos_changed_virustype <- in_new_and_old_but_modified |> filter(name=="measurement")

      if(nrow(pos_changed_virustype) > 0){

        update_polis_log(.event = paste0("Virus type has changed for ", nrow(pos_changed_virustype), " records, review in Changed_virustype_virusTableData.csv"),
                         .event_type = "ALERT")

      }

      # Export records for which virus type has changed from last week to this week in the CSV file:
      tidypolis_io(obj = pos_changed_virustype,
                   io = "write",
                   file_path = paste0(polis_data_folder, "/Core_Ready_Files/Changed_virustype_virusTableData.csv"))

    }

    # list of records in new but not in old.
    in_new_not_old <- in_new_not_old |> select(place.admin.0, epid, dateonset, yronset, source, virustype)

    if(nrow(in_new_not_old) > 0 ){
      # Export records for which virus type has changed from last week to this week in the CSV file:
      tidypolis_io(obj = in_new_not_old,
                   io = "write",
                   file_path = paste0(polis_data_folder, "/Core_Ready_Files/in_new_not_old_virusTableData.csv"))
    }

  }else{
    cli::cli_process_done()
    cli::cli_alert_info("No previous archive identified")
    in_new_not_old <- tibble()
    in_old_not_new <- tibble()
    in_new_and_old_but_modified <- list()
  }



  #identify updated viruses logging change from VDPV to cVDPV
  class.updated <- afp.es.virus.01 |>
    dplyr::filter(vdpvclassificationchangedate <= Sys.Date() & vdpvclassificationchangedate > (Sys.Date()- 7)) |>
    dplyr::select(epid, virustype, measurement, vdpvclassificationchangedate, vdpvclassificationcode, createddate) |>
    dplyr::mutate(vdpvclassificationchangedate = as.Date(vdpvclassificationchangedate, "%Y-%m-%d"))

  if(nrow(class.updated > 0)){
    tidypolis_io(obj = class.updated,
                 io = "write",
                 file_path = paste0(polis_data_folder, "/Core_Ready_Files/virus_class_changed_date.csv"))
  }

  log.in.new.and.old.mod <- ifelse(
    is.list(in_new_and_old_but_modified),
    NA,
    length(unique(in_new_and_old_but_modified$epid))
  )


  update_polis_log(.event = paste0("POS New Records: ", nrow(in_new_not_old), "; ",
                                   "POS Removed Records: ", nrow(in_old_not_new), "; ",
                                   "POS Modified Records: ", log.in.new.and.old.mod, "; ",
                                   "POS Class Changed Records: ", length(unique(class.updated$epid))),
                   .event_type = "INFO")

  tidypolis_io(obj = afp.es.virus.02,
               io = "write",
               file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                   paste("positives", min(afp.es.virus.01$dateonset, na.rm = T),
                                         max(afp.es.virus.01$dateonset, na.rm = T),
                                         sep = "_"
                                   ),
                                   ".rds",
                                   sep = ""
  ))


  cli::cli_process_done()

  cli::cli_process_start("Checking for positives that don't match to GUIDs")

  # AFP and ES that do not match to shape file
  unmatched.afp.es.viruses.01 <- dplyr::anti_join(afp.es.virus.02, long.global.dist.01, by = c("admin2guid" = "GUID", "yronset" = "active.year.01"))

  # CSV file listing out unmatch virus

  tidypolis_io(obj = unmatched.afp.es.viruses.01,
               io = "write",
               file_path = paste(polis_data_folder, "/Core_Ready_Files/",
                                               paste("unmatch_positives", min(unmatched.afp.es.viruses.01$yronset, na.rm = T),
                                                     max(unmatched.afp.es.viruses.01$yronset, na.rm = T),
                                                     sep = "_"
                                               ),
                                               ".csv",
                                               sep = ""
  ))

  cli::cli_process_done()

  update_polis_log(.event = "Positives file finished",
                   .event_type = "PROCESS")

  cli::cli_process_start("Clearing memory")
  rm(list = ls())
  gc()
  cli::cli_process_done()

  update_polis_log(.event = "Processing of CORE datafiles complete",
                   .event_type = "END")


  #log_report()
  #archive_log()

}

#Began work on pop processing pipeline but not ready for V1

#' #' Preprocess population data into flat files
#' #'
#' #' @description Process POLIS population data using CDC and other standards
#' #' @import readr dplyr
#' #' @param type str: "cdc" or "who" (default)
#' #' @param pop_file tibble: WHO POLIS population file, defaults to tidypolis folder
#' #' @return list with tibble for ctry, prov and dist
#' process_pop <- function(type = "who", pop_file = readr::read_rds(file.path(Sys.getenv("POLIS_DATA_FOLDER"), "data", "pop.rds"))){
#'
#'   #subset to <= 15
#'   pop_file <- pop_file |>
#'     filter(AgeGroupName == "0 to 15 years")
#'
#'   #extract into country prov and dist
#'
#'   x <- lapply(unique(pop_file$Admin0Name), function(x){
#'     pop_file |>
#'       filter(is.na(Admin1Name) & is.na(Admin2Name)) |>
#'       rename(year = Year, u15pop = Value, GUID = Admin0GUID, ctry = Admin0Name) |>
#'       mutate(u15pop = as.integer(u15pop)) |>
#'       arrange(year) |>
#'       filter(ctry == x) |>
#'       group_by(year) |>
#'       filter(!is.na(u15pop)) |>
#'       filter(UpdatedDate == max(UpdatedDate, na.rm = T)) |>
#'       ungroup() |>
#'       select(ctry, year, u15pop, GUID) |>
#'       full_join(tibble(ctry = x, year = 2000:(lubridate::year(Sys.time()))), by = c("ctry", "year"))
#'   }) |>
#'     bind_rows()
#'
#' }
#'

#' @description
#' a function to process WHO spatial datasets
#' @import dplyr sf lubridate stringr readr tibble utils
#' @param gdb_folder str the folder location of spatial datasets, should end with .gdb, if on edav the gdb will need to be zipped,
#' ensure that the gdb and the zipped file name are the same
#' @param output_folder str folder location to write outputs to
#' @param edav boolean T or F, whether gdb is on EDAV or local
process_spatial <- function(gdb_folder,
                            output_folder,
                            edav) {
  if(edav) {
    output_folder <- stringr::str_replace(output_folder, paste0("GID/PEB/SIR/"), "")
  }

  if(edav) {
    azcontainer = suppressMessages(get_azure_storage_connection())
    dest <- tempdir()
    AzureStor::storage_download(container = azcontainer, gdb_folder, paste0(dest, "/gdb.zip"), overwrite = T)

    utils::unzip(zipfile = paste0(dest, "/gdb.zip"), exdir = dest)

    # Country shapes EDAV===============
    global.ctry.01 <- sf::st_read(dsn = stringr::str_remove(paste0(dest, "/", sub(".*\\/", "", gdb_folder)), ".zip"),
                                  layer = "GLOBAL_ADM0") |>
      dplyr::mutate(STARTDATE = as.Date(STARTDATE),
                    ENDDATE = as.Date(ENDDATE),
                    yr.st = lubridate::year(STARTDATE),
                    yr.end = lubridate::year(ENDDATE),
                    ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"), "COTE D IVOIRE", ADM0_NAME))

    # Province shapes EDAV===============
    global.prov.01 <- sf::st_read(dsn = stringr::str_remove(paste0(dest, "/", sub(".*\\/", "", gdb_folder)), ".zip"),
                                  layer = "GLOBAL_ADM1") |>
      dplyr::mutate(STARTDATE = as.Date(STARTDATE),
                    ENDDATE = as.Date(ENDDATE),
                    yr.st = lubridate::year(STARTDATE),
                    yr.end = lubridate::year(ENDDATE),
                    ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"), "COTE D IVOIRE", ADM0_NAME)
      )

    # District shapes EDAV===============
    global.dist.01 <- sf::st_read(dsn = stringr::str_remove(paste0(dest, "/", sub(".*\\/", "", gdb_folder)), ".zip"),
                                  layer = "GLOBAL_ADM2") |>
      dplyr::mutate(STARTDATE = as.Date(STARTDATE),
                    ENDDATE = as.Date(ENDDATE),
                    yr.st = lubridate::year(STARTDATE),
                    yr.end = lubridate::year(ENDDATE),
                    ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"), "COTE D IVOIRE", ADM0_NAME))

    unlink(dest)
  } else {

    # Country shapes local===============
    global.ctry.01 <- sf::st_read(dsn = gdb_folder, layer = "GLOBAL_ADM0") |>
      dplyr::mutate(STARTDATE = as.Date(STARTDATE),
                    ENDDATE = as.Date(ENDDATE),
                    yr.st = lubridate::year(STARTDATE),
                    yr.end = lubridate::year(ENDDATE),
                    ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"), "COTE D IVOIRE", ADM0_NAME))

    # Province shapes local===============
    global.prov.01 <- sf::st_read(dsn = gdb_folder, layer = "GLOBAL_ADM1") |>
      dplyr::mutate(STARTDATE = as.Date(STARTDATE),
                    ENDDATE = as.Date(ENDDATE),
                    yr.st = lubridate::year(STARTDATE),
                    yr.end = lubridate::year(ENDDATE),
                    ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"), "COTE D IVOIRE", ADM0_NAME)
      )

    # District shapes local===============
    global.dist.01 <- sf::st_read(dsn = gdb_folder, layer = "GLOBAL_ADM2") |>
      dplyr::mutate(STARTDATE = as.Date(STARTDATE),
                    ENDDATE = as.Date(ENDDATE),
                    yr.st = lubridate::year(STARTDATE),
                    yr.end = lubridate::year(ENDDATE),
                    ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"), "COTE D IVOIRE", ADM0_NAME))

  }

  #identifying bad shapes
  check.ctry.valid <- tibble::as_tibble(sf::st_is_valid(global.ctry.01))
  row.num.ctry <- which(check.ctry.valid$value == FALSE)
  invalid.ctry.shapes <- global.ctry.01 |>
    dplyr::slice(row.num.ctry) |>
    dplyr::select(ADM0_NAME, GUID, yr.st, yr.end, Shape) |>
    dplyr::arrange(ADM0_NAME)

  sf::st_geometry(invalid.ctry.shapes) <- NULL

  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/invalid_ctry_shapes.csv"),
                 obj = invalid.ctry.shapes)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/invalid_ctry_shapes.csv"),
                 obj = invalid.ctry.shapes)
  }

  empty.ctry <- global.ctry.01 |>
    dplyr::mutate(empty = sf::st_is_empty(Shape)) |>
    dplyr::filter(empty == TRUE)

  sf::st_geometry(empty.ctry) <- NULL

  if(nrow(empty.ctry) > 0) {
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/empty_ctry_shapes.csv"),
                   obj = empty.ctry)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/empty_ctry_shapes.csv"),
                   obj = empty.ctry)
    }
  }

  rm(invalid.ctry.shapes, check.ctry.valid, row.num.ctry, empty.ctry)
  # save global country geodatabase in RDS file:
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/global.ctry.rds"),
                 obj = global.ctry.01)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/global.ctry.rds"),
                 obj = global.ctry.01)
  }

  sf::st_geometry(global.ctry.01) <- NULL


  # Province shapes overlapping in Lower Juba in Somalia.
  global.prov.01 <- global.prov.01 |>
    dplyr::mutate(yr.end = ifelse(ADM0_GUID == '{B5FF48B9-7282-445C-8CD2-BEFCE4E0BDA7}' &
                                    GUID == '{EE73F3EA-DD35-480F-8FEA-5904274087C4}', 2021, yr.end))

  check.prov.valid <- tibble::as_tibble(sf::st_is_valid(global.prov.01))
  row.num.prov <- which(check.prov.valid$value == FALSE)
  invalid.prov.shapes <- global.prov.01 |>
    dplyr::slice(row.num.prov) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, GUID, yr.st, yr.end, SHAPE) |>
    dplyr::arrange(ADM0_NAME)

  sf::st_geometry(invalid.prov.shapes) <- NULL

  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/invalid_prov_shapes.csv"),
                 obj = invalid.prov.shapes)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/invalid_prov_shapes.csv"),
                 obj = invalid.prov.shapes)
  }

  empty.prov <- global.prov.01 |>
    dplyr::mutate(empty = sf::st_is_empty(SHAPE)) |>
    dplyr::filter(empty == TRUE)

  if(nrow(empty.prov) > 0) {
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/empty_prov_shapes.csv"),
                   obj = empty.prov)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/empty_prov_shapes.csv"),
                   obj = empty.prov)
    }
  }

  rm(check.prov.valid, row.num.prov, invalid.prov.shapes, empty.prov)
  # save global province geodatabase in RDS file:
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/global.prov.rds"),
                 obj = global.prov.01)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/global.prov.rds"),
                 obj = global.prov.01)
  }

  sf::st_geometry(global.prov.01) <- NULL

  check.dist.valid <- tibble::as_tibble(sf::st_is_valid(global.dist.01))
  row.num.dist <- which(check.dist.valid$value == FALSE)
  invalid.dist.shapes <- global.dist.01 |>
    dplyr::slice(row.num.dist) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME, GUID, yr.st, yr.end, SHAPE) |>
    dplyr::arrange(ADM0_NAME)

  sf::st_geometry(invalid.dist.shapes) <- NULL

  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/invalid_dist_shapes.csv"),
                 obj = invalid.dist.shapes)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/invalid_dist_shapes.csv"),
                 obj = invalid.dist.shapes)
  }

  empty.dist <- global.dist.01 |>
    dplyr::mutate(empty = sf::st_is_empty(SHAPE)) |>
    dplyr::filter(empty == TRUE)

  if(nrow(empty.dist) > 0) {
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/empty_dist_shapes.csv"),
                   obj = empty.dist)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/empty_dist_shapes.csv"),
                   obj = empty.dist)
    }
  }

  rm(check.dist.valid, row.num.dist, invalid.dist.shapes, empty.dist)
  # save global province geodatabase in RDS file:
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/global.dist.rds"),
                 obj = global.dist.01)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/global.dist.rds"),
                 obj = global.dist.01)
  }

  sf::st_geometry(global.dist.01) <- NULL

  endyr <- year(format(Sys.time()))
  startyr <- 2000

  # Province long shape

  df.list <- list()

  for(i in startyr:endyr) {
    df02 <- sirfunctions:::f.yrs.01(global.prov.01, i)

    df.list[[i]] <- df02
  }

  long.global.prov.01 <- do.call(rbind, df.list)

  if(endyr == lubridate::year(format(Sys.time())) & startyr == 2000) {
    prov.shape.issue.01 <- long.global.prov.01 |>
      dplyr::group_by(ADM0_NAME, ADM1_NAME, active.year.01) |>
      dplyr::summarise(no.of.shapes = dplyr::n()) |>
      dplyr::filter(no.of.shapes > 1)

    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/prov_shape_multiple_",
                                      paste(min(prov.shape.issue.01$active.year.01, na.rm = T),
                                            max(prov.shape.issue.01$active.year.01, na.rm = T),
                                            sep = "_"), ".csv"),
                   obj = prov.shape.issue.01)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/prov_shape_multiple_",
                                      paste(min(prov.shape.issue.01$active.year.01, na.rm = T),
                                            max(prov.shape.issue.01$active.year.01, na.rm = T),
                                            sep = "_"), ".csv"),
                   obj = prov.shape.issue.01)
    }
  }

  # District long shape

  df.list <- list()

  for(i in startyr:endyr) {
    df02 <- sirfunctions:::f.yrs.01(global.dist.01, i)

    df.list[[i]] <- df02
  }

  long.global.dist.01 <- do.call(rbind, df.list)

  if(endyr == year(format(Sys.time())) & startyr == 2000) {
    dist.shape.issue.01 <- long.global.dist.01 |>
      dplyr::group_by(ADM0_NAME, ADM1_NAME, ADM2_NAME, active.year.01) |>
      dplyr::summarise(no.of.shapes = dplyr::n()) |>
      dplyr::filter(no.of.shapes > 1)

    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/dist_shape_multiple_",
                                      paste(min(dist.shape.issue.01$active.year.01, na.rm = T),
                                            max(dist.shape.issue.01$active.year.01, na.rm = T),
                                            sep = "_"), ".csv"),
                   obj = dist.shape.issue.01)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/dist_shape_multiple_",
                                      paste(min(dist.shape.issue.01$active.year.01, na.rm = T),
                                            max(dist.shape.issue.01$active.year.01, na.rm = T),
                                            sep = "_"), ".csv"),
                   obj = dist.shape.issue.01)
    }
  }

  remove(df.list, df02)

}
