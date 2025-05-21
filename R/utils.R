#### POLIS Interactions ####

#' Request data from single table
#'
#' @description Get POLIS table Data
#' @import cli lubridate dplyr readr
#' @param api_key API Key
#' @param .table Table value to retrieve
#' @returns Tibble with reference data
#' @examples
#' \dontrun{
#' get_table_data(.table = "case")
#' get_table_data(.table = "virus") #must run init_tidypolis first in order to specify API key
#' }
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
  doFuture::registerDoFuture() ## tell foreach to use future

  if (stringr::str_starts(Sys.getenv("SF_PARTNER"), "posit_workbench")) {
    future::plan(future::multicore)
  } else {
    future::plan(future::multisession) ## parallelize over a local PSOCK cluster
  }

  options(doFuture.rng.onMisuse = "ignore")
  xs <- seq_along(urls)

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
        log <- dplyr::tibble(time = Sys.time(),
                             call = urls[x],
                             event = "MADE CALL")

        tryCatch(
          {
            response <- call_single_url(urls[x])
            log <- log |>
              dplyr::add_row(time = Sys.time(),
                             call = urls[x],
                             event = "FINISHED CALL")
          },
          error = \(e) {
            response <- NA
            log <- log |>
              dplyr::add_row(time = Sys.time(),
                             call = urls[x],
                             event = "CALL FAILED")
          }
        )

        dplyr::tibble(response = list(response),
                      log = list(log))

      })
  })

  resp <- dplyr::bind_rows(y) |>
    dplyr::filter(!is.na(response)) |>
    dplyr::pull(response) |>
    dplyr::bind_rows()

  log <- dplyr::bind_rows(y) |>
    dplyr::pull(log) |>
    dplyr::bind_rows()

  if (as.logical(Sys.getenv("API_DEBUG"))) {
    api_log <- tidypolis_io(io = "read", file_path = Sys.getenv("POLIS_API_LOG_FILE"))
    api_log <- dplyr::bind_rows(api_log, log)
    tidypolis_io(api_log,
                 io = "write", file_path = Sys.getenv("POLIS_API_LOG_FILE"))
  }

  gc()
  return(resp)

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
#' @import tibble cli
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
  invisible(capture.output(log_file <- tidypolis_io(io = "read", file_path = log_file_path)))

  log_names <- log_file |>
    names()

  if(!"event_type" %in% log_names){

    invisible(
      capture.output(
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
      )
    )


  }else{

    invisible(
      capture.output(
        log_file |>
          tibble::add_row(time = .time,
                          user = .user,
                          event_type = .event_type,
                          event = .event) |>
          tidypolis_io(io = "write", file_path = log_file_path)
      )
    )

  }

  cli::cli_alert_info(paste0("Updated tidypolis log -- Event Type: ", .event_type))
}

#### Local Cache ####

#' Load local POLIS cache
#'
#' @description Pull cache data for a particular table
#' @import cli dplyr
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
#' @import dplyr lubridate
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

#### External Data ####

#' Get crosswalk data
#'
#' @description
#' Get all data from crosswalk location
#' @param file_loc str: location of crosswalk file
#' @import dplyr cli sirfunctions
#' @return tibble: crosswalk data
get_crosswalk_data <- function(
    file_loc = file.path(Sys.getenv("POLIS_DATA_FOLDER"),
                         "misc",
                         "crosswalk.rds")
){
  cli::cli_process_start("Import crosswalk")
  invisible(
    capture.output(
      crosswalk <-
        tidypolis_io(io = "read", file_path = file_loc) |>
        #TrendID removed from export
        dplyr::filter(!API_Name %in% c("Admin0TrendId", "Admin0Iso2Code"))
    )
  )
  cli::cli_process_done()
  return(crosswalk)
}

#' Function to get cached env site data
#' @description Function to get cached env site data
#' @param file_loc `path` Path to the env_sites.rds file.
#'
#' @returns `tibble` Env site list
#' @keywords internal
get_env_site_data <- function(file_loc = file.path(Sys.getenv("POLIS_DATA_FOLDER"),
                                                   "misc",
                                                   "env_sites.rds")) {

  invisible(capture.output(
    envSiteYearList <- tidypolis_io(io = "read",
                                    file_path = file_loc)
  ))

  return(envSiteYearList)
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
    dplyr::mutate(dplyr::across(dplyr::everything(), as.character))

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
#' @import dplyr stringr
#' @param old.download tibble
#' @param new.download tibble
#' @returns tibble: variables that are new or unaccounted for
f.download.compare.01 <- function(old.download, new.download) {

  if (!requireNamespace("purrr", quietly = TRUE)) {
    stop('Package "purrr" must be installed to use this function.',
         .call = FALSE
    )
  }

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
#' @import dplyr
#' @param df.from.f.download.compare.01 tibble: output from f.download.compare.01
#' @param old.download tibble
#' @param new.download tibble
#' @param type str name of the df that the comparison is being made on, for use in log
#' "AFP", "SIA", "ES", "POS"
#' @returns tibble of variables to compare
f.download.compare.02 <- function(df.from.f.download.compare.01,
                                  old.download,
                                  new.download,
                                  type = NULL) {

  if (!requireNamespace("purrr", quietly = TRUE)) {
    stop('Package "purrr" must be installed to use this function.',
         .call = FALSE
    )
  }

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
  new.distinct.value.01 <- list()

  for(i in 1:length(x)) {
    var.check <- dplyr::anti_join(
      (dplyr::bind_rows(old.download, new.download) |> dplyr::select(x[i]) |> dplyr::distinct()),
      (old.download |> dplyr::select(x[i]) |> dplyr::distinct()), by = x[i]
    )

    new.distinct.value.01[[i]] <- var.check

  }

  for (i in 1:length(new.distinct.value.01)) {
    update_polis_log(.event = paste0("New values in ", type, " for ", x[i], ": ",
                                     paste(unlist(new.distinct.value.01[i]),
                                           collapse = ", ")),
                     .event_type = "ALERT")
  }
}

#' Sample points for missing lat/lon
#' @description Create random samples of points for missing GPS data
#' @import dplyr sf tidyr tibble cli
#' @param df01 tibble: table of afp data
#' @param global.dist.01 sf: spatial file of all locations
#' @returns tibble with lat/lon for all unsampled locations
f.pre.stsample.01 <- function(df01, global.dist.01) {

  #need to identify cases with no lat/lon
  empty.coord <- df01 |>
    dplyr::filter(is.na(polis.latitude) | is.na(polis.longitude) |
                    (polis.latitude == 0 & polis.longitude == 0))

  cli::cli_process_start("Spatially joining AFP cases to global districts")
  #create sf object from lat lon and make global.dist valid
  df01.sf <- df01 |>
    dplyr::filter(!epid %in% empty.coord$epid) |>
    dplyr::mutate(lon = polis.longitude,
                  lat = polis.latitude) |>
    sf::st_as_sf(coords = c(x = "lon" , y = "lat"),
                 crs = sf::st_crs(global.dist.01))

  global.dist.02 <- sf::st_make_valid(global.dist.01)

  #identify bad shape rows after make_valid
  check.dist.2 <- tibble::as_tibble(sf::st_is_valid(global.dist.02))

  #removing all bad shapes post make valid
  valid.shapes <- global.dist.02[check.dist.2$value, ] |>
    dplyr::select(GUID, ADM1_GUID, ADM0_GUID, yr.st, yr.end, Shape)

  cli::cli_process_start("Evaluating invalid district shapes")
  #invalid shapes for which we'll turn off s2
  invalid.shapes <- global.dist.02[!check.dist.2$value, ] |>
    dplyr::select(GUID, ADM1_GUID, ADM0_GUID, yr.st, yr.end, Shape)

  #do 2 seperate st_joins the first, df02, is for valid shapes and those attached cases
  df02 <- sf::st_join(df01.sf |>
                        dplyr::filter(!Admin2GUID %in% invalid.shapes$GUID), valid.shapes, left = T) |>
    dplyr::filter(yronset >= yr.st & yronset <= yr.end)

  #second st_join is for invalid shapes and those attached cases, turning off s2
  sf::sf_use_s2(F)
  df03 <- sf::st_join(df01.sf |>
                        dplyr::filter(!Admin2GUID %in% valid.shapes$GUID), invalid.shapes, left = T) |>
    dplyr::filter(yronset >= yr.st & yronset <= yr.end)
  sf::sf_use_s2(T)

  cli::cli_process_done()

  #bind back together df02 and df03
  df04 <- dplyr::bind_rows(df02, df03)

  cli::cli_process_done()

  #df04 has a lot of dupes due to overlapping shapes, need to appropriately de dupe
  #identify duplicate obs
  dupes <- df04 |>
    dplyr::group_by(epid) |>
    dplyr::mutate(n = n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n >1)

  #duplicate obs where adm2guid matches GUID in shapefile
  dupes.01 <- dupes |>
    dplyr::filter(Admin2GUID == GUID) |>
    dplyr::select(-n)

  #duplicate obs where adm2guid is NA or doesn't match to shapefile
  dupes.02 <- dupes |>
    dplyr::filter(!epid %in% dupes.01$epid) |>
    dplyr::group_by(epid) |>
    dplyr::slice(1) |>
    dplyr::ungroup() |>
    dplyr::select(-n)

  #fixed duplicates
  dupes.fixed <- dplyr::bind_rows(dupes.01, dupes.02)

  rm(dupes, dupes.01, dupes.02)

  #remove the duplicate cases from df04 and bind back the fixed dupes
  df05 <- df04 |>
    dplyr::filter(!epid %in% dupes.fixed$epid) |>
    dplyr::bind_rows(dupes.fixed) |>
    dplyr::mutate(Admin2GUID = paste0("{", stringr::str_to_upper(admin2guid), "}", sep = ""),
                  Admin1GUID = paste0("{", stringr::str_to_upper(admin1guid), "}", sep = ""),
                  Admin0GUID = paste0("{", stringr::str_to_upper(admin0guid), "}", sep = ""))

  #fix guids after de-duping
  fix.bad.guids <- df05 |>
    dplyr::filter(Admin2GUID != GUID | Admin1GUID != ADM1_GUID | Admin0GUID != ADM0_GUID) |>
    dplyr::mutate(Admin2GUID = ifelse(Admin2GUID != GUID, GUID, Admin2GUID),
                  Admin1GUID = ifelse(Admin1GUID != ADM1_GUID, ADM1_GUID, Admin1GUID),
                  Admin0GUID = ifelse(Admin0GUID != ADM0_GUID, ADM0_GUID, Admin0GUID),
                  geo.corrected = 1) |>
    # if fix.bad.guids is empty, then the GUID cols become logical but the
    # join in df06 requires them to be of char type.
    dplyr::mutate(Admin2GUID = as.character(Admin2GUID),
                  Admin1GUID = as.character(Admin1GUID),
                  Admin0GUID = as.character(Admin0GUID))

  #bind back cases with fixed guids
  df06 <- df05 |>
    dplyr::filter(!epid %in% fix.bad.guids$epid) |>
    dplyr::mutate(geo.corrected = 0) |>
    dplyr::bind_rows(fix.bad.guids)

  rm(fix.bad.guids)
  #identify dropped obs. obs are dropped primarily because they match to a shape that doesn't
  #exist for the case's year onset (there are holes in the global map for certain years)
  df04$geometry <- NULL
  #antijoin from df01 to keep polis.lat/lon
  dropped.obs <- dplyr::anti_join(df01, df04, by = "epid") |>
    dplyr::filter(!epid %in% df04$epid & epid %in% df01.sf$epid)

  #bring df05 and dropped observations back together, create lat/lon var from sf object previously created
  df07 <- dplyr::bind_cols(
    tibble::as_tibble(df06),
    sf::st_coordinates(df06) %>%
      {if (nrow(df06) == 0) {
        # if df06 is empty, as_tibble won't work and we need to create it manually
        dplyr::tibble(X = as.double(NA),
                      Y = as.double(NA)) |>
          dplyr::filter(!is.na(X))
      } else {
        dplyr::as_tibble(.)
      }
      } |>
      dplyr::rename("lon" = "X", "lat" = "Y")) %>%
    {
      if (nrow(dropped.obs) != 0) {
        dplyr::bind_rows(., dropped.obs)
      } else {
        .
      }
    } |>
    dplyr::select(-dplyr::all_of(c("GUID", "yr.st", "yr.end")))

  df07$geometry <- NULL

  sf::st_geometry(global.dist.02) <- NULL

  #feed only cases with empty coordinates into st_sample (vars = GUID, nperarm, id, Shape)
  if (nrow(empty.coord |> dplyr::filter(Admin2GUID != "{NA}")) > 0) {
    # remove NAs because can't be sampled
    empty.coord.01 <- empty.coord |>
      tibble::as_tibble() |>
      dplyr::group_by(Admin2GUID) |>
      dplyr::summarise(nperarm = dplyr::n()) |>
      dplyr::arrange(Admin2GUID) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::ungroup() |>
      dplyr::filter(Admin2GUID != "{NA}")

    empty.coord.02 <- global.dist.01 |>
      dplyr::select(GUID) |>
      dplyr::filter(GUID %in% empty.coord.01$Admin2GUID) |>
      dplyr::left_join(empty.coord.01, by = c("GUID" = "Admin2GUID"))

    cli::cli_process_start("Placing random points for cases with bad coordinates")
    pt01 <- lapply(1:nrow(empty.coord.02), function(x){

      tryCatch(
        expr = {suppressMessages(sf::st_sample(empty.coord.02[x,],
                                               dplyr::pull(empty.coord.02[x,], "nperarm"),
                                               exact = T)) |> sf::st_as_sf()},
        error = function(e) {
          guid = empty.coord.02[x, ]$GUID[1]
          ctry_prov_dist_name = global.dist.01 |>
            dplyr::filter(GUID == guid) |>
            dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME)
          cli::cli_alert_warning(paste0("Fixing errors for:\n",
                                        "Country: ", ctry_prov_dist_name$ADM0_NAME,"\n",
                                        "Province: ", ctry_prov_dist_name$ADM1_NAME, "\n",
                                        "District: ", ctry_prov_dist_name$ADM2_NAME))

          suppressWarnings(
            {
              sf::sf_use_s2(F)
              int <- empty.coord.02[x,] |> sf::st_centroid(of_largest_polygon = T)
              sf::sf_use_s2(T)

              sf::st_buffer(int, dist = 3000) |>
                sf::st_sample(dplyr::slice(empty.coord.02, x) |>
                                dplyr::pull(nperarm)) |>
                sf::st_as_sf()
            }
          )

        }
      )

    }) |>
      dplyr::bind_rows()

    cli::cli_process_done()

    pt01_joined <- dplyr::bind_cols(
      pt01,
      empty.coord.02 |>
        tibble::as_tibble() |>
        dplyr::select(GUID, nperarm) |>
        tidyr::uncount(nperarm)
    ) |>
      dplyr::left_join(tibble::as_tibble(empty.coord.02) |>
                         dplyr::select(-Shape),
                       by = "GUID")

    pt02 <- pt01_joined |>
      tibble::as_tibble() |>
      dplyr::select(-nperarm, -id) |>
      dplyr::group_by(GUID) |>
      dplyr::arrange(GUID, .by_group = TRUE) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      as.data.frame()

    pt03 <- empty.coord |>
      dplyr::group_by(Admin2GUID) |>
      dplyr::arrange(Admin2GUID, .by_group = TRUE) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::ungroup()

    pt04 <- dplyr::full_join(pt03, pt02, by = c("Admin2GUID" = "GUID", "id"))

    pt05 <- pt04 |>
      dplyr::bind_cols(
        tibble::as_tibble(pt04$x),
        sf::st_coordinates(pt04$x) |>
          tibble::as_tibble() |>
          dplyr::rename("lon" = "X", "lat" = "Y")) |>
      dplyr::select(-id)

    pt05$x <- NULL
    pt05$geometry <- NULL

    df08 <- dplyr::bind_rows(df07, pt05)

  } else {
    df08 <- df07
  }

  #bind back placed point cases with df06 and finished
  df09 <- df08 |>
    dplyr::left_join(global.dist.01 |> dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME, ADM0_GUID, ADM1_GUID, GUID),
                     by = c("Admin0GUID" = "ADM0_GUID", "Admin1GUID" = "ADM1_GUID", "Admin2GUID" = "GUID")) |>
    dplyr::mutate(geo.corrected = ifelse(paste0("{", stringr::str_to_upper(admin2guid), "}", sep = "") != Admin2GUID, 1, 0),
                  geo.corrected = ifelse(paste0("{", stringr::str_to_upper(admin1guid), "}", sep = "") != Admin1GUID, 1, geo.corrected),
                  geo.corrected = ifelse(paste0("{", stringr::str_to_upper(admin0guid), "}", sep = "") != Admin0GUID, 1, geo.corrected),
                  place.admin.0 = ifelse((place.admin.0 != ADM0_NAME | is.na(place.admin.0)) & !is.na(ADM0_NAME), ADM0_NAME, place.admin.0),
                  place.admin.1 = ifelse((place.admin.1 != ADM1_NAME | is.na(place.admin.1)) & !is.na(ADM1_NAME), ADM1_NAME, place.admin.1),
                  place.admin.2 = ifelse((place.admin.2 != ADM2_NAME | is.na(place.admin.2)) & !is.na(ADM2_NAME), ADM2_NAME, place.admin.2)) |>
    dplyr::select(-c("wrongAdmin0GUID", "wrongAdmin1GUID", "wrongAdmin2GUID", "ADM1_GUID", "ADM0_GUID", "ADM0_NAME",
                     "ADM1_NAME", "ADM2_NAME")) |>
    dplyr::mutate(geo.corrected = ifelse(is.na(geo.corrected), 0, geo.corrected))

  df09$Shape <- NULL

  final.guid.check <- df09 |>
    dplyr::filter((paste0("{", stringr::str_to_upper(admin2guid), "}", sep = "") != Admin2GUID |
                     paste0("{", stringr::str_to_upper(admin1guid), "}", sep = "") != Admin1GUID |
                     paste0("{", stringr::str_to_upper(admin0guid), "}", sep = "") != Admin0GUID) &
                    geo.corrected == 0) |>
    dplyr::select(epid, yronset, place.admin.0, place.admin.1, place.admin.2, admin0guid, admin1guid, admin2guid, Admin0GUID, Admin1GUID, Admin2GUID, geo.corrected)


  final.names.check <- df09 |>
    dplyr::select(epid, yronset, place.admin.0, place.admin.1, place.admin.2, admin0guid, admin1guid, admin2guid, Admin0GUID, Admin1GUID, Admin2GUID, geo.corrected) |>
    dplyr::filter((is.na(place.admin.0) & !is.na(admin0guid)) |
                    (is.na(place.admin.1) & !is.na(admin1guid)) |
                    (is.na(place.admin.2) & !is.na(admin2guid)))

  if (nrow(final.guid.check) > 0 | nrow(final.names.check) > 0) {
    cli::cli_alert_warning("A GUID or name has been misclassified, please run pre.stsample manually to identify")
    stop()
  } else {
    rm(final.names.check, final.guid.check)
  }

  return(df09)
}


#' Function for data qa check in AFP line list cleaning
#' @description function creates a new variable when combined with a mutate statement in R code
#' @import dplyr
#' @param date1 date 1 is the date to be checked against date2
#' @param date2 date 2 is the date of onset such that the date should be after onset
#' @returns quality controlled date variable
f.datecheck.onset <- function(date1, date2) {
  date1.qa <-
    dplyr::case_when(
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
                  dplyr::filter(is.na(var_class.x)))$var_name

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
    warning(print("There are variables in the POLIS table with different classes compared to when it was last retrieved Review in 'class_changed_vars'"))

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
#' @import skimr dplyr
#' @param dataframe tibble
#' @param categorical_max int: maximum number of categories considered
#' @returns tibble: metadata
f.summarise.metadata <- function(dataframe, categorical_max = 10){

  if (!requireNamespace("tidyselect", quietly = TRUE)) {
    stop('Package "tidyselect" must be installed to use this function.',
         .call = FALSE
    )
  }
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

  readr::write_csv(x = changed.virus.type, file = file.path(tempdir(), "changed_virus_type.csv"))
  readr::write_csv(x = change.virus.class, file = file.path(tempdir(), "changed_virus_class.csv"))
  readr::write_csv(x = new.virus.records, file = file.path(tempdir(), "new_virus_records.csv"))

  #coms section
  sirfunctions::send_teams_message(msg = paste0("New CORE data files info: ", report_info))
  sirfunctions::send_teams_message(msg = paste0("New CORE data files alerts: ", report_alert))
  sirfunctions::send_teams_message(msg = "Attached CSVs contain information on new/changed virus records",
                                   attach = c(file.path(tempdir(), "changed_virus_type.csv"),
                                              file.path(tempdir(), "changed_virus_class.csv"),
                                              file.path(tempdir(), "new_virus_records.csv"))
                                   )

}


#' function to archive log report
#'
#' @description
#' Function to read in log file and archive entries older than 3 months
#'
#' @import dplyr
#' @param log_file str: location of POLIS log file
#' @param polis_data_folder str: location of the POLIS data folder
archive_log <- function(log_file = Sys.getenv("POLIS_LOG_FILE"),
                        polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")){

  #create log archive

  archive.path <- file.path(polis_data_folder, "Log_Archive")

  flag.log.exists <- tidypolis_io(io = "exists.dir", file_path = archive.path)

  if (!flag.log.exists) {
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
  flag.log.exists <- tidypolis_io(io = "exists.file",
                                  file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds"))

  if (!flag.log.exists) {
    tidypolis_io(io = "write", obj = log.to.arch,
                 file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds"))
  } else {
    log_archrive <- tidypolis_io(io = "read",
                                 file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds"))
    log_archive <- log_archive |> dplyr::bind_rows(log.to.arch)
    tidypolis_io(obj = log_archive,
                 io = "write",
                 file_path = file.path(polis_data_folder, "Log_Archive/log_archive.rds"))
  }
}

#' function to remove original character formatted date vars from data tables
#'
#' @description
#' remove original date variables from POLIS tables
#' @import dplyr lubridate
#' @param type str: the table on which to remove original date vars, "AFP", "ES", "POS"
#' @param df tibble: the dataframe from which to remove character formatted dates
#' @param polis_data_folder str:  location of user's polis data folder
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return outputs a saved reference table of original date vars and a smaller
#' core ready file without character dates
remove_character_dates <- function(type,
                                   df,
                                   polis_data_folder = Sys.getenv("POLIS_DATA_CACHE"),
                                   output_folder_name){

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

  tidypolis_io(io = "write", obj = df.01, file_path = paste0(polis_data_folder, "/", output_folder_name, "/", type, "_orig_char_dates.rds"))

  return(df.02)
}

#' Creates a summary of SIA responses to cVDPV detections
#'
#' @description
#' A function to create summary SIA response variables for cVDPVs in the positives dataset.
#'
#' @import dplyr
#' @param pos `tibble` The positive viruses dataset.
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @returns `tibble` Positives dataset with summary of SIA response variables.
#' @keywords internal
create_response_vars <- function(pos,
                                 output_folder_name,
                                 polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")){

  #bring in processed SIA data
  path <- tidypolis_io(io = "list", file_path = file.path(Sys.getenv("POLIS_DATA_CACHE"), output_folder_name), full_names = T)

  tryCatch({
    sia <- tidypolis_io(io = "read", file_path = path[grepl("sia_2000", path)])
  }, error = \(e) {
    cli::cli_abort("Please run Step 3 of preprocessing before Step 5.")
  })

  sia.sub <- sia |>
    dplyr::select(sia.code, sia.sub.activity.code, activity.start.date,
                  sub.activity.start.date, vaccine.type, adm2guid)

  pos.sub <- pos |>
    dplyr::select(epid, dateonset, yronset, measurement, ntchanges, emergencegroup,
                  place.admin.0, place.admin.1, place.admin.2, adm0guid, adm1guid,
                  admin2guid) |>
    dplyr::filter(measurement %in% c("cVDPV 1", "cVDPV 2", "cVDPV 3"))

  #attach all sias post onset/collection for appropriate type and filter to 6 months
  #cVDPV2 <- tOPV / nOPV2 / mOPV2
  #cVDPV1 <- tOPV / bOPV / mOPV1
  #cVDPV3 <- tOPV / bOPV / mOPV3
  type1 <- dplyr::left_join(pos.sub |> dplyr::filter(measurement == "cVDPV 1"),
                            sia.sub |> dplyr::filter(vaccine.type %in% c("tOPV", "bOPV", "mOPV1")),
                            by = c("admin2guid" = "adm2guid")) |>
    dplyr::mutate(time.to.response = difftime(sub.activity.start.date, dateonset, units = "days")) |>
    dplyr::filter(sub.activity.start.date < Sys.Date(),
                  dateonset < sub.activity.start.date,
                  time.to.response <= 180) |>
    unique()

  type2 <- dplyr::left_join(pos.sub |> dplyr::filter(measurement == "cVDPV 2"),
                            sia.sub |> dplyr::filter(vaccine.type %in% c("tOPV", "nOPV2", "mOPV2")),
                            by = c("admin2guid" = "adm2guid")) |>
    dplyr::mutate(time.to.response = difftime(sub.activity.start.date, dateonset, units = "days")) |>
    dplyr::filter(sub.activity.start.date < Sys.Date(),
                  dateonset < sub.activity.start.date,
                  time.to.response <= 180) |>
    unique()

  type3 <- dplyr::left_join(pos.sub |> dplyr::filter(measurement == "cVDPV 3"),
                            sia.sub |> dplyr::filter(vaccine.type %in% c("tOPV", "bOPV", "mOPV3")),
                            by = c("admin2guid" = "adm2guid")) |>
    dplyr::mutate(time.to.response = difftime(sub.activity.start.date, dateonset, units = "days")) |>
    dplyr::filter(sub.activity.start.date < Sys.Date(),
                  dateonset < sub.activity.start.date,
                  time.to.response <= 180) |>
    unique()

  finished.responses <- rbind(type1, type2, type3) |>
    dplyr::group_by(epid, ntchanges, emergencegroup) |>
    dplyr::mutate(finished.responses = n()) |>
    dplyr::ungroup() |>
    dplyr::select(epid, dateonset, ntchanges, emergencegroup, finished.responses) |>
    unique()

  rm(type1, type2, type3)

  #identify planned sias and attach them to positive cases
  planned.sia <- sia.sub |>
    dplyr::filter(activity.start.date > Sys.Date()) |>
    dplyr::select(sia.code, sia.sub.activity.code, activity.start.date,
                  sub.activity.start.date, vaccine.type, adm2guid) |>
    dplyr::group_by(adm2guid) |>
    dplyr::mutate(planned.campaigns = n()) |>
    dplyr::ungroup()

  planned.responses <- dplyr::left_join(pos.sub, planned.sia |> dplyr::select(adm2guid, planned.campaigns,
                                                                              sub.activity.start.date),
                                        by = c("admin2guid" = "adm2guid")) |>
    dplyr::mutate(planned.campaigns = ifelse(is.na(planned.campaigns), 0, planned.campaigns)) |>
    unique() |>
    dplyr::select(epid, dateonset, ntchanges, emergencegroup, planned.campaigns, sub.activity.start.date) |>
    dplyr::filter(difftime(sub.activity.start.date, dateonset, units = "days") <= 180)

  #identify completed ipv campaigns
  ipv.response <- dplyr::left_join(pos.sub,
                                   sia.sub |>
                                     dplyr::filter(vaccine.type == "IPV") |>
                                     dplyr::select(sub.activity.start.date, adm2guid),
                                   by = c("admin2guid" = "adm2guid")) |>
    dplyr::filter(dateonset < sub.activity.start.date,
                  difftime(sub.activity.start.date, dateonset, units = "days") <= 180) |>
    dplyr::group_by(epid, ntchanges, emergencegroup, admin2guid) |>
    dplyr::mutate(ipv.campaigns = n()) |>
    dplyr::ungroup() |>
    dplyr::select(epid, dateonset, ntchanges, emergencegroup, ipv.campaigns) |>
    unique()

  pos.sub.01 <- dplyr::left_join(pos.sub, finished.responses, by = c("epid", "dateonset", "ntchanges", "emergencegroup"))
  pos.sub.02 <- dplyr::left_join(pos.sub.01, planned.responses, by = c("epid", "dateonset", "ntchanges", "emergencegroup"))
  pos.sub.03 <- dplyr::left_join(pos.sub.02, ipv.response, by = c("epid", "dateonset", "ntchanges", "emergencegroup")) |>
    dplyr::mutate(finished.responses = ifelse(is.na(finished.responses), 0, finished.responses),
                  planned.campaigns = ifelse(is.na(planned.campaigns), 0, planned.campaigns),
                  ipv.campaigns = ifelse(is.na(ipv.campaigns), 0, ipv.campaigns))

  pos.final <- dplyr::left_join(pos, pos.sub.03) |>
    dplyr::select(-sub.activity.start.date) |>
    unique()

  return(pos.final)
}


#' A general function cluster sets of dates
#' @description
#' This function takes in a set of dates (primarily used for immunization
#' campaigns) and clusters them into likely "rounds" which is not readily
#' captured in the data using K-means clustering
#'
#' @import dplyr cluster
#' @param x `tibble` dataset with dates to be clustered. Requires that the tibble
#' has as a minimum a column named "sub.activity.start.date"
#' @param seed `int` defaults to 1234, used to ensure the clusters generated by
#' different individuals are the same
#' @param method `str` cluster method to use, can be "kmeans" (default) or "mindate"
#' @param grouping_days `int` defaults to 365. If there are not enough observations
#' to use the "k-means" method we will group by events taking place within a certain
#' period of time of each other. In this case the grouping days will be 365 or a year
#' @returns `tibble` with clusters groups and cluster methodology used
#' @export
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
    #calculate the optimal number of clusters using the "Gap" method so that
    #researchers don't have to choose arbitrary cutoffs
    invisible(capture.output(
      optim_k <- y %>%
        #calculate optimal number of clusters using the
        #gap statistic
        #see clusGap documentation for parameter specifications
        {
          cluster::clusGap(
            x = .,
            FUN = stats::kmeans,
            nstart = 25,
            K.max = max(min(nrow(.)-1, nrow(.)/2), 2),
            B = 100)
        } %>%
        #extract gap statistic matrix
        {.$Tab[,"gap"]} %>%
        #calculate the max gap statistic, given the sparsity in the data
        #am not limiting to the first max SE method
        which.max()
    ))

    set.seed(seed)
    #calculate the clusters
    invisible(capture.output(
      x$cluster <- stats::kmeans(y, optim_k)$cluster %>%
        #clusters don't always come out in the order we want them to
        #so here we convert them into factors, relevel and then extract
        #the numeric value to ensure that the cluster numbers are in order
        {factor(., levels = unique(.))} |>
        as.numeric()
    ))

    #outputting the method used
    x$cluster_method <- method

    return(x)

  }else{
    #if there are at least a minimum number of observations (default is 4)
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




#' Function to create summary of key variable missingness in CORE datafiles
#' @description
#' a function to assess key variable missingness
#' @import dplyr
#' @param data tibble the datatable for which we want to check key variable missingness
#' @param type str "AFP", "ES", or "POS", type of dataset to check missingness
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
check_missingness <- function(data,
                              type, output_folder_name) {

  if (type == "AFP") {

    afp.vars <- c("notification.date", "investigation.date",
                  "stool.1.collection.date", "stool.2.collection.date",
                  "date.notification.to.hq", "results.seq.date.to.program", "specimen.date",
                  "case.date", "date.onset", "stool.date.sent.to.lab", "clinical.admitted.date",
                  "followup.date", "stool.1.condition", "stool.2.condition", "age.months")

    missing_by_group <- data |>
      dplyr::select("yronset", "place.admin.0", dplyr::any_of(afp.vars)) |>
      dplyr::summarise(dplyr::across(dplyr::everything(), ~ mean(is.na(.)) * 100), .by = c("yronset", "place.admin.0")) |>
      dplyr::filter(dplyr::if_any(dplyr::any_of(afp.vars), ~ . >= 10))

    invisible(capture.output(
      tidypolis_io(io = "write",
                   obj = missing_by_group,
                   file_path = paste0(Sys.getenv("POLIS_DATA_CACHE"), "/",
                                      output_folder_name,
                                      "/afp_missingness.rds"))
    ))


  }

  if (type == "ES") {

    missing_by_group <- data |>
      dplyr::select(collect.yr, admin.0, who.region, collection.date) |>
      dplyr::summarise(dplyr::across(dplyr::everything(), ~ mean(is.na(.)) * 100), .by = c("collect.yr", "admin.0")) |>
      dplyr::filter(who.region >= 10 | collection.date >= 10)

    if (nrow(missing_by_group) > 0) {

      invisible(capture.output(
        tidypolis_io(io = "write",
                     obj = missing_by_group,
                     file_path = paste0(Sys.getenv("POLIS_DATA_CACHE"), "/",
                                        output_folder_name, "/es_missingness.rds"))
      ))


    }
  }

  return(missing_by_group)
}

#### Pre-processing ####


#' Preprocess data process data in the CDC style
#'
#' @description
#' Process POLIS data into analytic datasets needed for CDC
#' @import cli sirfunctions dplyr readr lubridate stringr tidyr stringi
#' @param polis_folder str: location of the POLIS data folder
#' @param who_region str: optional WHO region to filter data
#'      Available inputs include AFRO, AMRO, EMRO, EURO, SEARO and  WPRO.
#' @returns Outputs intermediary core ready files
#' @keywords internal
#'
preprocess_cdc <- function(polis_folder = Sys.getenv("POLIS_DATA_FOLDER"),
                           who_region = NULL) {

  # Static global variables used in some part of our code
  polis_data_folder <- file.path(polis_folder, "data")
  ts <- Sys.time()
  timestamp <- paste0(
    lubridate::date(ts),
    "_",
    lubridate::hour(ts),
    "-",
    lubridate::minute(ts),
    "-",
    round(lubridate::second(ts), 0)
  )

  suppressMessages(
    latest_folder_in_archive <- s2_find_latest_archive(
      polis_data_folder = polis_data_folder, timestamp = timestamp,
      output_folder_name = "Core_Ready_Files")
  )

  if (!requireNamespace("purrr", quietly = TRUE)) {
    stop('Package "purrr" must be installed to use this function.',
         .call = FALSE
    )
  }

  if (!requireNamespace("tidyselect", quietly = TRUE)) {
    stop('Package "tidyselect" must be installed to use this function.',
         .call = FALSE
    )
  }

  if (!requireNamespace("stats", quietly = TRUE)) {
    stop('Package "stats" must be installed to use this function.',
         .call = FALSE
    )
  }

  cli::cli_process_start("Long district spatial file")
  invisible(capture.output(
    long.global.dist.01 <- switch(Sys.getenv("POLIS_EDAV_FLAG"),
    "TRUE" = {
      sirfunctions::load_clean_dist_sp(
        fp = file.path(
          "GID/PEB/SIR",
          polis_folder,
          "misc",
          "global.dist.rds"
        ),
        type = "long"
      )
    },
    "FALSE" = {
      sirfunctions::load_clean_dist_sp(
        fp = file.path(
          polis_folder,
          "misc",
          "global.dist.rds"
        ),
        edav = FALSE,
        type = "long"
      )
    }
  )))

  cli::cli_process_done()

  #Step 0 - create a CORE datafiles to combine folder and check for datasets before continuing with pre-p =========


  core_files_folder_path <- file.path(polis_data_folder, "core_files_to_combine")
  if (!tidypolis_io(io = "exists.dir", file_path = core_files_folder_path)) {
    tidypolis_io(io = "create", file_path = core_files_folder_path)
  }

  # Check for required static files
  missing_static_files <- check_missing_static_files(core_files_folder_path)
  if (length(missing_static_files) > 0) {
    cli::cli_alert_warning(paste0(
      "Please request the following file(s) from the SIR team",
      "and move them into: ", core_files_folder_path
    ))
    for (file in missing_static_files) {
      cli::cli_alert_info(paste0(file, "\n"))
    }
    cli::cli_abort("Halting execution of preprocessing due to missing files.")
  }
  rm(core_files_folder_path, missing_static_files)


  # WHO Region set-up and validation ------------------------------------------
  # Default output folder name
  output_folder_name <- "Core_Ready_Files"

  # If a WHO region is provided:
  if (!is.null(who_region)) {
    # 2a. Validate region code
    valid <- c("AFRO","AMRO","EMRO","EURO","SEARO","WPRO")
    if (!who_region %in% valid) {
      cli::cli_abort("‘{who_region}’ is not a valid WHO region.")
    }

    # Set region-specific folder name
    output_folder_name <- paste0("Core_Ready_Files_", who_region)

    # Locate latest archive folder
    latest_folder_in_archive <-
      s2_find_latest_archive(polis_data_folder, timestamp,
                             output_folder_name)

  }

  #Step 1 - Basic cleaning and crosswalk ======
  cli::cli_h1("Step 1/5: Basic cleaning and crosswalk across datasets")

  s1_prep_polis_tables(polis_folder, polis_data_folder,
                       long.global.dist.01, ts, timestamp,
                       who_region)

  # Step 2 - Creating AFP and EPI datasets =====================================

  update_polis_log(
    .event = "Creating AFP and Epi analytic datasets",
    .event_type = "PROCESS"
  )

  cli::cli_h1("Step 2/5 - Creating AFP and Epi analytic datasets")

  s2_fully_process_afp_data(
    polis_data_folder = polis_data_folder,
    polis_folder = polis_folder,
    long.global.dist.01 = long.global.dist.01,
    timestamp = timestamp,
    latest_folder_in_archive_path = latest_folder_in_archive,
    output_folder_name = output_folder_name)

  invisible(gc())

  # Step 3 - Creating SIA analytic datasets ====================================

  cli::cli_h1("Step 3/5 - Creating SIA analytic datasets")

  s3_fully_process_sia_data(
    long.global.dist.01,
    polis_data_folder,
    latest_folder_in_archive,
    timestamp,
    output_folder_name = output_folder_name
  )

  invisible(gc())

  #Step 4 - Creating ES datasets====
  update_polis_log(.event = "Creating ES analytic datasets",
                   .event_type = "PROCESS")

  cli::cli_h1("Step 4/5 - Creating ES analytic datasets")

  s4_fully_process_es_data(polis_folder = polis_folder,
                           polis_data_folder = polis_data_folder,
                           latest_folder_in_archive = latest_folder_in_archive,
                           output_folder_name = output_folder_name)

  #Step 5 - Creating Virus datasets ====
  cli::cli_h1("Step 5/5 - Creating Virus datasets")

  s5_fully_process_pos_data(polis_folder = polis_folder,
                            polis_data_folder = polis_data_folder,
                            latest_folder_in_archive,
                            long.global.dist.01,
                            output_folder_name = output_folder_name)

  update_polis_log(.event = "Processing of CORE datafiles complete",
                   .event_type = "END")

  #log_report()
  #archive_log()

}

#### Spatial ####

# #' Preprocess population data into flat files
# #'
# #' @description Process POLIS population data using CDC and other standards
# #' @import readr dplyr
# #' @param type str: "cdc" or "who" (default)
# #' @param pop_file tibble: WHO POLIS population file, defaults to tidypolis folder
# #' @return list with tibble for ctry, prov and dist
# process_pop <- function(type = "who", pop_file = readr::read_rds(file.path(Sys.getenv("POLIS_DATA_FOLDER"), "data", "pop.rds"))){
#
#   #subset to <= 15
#   pop_file <- pop_file |>
#     filter(AgeGroupName == "0 to 15 years")
#
#   #extract into country prov and dist
#
#   x <- lapply(unique(pop_file$Admin0Name), function(x){
#     pop_file |>
#       filter(is.na(Admin1Name) & is.na(Admin2Name)) |>
#       rename(year = Year, u15pop = Value, GUID = Admin0GUID, ctry = Admin0Name) |>
#       mutate(u15pop = as.integer(u15pop)) |>
#       arrange(year) |>
#       filter(ctry == x) |>
#       group_by(year) |>
#       filter(!is.na(u15pop)) |>
#       filter(UpdatedDate == max(UpdatedDate, na.rm = T)) |>
#       ungroup() |>
#       select(ctry, year, u15pop, GUID) |>
#       full_join(tibble(ctry = x, year = 2000:(lubridate::year(Sys.time()))), by = c("ctry", "year"))
#   }) |>
#     bind_rows()
#
# }


#' A function to process WHO spatial datasets
#'
#' @description
#' a function to process WHO spatial datasets
#' @import dplyr sf lubridate stringr readr tibble cli
#' @param gdb_folder `str` The folder location of spatial datasets, should end with .gdb,
#' if on edav the gdb will need to be zipped, ensure that the gdb and the zipped file name are the same.
#' @param output_folder `str` Folder location to write outputs to.
#' @param edav `bool` Whether gdb is on EDAV or local.
#' @param azcontainer `Azure container` Azure storage container.
#' @export
#' @examples
#' \dontrun{
#' process_spatial(gdb_folder = "local_path/GEODATABASE.gdb",
#' output_folder = "local_path",
#' edav = F)
#' process_spatial(gdb_folder = "edav_path/GEODATABASE.gdb",
#' output_folder = "edav_path",
#' edav = T)
#' }
process_spatial <- function(gdb_folder,
                            output_folder,
                            edav,
                            azcontainer = suppressMessages(sirfunctions::get_azure_storage_connection())) {

  if (!requireNamespace("utils", quietly = TRUE)) {
    stop('Package "utils" must be installed to use this function.',
         .call = FALSE
    )
  }

  if(edav) {
    output_folder <- stringr::str_replace(output_folder, paste0("GID/PEB/SIR/"), "")
  }

  cli::cli_process_start("Loading raw spatial data")
  if (edav) {
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
  cli::cli_process_done()

  #identify sf var in global.ctry
  cli::cli_process_start("Country shapefile processing")
  sf_columns_ctry <- sapply(global.ctry.01, function(col) inherits(col, "sfc"))

  sf_var_ctry <- names(global.ctry.01)[sf_columns_ctry]

  cli::cli_process_start("Checking country shapes for validity")
  #identifying bad shapes
  check.ctry.valid <- tibble::as_tibble(sf::st_is_valid(global.ctry.01))
  row.num.ctry <- which(check.ctry.valid$value == FALSE)
  invalid.ctry.shapes <- global.ctry.01 |>
    dplyr::slice(row.num.ctry) |>
    dplyr::select(ADM0_NAME, GUID, yr.st, yr.end, paste(sf_var_ctry)) |>
    dplyr::arrange(ADM0_NAME)

  sf::st_geometry(invalid.ctry.shapes) <- NULL

  cli::cli_process_done()

  cli::cli_process_start("Outputting invalid country shapes")
  if (edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/invalid_ctry_shapes.csv"),
                 obj = invalid.ctry.shapes, azcontainer = azcontainer)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/invalid_ctry_shapes.csv"),
                 obj = invalid.ctry.shapes, azcontainer = azcontainer)
  }
  cli::cli_process_done()

  empty.ctry <- global.ctry.01 |>
    dplyr::mutate(empty = sf::st_is_empty(!!dplyr::sym(sf_var_ctry))) |>
    dplyr::filter(empty == TRUE)

  sf::st_geometry(empty.ctry) <- NULL

  if(nrow(empty.ctry) > 0) {
    cli::cli_process_start("Outputting empty country shapes")
    if (edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/empty_ctry_shapes.csv"),
                   obj = empty.ctry,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/empty_ctry_shapes.csv"),
                   obj = empty.ctry,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  rm(invalid.ctry.shapes, check.ctry.valid, row.num.ctry, empty.ctry)

  #identify potential duplicates
  cli::cli_process_start("Checking country shapes for duplicates")
  dupe.guid.ctry <- global.ctry.01 |>
    dplyr::group_by(GUID) |>
    dplyr::mutate(n = dplyr::n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1)

  dupe.name.ctry <- global.ctry.01 |>
    dplyr::group_by(ADM0_NAME, yr.st, yr.end) |>
    dplyr::mutate(n = dplyr::n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1)
  cli::cli_process_done()

  if(nrow(dupe.guid.ctry) > 1 | nrow(dupe.name.ctry) > 1) {
    cli::cli_alert_warning("There is a country shape with an exact duplicate, please manually run shape preprocessing to inspect")
  }

  if(nrow(dupe.guid.ctry) > 1) {
    sf::st_geometry(dupe.guid.ctry) <- NULL
    cli::cli_process_start("Outputting country shapes with duplicate GUIDs")
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/duplicate_ctry_guid.csv"),
                   obj = dupe.guid.ctry,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/duplicate_ctry_guid.csv"),
                   obj = dupe.guid.ctry,
                   azcontainer = azcontainer)
    }
  }

    cli::cli_process_done()

  if(nrow(dupe.name.ctry) > 1) {
    sf::st_geometry(dupe.name.ctry) <- NULL
    cli::cli_process_start("Outputting country shapes with duplicate names")
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/duplicate_ctry_name.csv"),
                   obj = dupe.name.ctry,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/duplicate_ctry_name.csv"),
                   obj = dupe.name.ctry,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  rm(dupe.guid.ctry, dupe.name.ctry, sf_columns_ctry, sf_var_ctry)

  #ensure CRS of ctry file is 4326
  global.ctry.01 <- sf::st_set_crs(global.ctry.01, 4326)

  # save global country geodatabase in RDS file:
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/global.ctry.rds"),
                 obj = global.ctry.01,
                 azcontainer = azcontainer)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/global.ctry.rds"),
                 obj = global.ctry.01,
                 azcontainer = azcontainer)
  }

  sf::st_geometry(global.ctry.01) <- NULL

  cli::cli_process_done()

  # Province shapes overlapping in Lower Juba in Somalia.
  cli::cli_process_start("Province shapefile processing")
  global.prov.01 <- global.prov.01 |>
    dplyr::mutate(yr.end = ifelse(ADM0_GUID == '{B5FF48B9-7282-445C-8CD2-BEFCE4E0BDA7}' &
                                    GUID == '{EE73F3EA-DD35-480F-8FEA-5904274087C4}', 2021, yr.end))

  #identify sf var in global.prov
  sf_columns_prov <- sapply(global.prov.01, function(col) inherits(col, "sfc"))

  sf_var_prov <- names(global.prov.01)[sf_columns_prov]

  cli::cli_process_start("Checking province shape validity")
  check.prov.valid <- tibble::as_tibble(sf::st_is_valid(global.prov.01))
  row.num.prov <- which(check.prov.valid$value == FALSE)
  invalid.prov.shapes <- global.prov.01 |>
    dplyr::slice(row.num.prov) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, GUID, yr.st, yr.end, paste(sf_var_prov)) |>
    dplyr::arrange(ADM0_NAME)

  sf::st_geometry(invalid.prov.shapes) <- NULL

  cli::cli_process_done()

  cli::cli_process_start("Outputting invalid province shapes")
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/invalid_prov_shapes.csv"),
                 obj = invalid.prov.shapes,
                 azcontainer = azcontainer)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/invalid_prov_shapes.csv"),
                 obj = invalid.prov.shapes,
                 azcontainer = azcontainer)
  }
  cli::cli_process_done()

  empty.prov <- global.prov.01 |>
    dplyr::mutate(empty = sf::st_is_empty(!!dplyr::sym(sf_var_prov))) |>
    dplyr::filter(empty == TRUE)

  if(nrow(empty.prov) > 0) {
    cli::cli_process_start("Outputting empty province shapes")
    sf::st_geometry(empty.prov) <- NULL
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/empty_prov_shapes.csv"),
                   obj = empty.prov,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/empty_prov_shapes.csv"),
                   obj = empty.prov,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  rm(check.prov.valid, row.num.prov, invalid.prov.shapes, empty.prov)

  #duplicate checking in provinces
  cli::cli_process_start("Checking province shapes for duplicates")
  dupe.guid.prov <- global.prov.01 |>
    dplyr::group_by(GUID) |>
    dplyr::mutate(n = n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1)

  dupe.name.prov <- global.prov.01 |>
    dplyr::group_by(ADM0_NAME, ADM1_NAME, yr.st, yr.end) |>
    dplyr::mutate(n = n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1)

  cli::cli_process_done()
  if(nrow(dupe.guid.prov) > 1 | nrow(dupe.name.prov) > 1) {
    cli::cli_alert_warning("There is a duplicated province that is exactly the same, please run shape preprocessing manually to inspect")
  }

  if(nrow(dupe.guid.prov) > 1) {
    cli::cli_process_start("Outputting provinces with duplicate GUIDs")
    sf::st_geometry(dupe.guid.prov) <- NULL
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/duplicate_prov_guid.csv"),
                   obj = dupe.guid.prov,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/duplicate_prov_guid.csv"),
                   obj = dupe.guid.prov,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  if(nrow(dupe.name.prov) > 1) {
    cli::cli_process_start("Outputting provinces with duplicate names")
    sf::st_geometry(dupe.name.prov) <- NULL
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/duplicate_prov_name.csv"),
                   obj = dupe.name.prov,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/duplicate_prov_name.csv"),
                   obj = dupe.name.prov,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  rm(dupe.guid.prov, dupe.name.prov, sf_columns_prov, sf_var_prov)

  #ensure CRS is 4326
  global.prov.01 <- sf::st_set_crs(global.prov.01, 4326)
  # save global province geodatabase in RDS file:
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/global.prov.rds"),
                 obj = global.prov.01,
                 azcontainer = azcontainer)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/global.prov.rds"),
                 obj = global.prov.01,
                 azcontainer = azcontainer)
  }

  sf::st_geometry(global.prov.01) <- NULL

  cli::cli_process_done()

  #identify sf var in global.dist
  cli::cli_process_start("District shape processing")
  sf_columns_dist <- sapply(global.dist.01, function(col) inherits(col, "sfc"))

  sf_var_dist <- names(global.dist.01)[sf_columns_dist]

  cli::cli_process_start("Checking district shape validity")
  check.dist.valid <- tibble::as_tibble(sf::st_is_valid(global.dist.01))
  row.num.dist <- which(check.dist.valid$value == FALSE)
  invalid.dist.shapes <- global.dist.01 |>
    dplyr::slice(row.num.dist) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME, GUID, yr.st, yr.end, paste(sf_var_dist)) |>
    dplyr::arrange(ADM0_NAME)

  sf::st_geometry(invalid.dist.shapes) <- NULL

  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/invalid_dist_shapes.csv"),
                 obj = invalid.dist.shapes,
                 azcontainer = azcontainer)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/invalid_dist_shapes.csv"),
                 obj = invalid.dist.shapes,
                 azcontainer = azcontainer)
  }
  cli::cli_process_done()

  empty.dist <- global.dist.01 |>
    dplyr::mutate(empty = sf::st_is_empty(!!dplyr::sym(sf_var_dist))) |>
    dplyr::filter(empty == TRUE)

  if(nrow(empty.dist) > 0) {
    cli::cli_process_start("Outputting empty district shapes")
    sf::st_geometry(empty.dist) <- NULL
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/empty_dist_shapes.csv"),
                   obj = empty.dist,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/empty_dist_shapes.csv"),
                   obj = empty.dist,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done
  }

  rm(check.dist.valid, row.num.dist, invalid.dist.shapes, empty.dist)

  #evaluate district duplicates
  cli::cli_process_start("Checking district shapes for duplicates")
  dupe.guid.dist <- global.dist.01 |>
    dplyr::group_by(GUID) |>
    dplyr::mutate(n = n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1)

  dupe.name.dist <- global.dist.01 |>
    dplyr::group_by(ADM0_NAME, ADM1_NAME, ADM2_NAME, yr.st, yr.end) |>
    dplyr::mutate(n = n()) |>
    dplyr::ungroup() |>
    dplyr::filter(n > 1) |>
    dplyr::arrange(ADM0_NAME, ADM1_NAME, ADM2_NAME, yr.st)

  if(nrow(dupe.guid.dist) > 1 | nrow(dupe.name.dist) > 1) {
    cli::cli_alert_warning("There are duplicates in district shapes, please run shape processing manually to inspect")
  }

  if(nrow(dupe.guid.dist) > 1) {
    cli::cli_process_start("Outputting districts with duplicate GUIDs")
    sf::st_geometry(dupe.guid.dist) <- NULL
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/duplicate_dist_guid.csv"),
                   obj = dupe.guid.dist,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/duplicate_dist_guid.csv"),
                   obj = dupe.guid.dist,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  if(nrow(dupe.name.dist) > 1) {
    cli::cli_process_start("Outputting districts with duplicate names")
    sf::st_geometry(dupe.name.dist) <- NULL
    if(edav) {
      tidypolis_io(io = "write", edav = T,
                   file_path = paste0(output_folder, "/duplicate_dist_name.csv"),
                   obj = dupe.name.dist,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/duplicate_dist_name.csv"),
                   obj = dupe.name.dist,
                   azcontainer = azcontainer)
    }
    cli::cli_process_done()
  }

  cli::cli_process_done()
  rm(dupe.guid.dist, dupe.name.dist, sf_columns_dist, sf_var_dist)

  #ensure district CRS is 4326
  global.dist.01 <- sf::st_set_crs(global.dist.01, 4326)
  # save global province geodatabase in RDS file:
  if(edav) {
    tidypolis_io(io = "write", edav = T,
                 file_path = paste0(output_folder, "/global.dist.rds"),
                 obj = global.dist.01,
                 azcontainer = azcontainer)
  } else {
    tidypolis_io(io = "write", edav = F,
                 file_path = paste0(output_folder, "/global.dist.rds"),
                 obj = global.dist.01,
                 azcontainer = azcontainer)
  }

  cli::cli_process_done()

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
  cli::cli_process_start("Evaluating overlapping province shapes")

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
                   obj = prov.shape.issue.01,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/prov_shape_multiple_",
                                      paste(min(prov.shape.issue.01$active.year.01, na.rm = T),
                                            max(prov.shape.issue.01$active.year.01, na.rm = T),
                                            sep = "_"), ".csv"),
                   obj = prov.shape.issue.01,
                   azcontainer = azcontainer)
    }
  }

  cli::cli_process_done()

  # District long shape

  df.list <- list()

  for(i in startyr:endyr) {
    df02 <- sirfunctions:::f.yrs.01(global.dist.01, i)

    df.list[[i]] <- df02
  }

  long.global.dist.01 <- do.call(rbind, df.list)

  cli::cli_process_start("Evaluating overlapping district shapes")

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
                   obj = dist.shape.issue.01,
                   azcontainer = azcontainer)
    } else {
      tidypolis_io(io = "write", edav = F,
                   file_path = paste0(output_folder, "/dist_shape_multiple_",
                                      paste(min(dist.shape.issue.01$active.year.01, na.rm = T),
                                            max(dist.shape.issue.01$active.year.01, na.rm = T),
                                            sep = "_"), ".csv"),
                   obj = dist.shape.issue.01,
                   azcontainer = azcontainer)
    }
  }

  cli::cli_process_done()
  remove(df.list, df02)

}

#' add GPEI website reported cases
#' @description
#' a function to add manually extracted GPEI cases to positives file and estimate points
#' based on lowest level admin data
#' @import sirfunctions dplyr tibble sf
#' @param azcontainer Azure validated container object.
#' @param proxy_data_loc str location of proxy_data on EDAV
#' @param polis_pos_loc str location of latest positives dataset generated from POLIS API data
add_gpei_cases <- function(azcontainer = suppressMessages(get_azure_storage_connection()),
                           proxy_data_loc = "/Data/proxy/polio_proxy_data.csv",
                           polis_pos_loc = "/Data/polis/positives_2001-01-01_2025-01-06.rds") {

  long.global.ctry <- sirfunctions::load_clean_ctry_sp(type = "long")
  long.global.ctry$Shape <- NULL
  global.ctry <- sirfunctions::load_clean_ctry_sp()

  long.global.prov <- sirfunctions::load_clean_prov_sp(type = "long")
  long.global.prov$Shape <- NULL
  global.prov <- sirfunctions::load_clean_prov_sp()

  current.polis.pos <- sirfunctions::edav_io(io = "read", file_loc = polis_pos_loc)

  proxy.data <- sirfunctions::edav_io(io = "read", file_loc = proxy_data_loc) |>
    dplyr::filter(!epid %in% current.polis.pos$epid)

  proxy.data.fill.prov <- dplyr::left_join(proxy.data |> dplyr::filter(!is.na(place.admin.1)),
                                           long.global.prov |> dplyr::select(ADM0_NAME, ADM1_NAME, ADM0_GUID, GUID, active.year.01),
                                           by = c("place.admin.0" = "ADM0_NAME", "place.admin.1" = "ADM1_NAME", "yronset" = "active.year.01")) |>
    dplyr::mutate(adm0guid = ADM0_GUID,
                  adm1guid = GUID) |>
    dplyr::select(-c("ADM0_GUID", "GUID"))

  rm(long.global.prov)

  if(nrow(proxy.data.fill.prov) >= 1) {
    #feed only cases with empty coordinates into st_sample (vars = GUID, nperarm, id, Shape)
    proxy.data.fill.prov.01 <- proxy.data.fill.prov |>
      tibble::as_tibble() |>
      dplyr::group_by(adm1guid) |>
      dplyr::summarise(nperarm = dplyr::n()) |>
      dplyr::arrange(adm1guid) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::ungroup() |>
      dplyr::filter(adm1guid != "{NA}")

    proxy.data.fill.prov.02 <- global.prov |>
      dplyr::select(GUID) |>
      dplyr::filter(GUID %in% proxy.data.fill.prov.01$adm1guid) |>
      dplyr::left_join(proxy.data.fill.prov.01, by = c("GUID" = "adm1guid"))

    pt01 <- lapply(1:nrow(proxy.data.fill.prov.02), function(x){

      tryCatch(
        expr = {suppressMessages(sf::st_sample(proxy.data.fill.prov.02[x,], pull(proxy.data.fill.prov.02[x,], "nperarm"),
                                               exact = T)) |> st_as_sf()},
        error = function(e) {
          guid = proxy.data.fill.prov.02[x, ]$GUID[1]
          ctry_prov_dist_name = global.prov |> filter(GUID == adm1guid) |> select(ADM0_NAME, ADM1_NAME)
          cli::cli_alert_warning(paste0("Fixing errors for:\n",
                                        "Country: ", ctry_prov_dist_name$ADM0_NAME,"\n",
                                        "Province: ", ctry_prov_dist_name$ADM1_NAME, "\n"))

          suppressWarnings(
            {
              sf_use_s2(F)
              int <- proxy.data.fill.prov.02[x,] |> st_centroid(of_largest_polygon = T)
              sf_use_s2(T)

              st_buffer(int, dist = 3000) |>
                st_sample(slice(proxy.data.fill.prov.02, x) |>
                            pull(nperarm)) |>
                st_as_sf()
            }
          )

        }
      )

    }) |>
      bind_rows()

    pt01_joined <- dplyr::bind_cols(
      pt01,
      proxy.data.fill.prov.02 |>
        tibble::as_tibble() |>
        dplyr::select(GUID, nperarm) |>
        tidyr::uncount(nperarm)
    ) |>
      dplyr::left_join(tibble::as_tibble(proxy.data.fill.prov.02) |>
                         dplyr::select(-Shape),
                       by = "GUID")

    pt02 <- pt01_joined |>
      tibble::as_tibble() |>
      dplyr::select(-nperarm, -id) |>
      dplyr::group_by(GUID)|>
      dplyr::arrange(GUID, .by_group = TRUE) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      as.data.frame()

    pt03 <- proxy.data.fill.prov |>
      dplyr::group_by(adm1guid) |>
      dplyr::arrange(adm1guid, .by_group = TRUE) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::ungroup()

    pt04 <- dplyr::full_join(pt03, pt02, by = c("adm1guid" = "GUID", "id"))

    proxy.data.prov.final <- pt04 |>
      dplyr::bind_cols(
        tibble::as_tibble(pt04$x),
        sf::st_coordinates(pt04$x) |>
          tibble::as_tibble() |>
          dplyr::rename("lon" = "X", "lat" = "Y")) |>
      dplyr::mutate(longitude = lon,
                    latitude = lat) |>
      dplyr::select(-c("id", "lon", "lat"))

    proxy.data.prov.final$x <- NULL
    proxy.data.prov.final$geometry <- NULL

    rm(pt01, pt01_joined, pt02, pt03, pt04, proxy.data.fill.prov, proxy.data.fill.prov.01, proxy.data.fill.prov.02)
  }

  proxy.data.fill.ctry <- dplyr::left_join(proxy.data |> dplyr::filter(is.na(place.admin.1)),
                                           long.global.ctry |> dplyr::select(ADM0_NAME, GUID, active.year.01),
                                           by = c("place.admin.0" = "ADM0_NAME", "yronset" = "active.year.01")) |>
    dplyr::mutate(adm0guid = GUID) |>
    select(-GUID)

  rm(long.global.ctry)

  if(nrow(proxy.data.fill.ctry) >= 1) {
    proxy.data.fill.ctry.01 <- proxy.data.fill.ctry |>
      tibble::as_tibble() |>
      dplyr::group_by(adm0guid) |>
      dplyr::summarise(nperarm = dplyr::n()) |>
      dplyr::arrange(adm0guid) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::ungroup() |>
      dplyr::filter(adm0guid != "{NA}")

    proxy.data.fill.ctry.02 <- global.ctry |>
      dplyr::select(GUID) |>
      dplyr::filter(GUID %in% proxy.data.fill.ctry.01$adm0guid) |>
      dplyr::left_join(proxy.data.fill.ctry.01, by = c("GUID" = "adm0guid"))

    pt01 <- lapply(1:nrow(proxy.data.fill.ctry.02), function(x){

      tryCatch(
        expr = {suppressMessages(sf::st_sample(proxy.data.fill.ctry.02[x,], pull(proxy.data.fill.ctry.02[x,], "nperarm"),
                                               exact = T)) |> st_as_sf()},
        error = function(e) {
          guid = proxy.data.fill.ctry.02[x, ]$GUID[1]
          ctry_prov_dist_name = global.ctry |> filter(GUID == adm0guid) |> select(ADM0_NAME)
          cli::cli_alert_warning(paste0("Fixing errors for:\n",
                                        "Country: ", ctry_prov_dist_name$ADM0_NAME,"\n"))

          suppressWarnings(
            {
              sf_use_s2(F)
              int <- proxy.data.fill.ctry.02[x,] |> st_centroid(of_largest_polygon = T)
              sf_use_s2(T)

              st_buffer(int, dist = 3000) |>
                st_sample(slice(proxy.data.fill.ctry.02, x) |>
                            pull(nperarm)) |>
                st_as_sf()
            }
          )

        }
      )

    }) |>
      bind_rows()

    pt01_joined <- dplyr::bind_cols(
      pt01,
      proxy.data.fill.ctry.02 |>
        tibble::as_tibble() |>
        dplyr::select(GUID, nperarm) |>
        tidyr::uncount(nperarm)
    ) |>
      dplyr::left_join(tibble::as_tibble(proxy.data.fill.ctry.02) |>
                         dplyr::select(-Shape),
                       by = "GUID")

    pt02 <- pt01_joined |>
      tibble::as_tibble() |>
      dplyr::select(-nperarm, -id) |>
      dplyr::group_by(GUID)|>
      dplyr::arrange(GUID, .by_group = TRUE) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      as.data.frame()

    pt03 <- proxy.data.fill.ctry |>
      dplyr::group_by(adm0guid) |>
      dplyr::arrange(adm0guid, .by_group = TRUE) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::ungroup()

    pt04 <- dplyr::full_join(pt03, pt02, by = c("adm0guid" = "GUID", "id"))

    proxy.data.ctry.final <- pt04 |>
      dplyr::bind_cols(
        tibble::as_tibble(pt04$x),
        sf::st_coordinates(pt04$x) |>
          tibble::as_tibble() |>
          dplyr::rename("lon" = "X", "lat" = "Y")) |>
      dplyr::mutate(longitude = lon,
                    latitude = lat) |>
      dplyr::select(-c("id", "lon", "lat"))

    proxy.data.ctry.final$x <- NULL
    proxy.data.ctry.final$geometry <- NULL

    rm(proxy.data.fill.ctry, proxy.data.fill.ctry.01, proxy.data.fill.ctry.02,
       pt01, pt01_joined, pt02, pt03, pt04)
  }

  if(exists("proxy.data.prov.final") & exists("proxy.data.ctry.final")) {
    proxy.data.final <- rbind(proxy.data.prov.final, proxy.data.ctry.final) |>
      dplyr::mutate(dateonset = as.Date(dateonset, format = "%m/%d/%Y"),
                    report_date = as.Date(report_date, format = "%m/%d/%Y"),
                    latitude = as.character(latitude),
                    longitude = as.character(longitude))

    rm(proxy.data, proxy.data.prov.final, proxy.data.ctry.final)

    positives.new <- current.polis.pos |>
      dplyr::bind_rows(proxy.data.final)
  }

  if(exists("proxy.data.prov.final") & !exists("proxy.data.ctry.final")){
    proxy.data.final <- proxy.data.prov.final |>
      dplyr::mutate(dateonset = as.Date(dateonset, format = "%m/%d/%Y"),
                    report_date = as.Date(report_date, format = "%m/%d/%Y"),
                    latitude = as.character(latitude),
                    longitude = as.character(longitude))

    rm(proxy.data, proxy.data.prov.final)

    positives.new <- current.polis.pos |>
      dplyr::bind_rows(proxy.data.final)
  }

  if(!exists("proxy.data.prov.final") & exists("proxy.data.ctry.final")) {
    proxy.data.final <- proxy.data.ctry.final |>
      dplyr::mutate(dateonset = as.Date(dateonset, format = "%m/%d/%Y"),
                    report_date = as.Date(report_date, format = "%m/%d/%Y"),
                    latitude = as.character(latitude),
                    longitude = as.character(longitude))

    rm(proxy.data, proxy.data.ctry.final)

    positives.new <- current.polis.pos |>
      dplyr::bind_rows(proxy.data.final)
  }

  if(exists("positives.new")){
    sirfunctions::edav_io(obj = positives.new, io = "write", file_loc = paste("/Data/proxy/",
                                                                              paste("positives", min(positives.new$dateonset, na.rm = T),
                                                                                    max(positives.new$dateonset, na.rm = T),
                                                                                    sep = "_"), ".rds", sep = ""))
  }
}

#' Check for missing static files
#'
#' @description
#' Performs a check for the static files.
#'
#' @param core_files_folder_path `str` Path to the `core_files_to_combine` folder.
#' @param edav `bool` Check static files on EDAV?
#'
#' @returns `list` A list of missing files.
#' @keywords internal
#'
check_missing_static_files <- function(core_files_folder_path,
                                       edav_flag = Sys.getenv("POLIS_EDAV_FLAG")) {

  required_files <- c(
    "afp_linelist_2001-01-01_2012-12-31.rds",
    "afp_linelist_2013-01-01_2016-12-31.rds",
    "afp_linelist_2017-01-01_2019-12-31.rds",
    "other_surveillance_type_linelist_2016_2016.rds",
    "other_surveillance_type_linelist_2017_2019.rds",
    "sia_2000_2019.rds"
  )

  core_files_to_combine <- tidypolis_io(io = "list",
                                        file_path = core_files_folder_path,
                                        edav = edav_flag)
  missing_static_files <- setdiff(required_files, core_files_to_combine)

  return(missing_static_files)
}

#Began work on pop processing pipeline but not ready for V1

# Preprocess population data into flat files
#
#  Process POLIS population data using CDC and other standards
#  readr dplyr
# str: "cdc" or "who" (default)
#  tibble: WHO POLIS population file, defaults to tidypolis folder
#  list with tibble for ctry, prov and dist
# process_pop <- function(type = "who", pop_file = readr::read_rds(file.path(Sys.getenv("POLIS_DATA_FOLDER"), "data", "pop.rds"))){
#
#   subset to <= 15
#   pop_file <- pop_file |>
#     filter(AgeGroupName == "0 to 15 years")
#
#   #extract into country prov and dist
#
#   x <- lapply(unique(pop_file$Admin0Name), function(x){
#     pop_file |>
#       filter(is.na(Admin1Name) & is.na(Admin2Name)) |>
#       rename(year = Year, u15pop = Value, GUID = Admin0GUID, ctry = Admin0Name) |>
#       mutate(u15pop = as.integer(u15pop)) |>
#       arrange(year) |>
#       filter(ctry == x) |>
#       group_by(year) |>
#       filter(!is.na(u15pop)) |>
#       filter(UpdatedDate == max(UpdatedDate, na.rm = T)) |>
#       ungroup() |>
#       select(ctry, year, u15pop, GUID) |>
#       full_join(tibble(ctry = x, year = 2000:(lubridate::year(Sys.time()))), by = c("ctry", "year"))
#   }) |>
#     bind_rows()
#
# }

# Private functions ----

###### Step 1 Private Functions ----

#' Prepare the POLIS tables for preprocessing
#'
#' @description
#' Prepares the POLIS tables downloaded via API for further preprocessing.
#'
#' @inheritParams preprocess_cdc
#' @param polis_data_folder `str` Data folder within the POLIS folder.
#' @param long.global.dist.01 `sf` Long global district shapefile.
#' @param ts `str` Time stamp from [Sys.time()].
#' @param timestamp `str` Formatted time stamp.
#' @param who_region str: optional WHO region to filter data
#'      Available inputs include AFRO, AMRO, EMRO, EURO, SEARO and  WPRO.
#'
#' @returns `NULL` quietly upon success.
#' @export
#'
s1_prep_polis_tables <- function(polis_folder, polis_data_folder,
                                 long.global.dist.01, ts, timestamp, who_region) {

  if (!is.null(who_region)) {
    # Set region-specific folder name
    output_folder_name <- paste0("Core_Ready_Files_", who_region)
  }

  # update log for start of creation of CORE ready datasets
  update_polis_log(.event = "Beginning Preprocessing - Creation of CORE Ready Datasets",
                   .event_type = "START")

  crosswalk_data <- get_crosswalk_data(file.path(polis_folder,
                                                 "misc", "crosswalk.rds"))

  cli::cli_h2("Case")
  api_case_data <- s1_clean_case_table(
    path = file.path(polis_data_folder, "case.rds"),
    crosswalk = crosswalk_data)

  if (!is.null(who_region)) {
    api_case_data <- api_case_data |>
      dplyr::filter(`WHO Region` == who_region)
    cli::cli_alert_success(
      paste0("Filtered case data to region: ", who_region))
  } else {
    cli::cli_alert_warning(
      "Could not find WHO region column in case data. No filtering applied.")
  }

  cli::cli_h2("Environmental Samples")
  api_es_data <- s1_clean_es_table(
    path = file.path(polis_data_folder, "environmental_sample.rds"),
    crosswalk = crosswalk_data
  )

  if (!is.null(who_region)) {
    api_es_data <- api_es_data |>
      dplyr::filter(`WHO Region` == who_region)
    cli::cli_alert_success(
      paste0("Filtered ES data to region: ", who_region))
  } else {
    cli::cli_alert_warning(
      "Could not find WHO region column in ES data. No filtering applied.")
  }

  cli::cli_h2("Virus")
  api_virus_data <- s1_clean_virus_table(
    path = file.path(polis_data_folder, "virus.rds"),
    crosswalk = crosswalk_data)

  if (!is.null(who_region)) {
    api_virus_data <- api_virus_data |>
      dplyr::filter(`WHO Region` == who_region)
    cli::cli_alert_success(
      paste0("Filtered Virus data to region: ", who_region))
  } else {
    cli::cli_alert_warning(
      "Could not find WHO region column in Virus data. No filtering applied.")
  }

  cli::cli_h2("Activity")
  api_activity_data <- s1_clean_activity_table(
    path = file.path(polis_data_folder, "activity.rds"),
    subactivity_path = file.path(polis_data_folder, "sub_activity.rds"),
    crosswalk = crosswalk_data
  )

  if (!is.null(who_region)) {
    api_activity_data <- api_activity_data |>
      dplyr::filter(`WHORegion` == who_region)
    cli::cli_alert_success(
      paste0("Filtered Activity data to region: ", who_region))
  } else {
    cli::cli_alert_warning(
      "Could not find WHO region column in Activity data. No filtering applied.")
  }

  cli::cli_h2("Sub-activity")
  api_subactivity_data <- s1_clean_subactivity_table(
    file.path(polis_data_folder, "sub_activity.rds"),
    api_activity_data,
    crosswalk_data,
    long.global.dist.01
  )

  if (!is.null(who_region)) {
    api_subactivity_data <- api_subactivity_data |>
      dplyr::filter(WHORegion == who_region)
    cli::cli_alert_success(
      paste0("Filtered Sub-activity data to region: ", who_region))
  } else {
    cli::cli_alert_warning(
      "Could not find WHO region column in Sub-activity data. No filtering applied.")
  }

  rm(crosswalk_data)

  invisible(capture.output(gc()))

  #13. Export csv files that match the web download, and create archive and change log
  cli::cli_h2("Creating change log and exporting data")

  # create directory for the Archive and Changelogs
  s1_create_core_ready_dir(polis_data_folder, timestamp, output_folder_name)

  # Get list of most recent files
  most_recent_file_patterns <- c(
    "Activity_Data",
    "EnvSamples",
    "Human_Detailed",
    "Viruses_Detailed"
  )
  most_recent_files <- s1_get_most_recent_files(polis_data_folder,
                                                most_recent_file_patterns,
                                                output_folder_name = output_folder_name)

  if (length(most_recent_files) > 0) {
    for (i in most_recent_files) {
      s1_create_change_log(polis_data_folder, i, timestamp,
                           api_case_data,
                           api_es_data,
                           api_virus_data,
                           api_subactivity_data,
                           output_folder_name = output_folder_name)
    }
  } else {
    cli::cli_alert_info("No previous main Core Ready Files found, creating new files")
  }

  invisible(capture.output(gc()))

  # Move files from the current POLIS data folder into the archive
  s1_archive_old_files(polis_data_folder, timestamp,
                       output_folder_name = output_folder_name)

  invisible(capture.output(gc()))

  # Export files (as csv) to be used as pre-processing starting points
  s1_export_final_core_ready_files(
    polis_data_folder = polis_data_folder,
    ts = ts,
    timestamp = timestamp,
    api_case_data = api_case_data,
    api_subactivity_data = api_subactivity_data,
    api_es_data = api_es_data,
    api_virus_data = api_virus_data,
    output_folder_name = output_folder_name)

  rm(api_case_data, api_es_data, api_virus_data, api_activity_data,
     api_subactivity_data)

  invisible(capture.output(gc()))

  update_polis_log(.event = "CORE Ready files and change logs complete", .event_type = "PROCESS")

}

#' Clean Case table
#'
#' @description
#' The function performs cleaning of the Case table. In particular,
#' de-duplication, renaming of variables via crosswalk, and removal of
#' empty columns.
#'
#' @param path `str` File path to the table.
#' @param crosswalk `tibble` The crosswalk table. This is the output of
#' [get_crosswalk_data()].
#' @param edav `bool` Data located on EDAV? Defaults to the
#' EDAV flag.
#'
#' @returns `tibble` cleaned Case data.
#' @keywords internal
#'
s1_clean_case_table <- function(path, crosswalk,
                                edav = Sys.getenv("POLIS_EDAV_FLAG")) {

  cli::cli_process_start("Loading Case table")
  invisible(capture.output(
    api_case_2019_12_01_onward <-
      tidypolis_io(io = "read", file_path = path) |>
      dplyr::mutate_all(as.character)
  ))
  cli::cli_process_done()

  cli::cli_process_start("De-duplicating data")
  api_case_sub1 <- api_case_2019_12_01_onward |>
    dplyr::distinct()
  rm(api_case_2019_12_01_onward)
  cli::cli_process_done()

  cli::cli_process_start("Crosswalk and rename variables")
  api_case_sub1 <- api_case_sub1 |>
    dplyr::rename(DosesTotal = DosesOPVNumber)
  api_case_sub2 <- rename_via_crosswalk(api_data = api_case_sub1,
                                        crosswalk = crosswalk,
                                        table_name = "Case")
  rm(api_case_sub1)
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
  rm(api_case_sub2)
  cli::cli_process_done()

  cli::cli_process_start("Removing empty columns")
  api_case_sub3 <- remove_empty_columns(api_case_sub3)
  cli::cli_process_done()

  return(api_case_sub3)
}

#' Cleans the Environmental Surveillance Table
#'
#' @description
#' The function performs cleaning of the Case table. In particular,
#' de-duplication, renaming of variables via crosswalk, and removal of
#' empty columns.
#'
#' @inheritParams s1_clean_case_table
#'
#' @returns `tibble` Cleaned ES table
#' @keywords internal
#'
s1_clean_es_table <- function(path, crosswalk,
                              edav = Sys.getenv("POLIS_EDAV_FLAG")) {

  cli::cli_process_start("Loading Environmental Samples table")
  invisible(capture.output(
    api_es_complete <-
      tidypolis_io(io = "read", file_path = path) |>
      dplyr::mutate_all(as.character)
  ))
  cli::cli_process_done()

  cli::cli_process_start("De-duplicating data")
  api_es_sub1 <- api_es_complete |>
    dplyr::distinct()
  cli::cli_process_done()

  rm(api_es_complete)

  cli::cli_process_start("Crosswalk and rename variables")
  api_es_sub2 <- rename_via_crosswalk(api_data = api_es_sub1,
                                      crosswalk = crosswalk,
                                      table_name = "EnvSample")
  cli::cli_process_done()

  rm(api_es_sub1)

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

  rm(api_es_sub2)

  cli::cli_process_start("Removing empty columns")
  api_es_sub3 <- remove_empty_columns(api_es_sub3)
  cli::cli_process_done()

  #if nVaccine 2 is removed recreate with empty values so downstream code doesn't break
  es_names <- api_es_sub3 |>
    names()

  if (!"nVaccine 2" %in% es_names) {
    api_es_sub3 <- api_es_sub3 |> cbind("nVaccine 2" = NA)
  }

  return(api_es_sub3)
}

#' Clean the virus table
#'
#' @inheritParams s1_clean_case_table
#'
#' @returns `tibble` Cleaned virus table.
#' @keywords internal
#'
s1_clean_virus_table <- function(path, crosswalk,
                                 edav = Sys.getenv("POLIS_EDAV_FLAG")) {

  cli::cli_process_start("Loading virus data")
  invisible(capture.output(
    api_virus_complete <-
      tidypolis_io(io = "read", file_path = path) |>
      dplyr::mutate_all(as.character)
  ))
  cli::cli_process_done()

  cli::cli_process_start("De-duplicating data")
  api_virus_sub1 <- api_virus_complete |>
    dplyr::distinct()
  cli::cli_process_done()

  rm(api_virus_complete)

  cli::cli_process_start("Crosswalk and rename variables")
  api_virus_sub2 <- rename_via_crosswalk(api_data = api_virus_sub1,
                                         crosswalk = crosswalk,
                                         table_name = "Virus")
  cli::cli_process_done()

  rm(api_virus_sub1)

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

  rm(api_virus_sub2)

  cli::cli_process_start("Removing empty columns")
  api_virus_sub3 <- remove_empty_columns(api_virus_sub3)
  cli::cli_process_done()

  return(api_virus_sub3)

}

#' Clean the Activity table
#'
#' @description
#' The function performs cleaning of the Activity table. In particular,
#' de-duplication, renaming of variables via crosswalk, and removal of
#' empty columns.
#'
#' @inheritParams s1_clean_case_table
#' @param subactivity_path `str` Path to the sub-activity table.
#'
#' @returns `tibble` A cleaned Activity table.
#' @keywords internal
#'
s1_clean_activity_table <- function(path, subactivity_path, crosswalk,
                                    edav = Sys.getenv("POLIS_EDAV_FLAG")) {

  cli::cli_process_start("Loading activity data")
  invisible(capture.output(
    api_activity_complete <-
      tidypolis_io(io = "read", file_path = path) |>
      dplyr::mutate_all(as.character)
  ))

  cli::cli_process_done()

  cli::cli_process_start("Loading subactivity data")
  invisible(capture.output(
    sia_subactivity_code <- tidypolis_io(io = "read",
                                         file_path = subactivity_path) |>
      dplyr::distinct() |>
      dplyr::pull(SIASubActivityCode)
  ))
  cli::cli_process_done()

  cli::cli_process_start("De-duplicating data")
  api_activity_sub1 <- api_activity_complete |>
    dplyr::filter(SIASubActivityCode %in% sia_subactivity_code) |>
    dplyr::distinct()
  cli::cli_process_done()

  rm(api_activity_complete)

  cli::cli_process_start("Crosswalk and rename variables")
  api_activity_sub2 <-
    rename_via_crosswalk(api_data = api_activity_sub1,
                         crosswalk = crosswalk,
                         table_name = "Activity") |>
    dplyr::select(SIASubActivityCode, WHORegion,
                  crosswalk$Web_Name[crosswalk$Table == "Activity"]) |>
    dplyr::select(-c("Admin 0 Id"))
  cli::cli_process_done()

  return(api_activity_sub2)
}

#' Clean the subactivity table
#'
#' @description
#' The function performs cleaning of the sub-activity table. In particular,
#' de-duplication, renaming of variables via crosswalk, and removal of
#' empty columns.
#'
#' @inheritParams s1_clean_case_table
#' @param activity_table `tibble` Output of [clean_activity_table()].
#' @param long_dist_sf `tibble` District shapefile in long format. The output of
#' `sirfunctions::load_clean_dist_sp(type = "long")`.
#'
#' @returns `tibble` Cleaned Sub-activity table.
#' @keywords internal
#'
s1_clean_subactivity_table <- function(path, activity_table, crosswalk,
                                       long_dist_sf,
                                       edav = Sys.getenv("POLIS_EDAV_FLAG")) {

  cli::cli_process_start("Loading sub-activity table")
  invisible(capture.output(
    api_subactivity_complete <-
      tidypolis_io(io = "read", file_path = path) |>
      dplyr::mutate_all(as.character)
  ))
  cli::cli_process_done()

  cli::cli_process_start("De-duplicating data")
  api_subactivity_sub1 <- api_subactivity_complete |>
    dplyr::distinct()
  cli::cli_process_done()

  rm(api_subactivity_complete)

  cli::cli_process_start("Processing sub-activity spatial data")
  api_subactivity_sub2 <- api_subactivity_sub1 |>
    dplyr::mutate(year = lubridate::year(DateFrom)) |>
    dplyr::left_join(
      long_dist_sf |>
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

  rm(api_subactivity_sub1)

  cli::cli_process_start("Crosswalk and rename variables")
  api_subactivity_sub3 <-
    rename_via_crosswalk(api_data = api_subactivity_sub2,
                         crosswalk = crosswalk,
                         table_name = "SubActivity") |>
    dplyr::left_join(activity_table,
                     by = c("SIA Sub-Activity Code" = "SIASubActivityCode"))
  cli::cli_process_done()

  rm(api_subactivity_sub2)

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
                                         crosswalk$API_Name != "ActivityAdminCoveragePercentage"]), WHORegion)
  cli::cli_process_done()

  rm(api_subactivity_sub3)

  cli::cli_process_start("Removing empty columns")
  api_subactivity_sub4 <- remove_empty_columns(api_subactivity_sub4)
  cli::cli_process_done()

  return(api_subactivity_sub4)
}

#' Helper function to create Core Ready Archive and Change Log
#'
#' @param polis_data_folder `str` Location of the POLIS data folder.
#' @param timestamp `str` Folder name as the timestamp.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns NULL
#' @keywords internal
#'
s1_create_core_ready_dir <- function(polis_data_folder, timestamp,
                                     output_folder_name) {

  cli_process_start("Checking on requisite file structure")

  dirs <- c(
    file.path(polis_data_folder, output_folder_name),
    file.path(polis_data_folder, output_folder_name, "Archive"),
    file.path(polis_data_folder, output_folder_name, "Archive", timestamp),
    file.path(polis_data_folder, output_folder_name, "Change Log"),
    file.path(polis_data_folder, output_folder_name, "Change Log", timestamp)
  )

  sapply(dirs, function(x) {
    if (!tidypolis_io(io = "exists.dir", file_path = x)) {
      tidypolis_io(io = "create", file_path = x)
    }
  })

  cli_process_done()

  return(NULL)
}

#' Gets the most recent files in the Core Ready folder
#'
#' @param polis_data_folder `str` Location of the POLIS data folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `list` With names of the most recent files that matches the patterns.
#' @keywords internal
#'
s1_get_most_recent_files <- function(polis_data_folder, patterns,
                                     output_folder_name) {
  files <- tidypolis_io(
    io = "list",
    file_path = file.path(polis_data_folder, output_folder_name)
  )

  files <- files[grepl(paste(patterns, collapse = "|"), files)]

  return(files)
}

#' Create the change log for table data
#'
#' @param polis_data_folder `str` Path to the POLIS data folder
#' @param file `str` Name of the file
#' @param timestamp `str` Name of the timestamp folder
#' @param api_case_data `tibble` Cleaned case data
#' @param api_es_data `tibble` Cleaned ES data
#' @param api_virus_data `tibble` Cleaned virus data
#' @param api_subactivity_data `tibble` Cleaned subactivity data
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `NULL`, if successful
#' @keywords internal
#'
s1_create_change_log <- function(polis_data_folder,
                                 file, timestamp,
                                 api_case_data,
                                 api_es_data,
                                 api_virus_data,
                                 api_subactivity_data,
                                 output_folder_name) {

  cli::cli_process_start(paste0("Processing data for: ", file))
  # compare current dataset to most recent and save summary to change_log
  invisible(capture.output(  old <- tidypolis_io(
    io = "read",
    file_path = file.path(
      polis_data_folder,
      output_folder_name,
      file
    )
  ) |>
    dplyr::mutate_all(as.character)))

  if (grepl("EnvSamples", file)) {
    new <- api_es_data |>
      dplyr::mutate(Id = `Env Sample Manual Edit Id`) |>
      dplyr::mutate_all(as.character)

    old <- old |>
      dplyr::mutate(Id = `Env Sample Manual Edit Id`)
  }

  if (grepl("Viruses", file)) {
    new <- api_virus_data |>
      dplyr::mutate(Id = `Virus ID`) |>
      dplyr::mutate_all(as.character)

    old <- old |>
      dplyr::mutate(Id = `Virus ID`)
  }

  if (grepl("Human_Detailed", file)) {
    new <- api_case_data |>
      dplyr::mutate(Id = `EPID`) |>
      dplyr::mutate_all(as.character)

    old <- old |>
      dplyr::mutate(Id = `EPID`)
  }

  if (grepl("Activity", file)) {
    new <- api_subactivity_data |>
      dplyr::mutate(Id = paste0(`SIA Sub-Activity Code`, "_", `Admin 2 Guid`)) |>
      dplyr::mutate_all(as.character)

    old <- old |>
      dplyr::mutate(Id = paste0(`SIA Sub-Activity Code`, "_", `Admin 2 Guid`))
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
    dplyr::filter(!(Id %in% potential_duplicates_new$Id) &
                    !(Id %in% potential_duplicates_old$Id))

  old <- old |>
    dplyr::filter(!(Id %in% potential_duplicates_new$Id) &
                    !(Id %in% potential_duplicates_old$Id))

  in_new_not_old <- new |>
    filter(!(Id %in% old$Id))

  in_old_not_new <- old |>
    filter(!(Id %in% new$Id))

  in_new_and_old_but_modified <- new |>
    dplyr::filter(Id %in% old$Id) |>
    dplyr::select(-c(setdiff(colnames(new), colnames(old))))

  in_new_and_old_but_modified <- setdiff(in_new_and_old_but_modified, old |>
                                           dplyr::select(-c(setdiff(
                                             colnames(old), colnames(new)
                                           ))))

  x <- old |>
    dplyr::filter(Id %in% new$Id) |>
    dplyr::select(-c(setdiff(colnames(old), colnames(new))))


  if (nrow(in_new_and_old_but_modified) >= 1) {
    in_new_and_old_but_modified <- dplyr::inner_join(in_new_and_old_but_modified,
                                                     setdiff(x, new |>
                                                               dplyr::select(-c(
                                                                 setdiff(colnames(new), colnames(old))
                                                               ))),
                                                     by = "Id"
    ) |>
      # wide_to_long
      tidyr::pivot_longer(cols = -Id) |>
      dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
      dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
      # long_to_wide
      tidyr::pivot_wider(names_from = source, values_from = value) |>
      dplyr::mutate(new = as.character(new), old = as.character(old))

    in_new_and_old_but_modified <- in_new_and_old_but_modified |>
      dplyr::filter(new != old)
  }

  cli::cli_process_done()

  cli_process_start(paste0("Creating change log for: ", file))

  n_added <- nrow(in_new_not_old)
  n_edited <- length(unique(in_new_and_old_but_modified$Id))
  n_deleted <- nrow(in_old_not_new)
  vars_added <- setdiff(colnames(new), colnames(old))
  vars_dropped <- setdiff(colnames(old), colnames(new))

  change_summary <- list(
    n_added = n_added,
    n_edited = n_edited,
    n_deleted = n_deleted,
    vars_added = vars_added,
    vars_dropped = vars_dropped,
    obs_added = in_new_not_old,
    obs_edited = in_new_and_old_but_modified,
    obs_deleted = in_old_not_new
  )

  invisible(capture.output(  tidypolis_io(
    io = "write",
    obj = change_summary,
    file_path = file.path(
      polis_data_folder,
      output_folder_name,
      "Change Log",
      timestamp,
      paste0(substr(file, 1, nchar(file) - 4), ".rds")
    )
  )))

  invisible(capture.output(
    # Move most recent to archive
    tidypolis_io(io = "read", file_path = file.path(polis_data_folder, output_folder_name, file)) |>
      tidypolis_io(
        io = "write",
        file_path = file.path(polis_data_folder, output_folder_name, "Archive", timestamp, file)
      )
  ))

  invisible(capture.output(

    # Delete the original file
    tidypolis_io(io = "delete", file_path = file.path(polis_data_folder, output_folder_name, file))
  ))
  cli_process_done()

  return(NULL)
}

#' Archives the old files in the data folder
#'
#' @param polis_data_folder `str` Path to the POLIS data folder
#' @param timestamp `str` Time stamp folder name
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns NULL
#' @keywords internal
#'
s1_archive_old_files <- function(polis_data_folder, timestamp, output_folder_name) {

  cli_process_start("Archiving old files")
  most_recent_files_01 <- s1_get_most_recent_files(polis_data_folder,
                                                   c(".rds", ".csv", ".xlsx"),
                                                   output_folder_name)

  if (length(most_recent_files_01) > 0) {
    for (file in most_recent_files_01) {
      cli_process_start(paste0("Archiving Data for: ", file))

      invisible(capture.output(
        tidypolis_io(
          io = "read",
          file_path = file.path(polis_data_folder, output_folder_name, file)
        ) %>%
          tidypolis_io(
            io = "write",
            file_path = file.path(polis_data_folder, output_folder_name, "Archive",
                                  timestamp, file)
          )
      ))

      invisible(capture.output(
        tidypolis_io(
          io = "delete",
          file_path = file.path(polis_data_folder, output_folder_name, file)
        )
      ))

      cli_process_done()
    }
  } else {
    cli_alert_info("No previous secondary Core Ready Files found, creating new files")
  }
}

#' Export the final Core Ready files
#'
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param ts `ts` A Sys.time() output.
#' @param timestamp `str` Formatted time stamp.
#' @param api_case_data `tibble` Cleaned Case table.
#' @param api_subactivity_data `tibble` Cleaned Subactivity table.
#' @param api_es_data `tibble` Cleaned ES table.
#' @param api_virus_data `tibble` Cleaned Virus table.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `NULL`
#' @keywords internal
#'
s1_export_final_core_ready_files <- function(polis_data_folder, ts, timestamp,
                                             api_case_data,
                                             api_subactivity_data,
                                             api_es_data,
                                             api_virus_data,
                                             output_folder_name) {

  cli::cli_process_start("Writing all final Core Ready files")

  invisible(capture.output(
    tidypolis_io(
      obj = api_case_data,
      io = "write",
      file_path = file.path(
        polis_data_folder,
        output_folder_name,
        paste0(
          "Human_Detailed_Dataset_",
          timestamp,
          "_from_01_Dec_2019_to_",
          format(ts, "%d_%b_%Y"),
          ".rds"
        )
      )
    )
  ))

  invisible(capture.output(
    tidypolis_io(
      obj = api_subactivity_data,
      io = "write",
      file_path = file.path(
        polis_data_folder,
        output_folder_name,
        paste0(
          "Activity_Data_with_All_Sub-Activities_(1_district_per_row)_",
          timestamp,
          "_from_01_Jan_2020_to_",
          format(Sys.Date() + 365 / 2, "%d_%b_%Y"),
          ".rds"
        )
      )
    )
  ))

  invisible(capture.output(
    tidypolis_io(
      obj = api_es_data,
      io = "write",
      file_path = file.path(
        polis_data_folder,
        output_folder_name,
        paste0(
          "EnvSamples_Detailed_Dataset_",
          timestamp,
          "_from_01_Jan_2000_to_",
          format(ts, "%d_%b_%Y"),
          ".rds"
        )
      )
    )
  ))

  invisible(capture.output(
    tidypolis_io(
      obj = api_virus_data,
      io = "write",
      file.path(
        polis_data_folder,
        output_folder_name,
        paste0(
          "Viruses_Detailed_Dataset_",
          timestamp,
          "_from_01_Dec_1999_to_",
          format(ts, "%d_%b_%Y"),
          ".rds"
        )
      )
    )
  ))

  cli::cli_process_done()
}


###### Step 2 Private Functions ----

#' Process AFP Data Pipeline
#'
#' This function processes AFP data through multiple standardization and
#' validation steps, including checking for duplicates, standardizing dates,
#' classifying cases, and processing coordinates.
#'
#' @param polis_data_folder `str` Path to the POLIS data folder containing
#'   Core_Ready_Files.
#' @param polis_folder `str` Path to the main POLIS folder.
#' @param long.global.dist.01 `sf` Global district lookup table for GUID
#'   validation.
#' @param timestamp `str` Time stamp
#' @param latest_folder_in_archive_path `str` The path to the latest archive folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @export
s2_fully_process_afp_data <- function(polis_data_folder, polis_folder,
                                      long.global.dist.01, timestamp,
                                      latest_folder_in_archive_path,
                                      output_folder_name) {

  if (!tidypolis_io(io = "exists.dir",
                    file_path = file.path(polis_data_folder, output_folder_name))) {
    cli::cli_abort("Please run Step 1 and create a Core Ready folder before running this step.")
  }

  # Step 2a: Read in "old" data file (System to find "Old" data file)

  # list archive files
  x <- tidypolis_io(
    io = "list",
    file_path = file.path(
      polis_data_folder, output_folder_name, "Archive",
      latest_folder_in_archive_path
    ),
    full_names = TRUE
  )

  # load new data files
  y <- tidypolis_io(
    io = "list",
    file_path = file.path(polis_data_folder, output_folder_name),
    full_names = TRUE
  )

  # read and process AFP data
  afp_raw_new <- s2_read_afp_data(file_path = y[grepl("Human", y)])
  invisible(gc())

  # Step 2b: Check for duplicate EPIDs
  has_duplicates <- s2_check_duplicated_epids(
    data = afp_raw_new,
    polis_data_folder = polis_data_folder, output_folder_name = output_folder_name)

  if (has_duplicates) {
    cli::cli_alert_warning("Please review duplicates!")
  }

  # Step 2c: Standardize dates and variable names
  afp_standardized <- s2_standardize_dates(data = afp_raw_new)

  # Step 2d: Write out epids with no dateonset
  s2_export_missing_onsets(
    data = afp_standardized,
    polis_data_folder = polis_data_folder, output_folder_name = output_folder_name)

  # Step 2e: Check for missingness
  s2_check_missingness(
    data = afp_standardized,
    type = "AFP",
    polis_data_folder,
    output_folder_name)

  # Step 2f: Classify cases
  afp_classified <- s2_classify_afp_cases(
    data = afp_standardized,
    startyr = 2020)

  # Step 2g: Validate classifications
  afp_validated <- s2_validate_classifications(data = afp_classified)
  invisible(gc())

  # Step 2h: Validate and fix GUIDs
  afp_with_guids <- s2_fix_admin_guids(data = afp_validated,
                                       shape_data = long.global.dist.01)

  invisible(gc())

  # Step 2i: Process coordinates
  afp_processed <- s2_process_coordinates(
    data = afp_with_guids,
    polis_data_folder = polis_data_folder,
    polis_folder = polis_folder,
    output_folder_name = output_folder_name
  )

  # Step 2j: Create key AFP variables
  afp_final <- s2_create_afp_variables(data = afp_processed)

  # Step 2k: Compare archives and generate outputs
  s2_export_afp_outputs(
    data = afp_final,
    latest_archive = latest_folder_in_archive_path,
    polis_data_folder = polis_data_folder,
    col_afp_raw = colnames(afp_raw_new),
    output_folder_name = output_folder_name
  )

}


#' Find Latest Archive in Core Ready Files
#'
#' @description
#' This function searches for and returns the name of the most recent archive
#' folder in the Core Ready Files directory. If no archive is found, it returns
#' the current timestamp based on prespecfied time format.
#'
#' @param polis_data_folder `str` specifying the path to the main
#' data folder containing Core Ready Files
#' @param timestamp `str` Time stamp
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return `str` containing either the name of the most recent
#'   archive folder or the current timestamp if no archives exist
#'
#' @details
#' The function performs the following steps:
#' 1. Lists contents of the Archive folder in Core Ready Files
#' 2. Converts folder names to datetime objects
#' 3. If archives exist, returns name of the most recent one
#' 4. If no archives exist, returns current timestamp
#'
#' @examples
#' \dontrun{
#' latest_archive <- find_latest_archive("path/to/polis/data")
#' }
#' @keywords internal
s2_find_latest_archive <- function(polis_data_folder, timestamp, output_folder_name) {
  latest_folder_in_archive <- tidypolis_io(
    io = "list",
    file_path = paste0(polis_data_folder, "/", output_folder_name, "/Archive")
  ) |>
    dplyr::tibble() |>
    dplyr::rename(name = 1) |>
    dplyr::mutate(date_time = lubridate::as_datetime(name))

  if (nrow(latest_folder_in_archive) > 0) {
    cli::cli_alert_info("Previous archive found!")

    latest_folder_in_archive <- latest_folder_in_archive |>
      dplyr::filter(date_time == max(date_time)) |>
      dplyr::pull(name)
  } else {
    cli::cli_alert_info(
      "No previous archive identified, will not perform any comparisons"
    )

    latest_folder_in_archive <- timestamp
  }

  invisible(gc(full = TRUE))
  return(latest_folder_in_archive)
}

#' Read and Process AFP Data
#'
#' @description Internal function to read AFP data from a file and process it
#'    by:
#'      1. Converting all columns to character
#'      2. Replacing spaces with dots in column names
#'      3. Converting empty strings to NA
#'      4. Converting column names to lowercase
#'
#' @param file_path `str` specifying path to AFP data file
#' @return `tibble` containing processed AFP data
#' @keywords internal
s2_read_afp_data <- function(file_path) {
  cli::cli_process_start("Loading human dataset",
                         msg_done = "Loaded human dataset"
  )

  invisible(capture.output(
    afp_raw <- tidypolis_io(io = "read", file_path = file_path) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~ dplyr::na_if(., "")))
  ))

  names(afp_raw) <- stringr::str_to_lower(names(afp_raw))
  cli::cli_process_done()

  invisible(gc(full = TRUE))
  return(afp_raw)
}

#' Check for Duplicated EPIDs in AFP Data
#'
#' This function checks for duplicate EPID entries in AFP surveillance data and
#' exports duplicates to a CSV file if found.
#'
#' @param data A dataframe containing AFP surveillance data
#' @param polis_data_folder String path to the POLIS data folder
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return Logical. TRUE if duplicates found, FALSE if no duplicates
#'
#' @details
#' The function:
#' 1. Processes dates and extracts year of onset
#' 2. Groups by EPID, onset date and admin level 0 location
#' 3. Identifies records with 2+ entries for same EPID
#' 4. Exports duplicates to CSV if found
#' 5. Logs the duplicate finding in POLIS system
#'
#' @examples
#' \dontrun{
#' s2_check_duplicated_epids(afp_data, "path/to/polis/folder")
#' }
s2_check_duplicated_epids <- function(data, polis_data_folder, output_folder_name) {
  cli::cli_process_start("Checking for duplicated EPIDs")

  afp_dup <- data |>
    dplyr::mutate(
      dateonset = as.Date(date.onset, format = "%Y-%m-%d"),
      yronset = lubridate::year(dateonset)
    ) |>
    dplyr::select(
      epid, date.onset, yronset, place.admin.0, yronset,
      `virus.type(s)`, classification, stool.1.collection.date,
      stool.2.collection.date, stool.1.condition, stool.2.condition
    ) |>
    dplyr::group_by(epid, date.onset, place.admin.0) |>
    dplyr::mutate(afp_dup = dplyr::n()) |>
    dplyr::filter(afp_dup >= 2) |>
    dplyr::select(
      epid, date.onset, place.admin.0, yronset, `virus.type(s)`,
      classification, stool.1.collection.date, stool.2.collection.date,
      stool.1.condition, stool.2.condition
    )

  if (nrow(afp_dup) >= 1) {
    # Export duplicate afp cases in the CSV file:
    invisible(capture.output(
      tidypolis_io(
        io = "write",
        obj = afp_dup,
        paste0(
          polis_data_folder,
          "/", output_folder_name, "/",
          paste(
            "duplicate_AFPcases_Polis",
            min(afp_dup$yronset, na.rm = TRUE),
            max(afp_dup$yronset, na.rm = TRUE),
            sep = "_"
          ),
          ".csv"
        )
      )
    ))

    cli::cli_alert_info(
      paste0(
        "Duplicate AFP case. Check the data for duplicate records located at:\n",
        paste0(
          polis_data_folder,
          "/", output_folder_name, "/",
          paste(
            "duplicate_AFPcases_Polis",
            min(afp_dup$yronset, na.rm = TRUE),
            max(afp_dup$yronset, na.rm = TRUE),
            sep = "_"
          ),
          ".csv"
        ), "\n",
        "If they are exact same, then contact POLIS"
      )
    )

    update_polis_log(
      .event = paste0(
        "Duplicate AFP cases output in ",
        "duplicate_AFPcases_Polis within ", output_folder_name,
      ),
      .event_type = "ALERT"
    )

    cli::cli_process_done(msg_done = "Found duplicate EPIDs")
    invisible(gc(full = TRUE))
    return(TRUE)
  } else {
    cli::cli_process_done(
      msg_done = "No duplicate EPIDs found, okay to proceed"
    )
    invisible(gc(full = TRUE))
    return(FALSE)
  }
}

#' Standardize dates and variables in AFP dataset
#'
#' @description
#' This function processes a raw AFP dataset by standardizing date formats,
#' renaming variables for consistency, and calculating derived date variables.
#'
#' @param data A data frame containing raw AFP surveillance data
#'
#' @return A processed data frame with standardized dates and derived variables:
#'   - All dates converted to ymd format
#'   - Renamed variables for consistency
#'   - Calculated intervals between key dates
#'   - Age in months
#'   - Cleaned poliovirus types
#'
#' @details
#' The function performs the following transformations:
#' - Renames variables to remove special characters and spaces
#' - Converts date strings to ymd format using lubridate
#' - Calculates year of onset and age in months
#' - Computes intervals between key dates (onset, notification,
#'      investigation, stool collection)
#' - Cleans poliovirus type strings
#' - Standardizes additional date fields with consistent naming
#'
#' @importFrom lubridate ymd year
#' @importFrom dplyr rename mutate across all_of
#' @importFrom stringr str_replace_all
#' @importFrom cli cli_process_start cli_process_done
s2_standardize_dates <- function(data) {
  cli::cli_process_start(
    "Fixing all dates from character to ymd format and fixing character variables",
    msg_done = "Fixed dates and character variables"
  )

  data_processed <- data |>
    dplyr::rename(
      poliovirustypes = `virus.type(s)`,
      classificationvdpv = `vdpv.classification(s)`,
      surveillancetypename = surveillance.type,
      whoregion = who.region,
      admin0guid = admin.0.guid,
      admin1guid = admin.1.guid,
      admin2guid = admin.2.guid
    ) |>
    dplyr::mutate(
      dateonset = lubridate::ymd(
        as.Date(date.onset, "%Y-%m-%dT%H:%M:%S"),
        quiet = TRUE
      ),
      datenotify = lubridate::ymd(
        as.Date(notification.date, "%Y-%m-%dT%H:%M:%S"),
        quiet = TRUE
      ),
      dateinvest = lubridate::ymd(
        as.Date(investigation.date, "%Y-%m-%dT%H:%M:%S"),
        quiet = TRUE
      ),
      datestool1 = lubridate::ymd(
        as.Date(stool.1.collection.date, "%Y-%m-%dT%H:%M:%S"),
        quiet = TRUE
      ),
      datestool2 = lubridate::ymd(
        as.Date(stool.2.collection.date, "%Y-%m-%dT%H:%M:%S"),
        quiet = TRUE
      ),
      followup.date = lubridate::ymd(
        as.Date(followup.date, "%Y-%m-%dT%H:%M:%S"),
        quiet = TRUE
      ),
      yronset = lubridate::year(dateonset),
      yronset = dplyr::if_else(is.na(yronset),
                               lubridate::year(datestool1), yronset
      ),
      yronset = dplyr::if_else(is.na(datestool1) & is.na(yronset),
                               lubridate::year(datenotify), yronset
      ),
      age.months = as.numeric(`calculated.age.(months)`),
      ontostool1 = as.numeric(datestool1 - dateonset),
      ontostool2 = as.numeric(datestool2 - dateonset),
      ontonot = as.numeric(datenotify - dateonset),
      ontoinvest = as.numeric(dateinvest - dateonset),
      nottoinvest = as.numeric(dateinvest - datenotify),
      investtostool1 = as.numeric(datestool1 - dateinvest),
      stool1tostool2 = as.numeric(datestool2 - datestool1),
      poliovirustypes = stringr::str_replace_all(poliovirustypes, "  ", " ")
    ) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::all_of(c(
          "date.notification.to.hq", "results.seq.date.to.program",
          "specimen.date", "stool.date.sent.to.ic.lab",
          "case.date", "stool.date.sent.to.lab",
          "clinical.admitted.date", "followup.date"
        )),
        ~ lubridate::ymd(as.Date(., "%Y-%m-%dT%H:%M:%S"), quiet = TRUE)
      )
    ) |>
    dplyr::mutate(datenotificationtohq = date.notification.to.hq,
                  casedate = case.date,
                  stooltolabdate = stool.date.sent.to.lab,
                  stooltoiclabdate = stool.date.sent.to.ic.lab,
                  clinicadmitdate = clinical.admitted.date)

  cli::cli_process_done()

  invisible(gc(full = TRUE))

  return(data_processed)
}

#' Export EPIDs with missing onset dates
#'
#' Identifies AFP cases with missing date of onset and exports them to a CSV
#' file for further review and investigation.
#'
#' @param data `tibble` A tibble containing AFP surveillance data
#' @param polis_data_folder `str` String path to the POLIS data folder
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return Invisibly returns the filtered data frame of records with missing
#'    onset dates
#' @export
s2_export_missing_onsets <- function(data, polis_data_folder, output_folder_name) {
  missing_onsets <- data |>
    dplyr::select(epid, dateonset) |>
    dplyr::filter(is.na(dateonset))

  invisible(capture.output(
    tidypolis_io(
      io = "write",
      obj = missing_onsets,
      file_path = paste0(polis_data_folder, "/",
                         output_folder_name, "/afp_no_onset.csv")
    )
  ))

  cli::cli_alert_info(paste0(
    "Exported ", nrow(missing_onsets), " records with missing onset dates"
  ))

  invisible(gc(full = TRUE))

  invisible(missing_onsets)
}

#' Check missingness in key AFP variables
#'
#' Analyzes and exports information about missingness in key variables that are
#' critical for AFP surveillance. This helps with data quality assessment.
#'
#' @param data A data frame containing AFP surveillance data
#' @param type String indicating the type of data (e.g., "AFP")
#' @param polis_data_folder String path to the POLIS data folder
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return Invisibly returns the missingness summary
#' @export
s2_check_missingness <- function(data, type = "AFP", polis_data_folder,
                                 output_folder_name) {
  cli::cli_process_start("Checking for missingness in key variables")

  # Call the existing check_missingness function
  result <- check_missingness(data = data, type = type,
                              output_folder_name = output_folder_name)

  cli::cli_process_done(
    paste0(
      "Check ",
      tolower(type), "_missingness.rds for missing key variables"
    )
  )

  invisible(gc(full = TRUE))

  invisible(result)
}

#' Classify AFP Cases Based on Lab Data
#'
#' This function takes AFP surveillance data and classifies cases based on
#' poliovirus types and CDC classification criteria.
#'
#' @param data `tibble` A tibble containing AFP surveillance data with columns:
#'   poliovirustypes, vtype.fixed, classification, yronset
#' @param startyr `int` The start year for filtering cases (default: 2020)
#' @param endyr `int` The end year for filtering cases (default: current
#'    year)
#'
#' @return A data frame with additional classification columns:
#'   - vtype: Classified poliovirus type
#'   - cdc.classification.all: CDC case classification
#'
#' @details
#' The function performs the following classifications:
#' - Wild poliovirus types (1, 2, 3 and combinations)
#' - Vaccine-derived poliovirus (VDPV) types (1, 2, 3 and combinations)
#' - CDC classifications including compatible cases
#'
#' @examples
#' \dontrun{
#' classified_data <- s2_classify_afp_cases(afp_data, 2020, 2023)
#' }
s2_classify_afp_cases <- function(data, startyr = 2020,
                                  endyr = lubridate::year(Sys.Date())) {
  cli::cli_process_start("Classification of cases using lab data",
                         msg_done = "Classification of cases completed"
  )

  classified_data <- data |>
    dplyr::mutate(
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "WILD1"), "WILD 1", "none"
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "WILD2"), "WILD 2", vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "WILD3"), "WILD 3", vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "WILD1, WILD3"),
        "WILD1andWILD3",
        vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV1"), "VDPV 1", vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV2"), "VDPV 2", vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV3"), "VDPV 3", vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV1") &
          stringr::str_detect(poliovirustypes, "VDPV2"),
        "VDPV1andVDPV2",
        vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV 1") &
          stringr::str_detect(poliovirustypes, "VDPV3"),
        "VDPV1andVDPV3",
        vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV2") &
          stringr::str_detect(poliovirustypes, "VDPV3"),
        "VDPV2andVDPV3",
        vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "VDPV1") &
          stringr::str_detect(poliovirustypes, "VDPV2") &
          stringr::str_detect(poliovirustypes, "VDPV3"),
        "VDPV12and3",
        vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(poliovirustypes, "WILD") &
          stringr::str_detect(poliovirustypes, "VDPV"),
        paste0("CombinationWild1-", vtype),
        vtype
      ),

      # now using classificationvdpv to add in c, i, and a
      # this would make combination isolation of multiple vdpv types as c/a/i
      # for the first listed virus type. this is not accurate as we dont know
      # which type is c/a/i

      classificationvdpv = dplyr::if_else(
        is.na(classificationvdpv), "", classificationvdpv
      ),
      vtype = dplyr::if_else(
        classificationvdpv == "Ambiguous" & !is.na(vtype),
        paste0("a", vtype),
        vtype
      ),
      vtype = dplyr::if_else(
        stringr::str_detect(classificationvdpv, "Circulating") & !is.na(vtype),
        paste0("c", vtype),
        vtype
      ),
      vtype = dplyr::if_else(
        classificationvdpv == "Immune Deficient" & !is.na(vtype),
        paste0("i", vtype),
        vtype
      ),

      # cleaning up cnone =none, and inone= none and may need additional fixes
      # if more weird things happen

      vtype = dplyr::if_else(vtype == "cnone", "none", vtype),
      vtype = dplyr::if_else(vtype == "inone", "none", vtype),
      vtype = dplyr::if_else(vtype == "anone", "none", vtype),
      vtype = dplyr::if_else(
        vtype == "cCombinationWild1-VDPV 2",
        "CombinationWild1-cVDPV 2",
        vtype
      ),
      vtype = dplyr::if_else(
        vtype == "cCombinationWild1-VDPV 3",
        "CombinationWild1-cVDPV 3",
        vtype
      ),
      vtype = dplyr::if_else(
        vtype == "cCombinationWild1-VDPV 1",
        "CombinationWild1-cVDPV 1",
        vtype
      ),
      vtype = dplyr::if_else(
        vtype == "cVDPV2andVDPV3", "cVDPV2andcVDPV3", vtype
      ),

      # creating vtype.fixed that hard codes a fix for Congo 2010 WPV1 cases
      # that were not tested by lab

      vtype.fixed = dplyr::if_else(
        (classification == "Confirmed (wild)" &
           place.admin.0 == "CONGO" &
           yronset == "2010"),
        "WILD 1",
        vtype
      ),

      # hard coding a fix for a Nigeria case that is WPV1 but
      # missing from the lab data
      vtype.fixed = dplyr::if_else(
        (is.na(vtype) == T &
           place.admin.0 == "NIGERIA" &
           yronset == "2011" &
           classification == "Confirmed (wild)"),
        "WILD 1",
        vtype.fixed
      ),

      # NEW hard coding to deal with WPV1 cases from before 2010
      # that have no lab data assuming these are WPV 1

      vtype.fixed = dplyr::if_else(
        (is.na(vtype) == T &
           yronset < "2010" &
           classification == "Confirmed (wild)"),
        "WILD 1",
        vtype.fixed
      ),

      # note POLIS data undercounts the total WPV1 cases in 2011,
      # in 2012 we have one extra WPV3 and one less WPV1

      # hard fix for Yemen case (YEM-SAD-2021-457-33) where classified as
      # vdpv1andvdpv2 instead of cvdpv2
      vtype.fixed = dplyr::if_else(
        vtype == "cVDPV1andVDPV2" &
          stringr::str_detect(poliovirustypes, "cVDPV2"),
        "VDPV1andcVDPV2",
        vtype.fixed
      ),

      # further fixing classification for cases with multiple vdpvs
      vtype.fixed = dplyr::if_else(
        vtype == "cVDPV1andVDPV2" &
          (stringr::str_detect(poliovirustypes, "cVDPV2") &
             stringr::str_detect(poliovirustypes, "cVDPV1")),
        "cVDPV1andcVDPV2",
        vtype.fixed
      ),

      # now creating cdc.classification.all categories includes non-AFP, NPAFP,
      # all vtypes and polio compatibles, pending, etc
      # CDC.classification is based first on lab, then on epi data.
      # If the epi data and lab data disagree, we default to lab classification

      cdc.classification.all = vtype.fixed,
      cdc.classification.all = dplyr::if_else(
        (vtype.fixed == "none" | is.na(vtype.fixed)) &
          classification == "Compatible",
        "COMPATIBLE",
        cdc.classification.all
      ),
      cdc.classification.all = dplyr::if_else(
        (vtype.fixed == "none" | is.na(vtype.fixed)) &
          classification == "Discarded",
        "NPAFP",
        cdc.classification.all
      ),
      cdc.classification.all = dplyr::if_else(
        (vtype.fixed == "none" | is.na(vtype.fixed)) &
          classification == "Not an AFP",
        "NOT-AFP",
        cdc.classification.all
      ),
      cdc.classification.all = dplyr::if_else(
        (vtype.fixed == "none" | is.na(vtype.fixed)) &
          classification == "Pending",
        "PENDING",
        cdc.classification.all
      ),
      cdc.classification.all = dplyr::if_else(
        (vtype.fixed == "none" | is.na(vtype.fixed)) &
          classification == "VAPP",
        "VAPP",
        cdc.classification.all
      ),
      cdc.classification.all = dplyr::if_else(
        (vtype.fixed == "none" | is.na(vtype.fixed)) &
          (classification == "Not Applicable" |
             classification == "Others" |
             classification == "VDPV"),
        "UNKNOWN",
        cdc.classification.all
      ),
      cdc.classification.all = dplyr::if_else(
        final.cell.culture.result == "Not received in lab" &
          cdc.classification.all == "PENDING",
        "LAB PENDING",
        cdc.classification.all
      ),
      hot.case = dplyr::if_else(
        paralysis.asymmetric == "Yes" &
          paralysis.onset.fever == "Yes" &
          paralysis.rapid.progress == "Yes",
        1,
        0
      ),
      hot.case = dplyr::if_else(is.na(hot.case), 99, hot.case),

      # now dealing with Sabin viruses- creating indicators for detection of
      # Sabin1, Sabin2 and Sabin3
      # note an AFP case can by NPAFP AND have Sabin virus isolated
      sabin1 = dplyr::if_else(stringr::str_detect(poliovirustypes, "VACCINE1"),
                              1, 0
      ),
      sabin2 = dplyr::if_else(stringr::str_detect(poliovirustypes, "VACCINE2"),
                              1, 0
      ),
      sabin3 = dplyr::if_else(stringr::str_detect(poliovirustypes, "VACCINE3"),
                              1, 0
      )
    ) |>
    dplyr::mutate(
      place.admin.0 = dplyr::if_else(
        stringr::str_detect(place.admin.0, "IVOIRE"),
        "COTE D IVOIRE",
        place.admin.0
      )
    ) |>
    dplyr::distinct() |>
    dplyr::mutate(
      yronset = dplyr::if_else(
        is.na(yronset) == T,
        lubridate::year(datestool1),
        yronset
      ),
      yronset = dplyr::if_else(
        is.na(datestool1) == T & is.na(yronset) == T,
        lubridate::year(datenotify),
        yronset
      )
    ) |> # adding year onset correction for nonAFP
    dplyr::filter(dplyr::between(yronset, startyr, endyr))

  cli::cli_process_done()
  return(classified_data)
}

#' Validate case classifications in AFP data
#'
#' @description
#' Checks the data for invalid or unclassified cases ('none' classification).
#' If found, flags them for investigation and stops processing unless the EPIDs
#' are in a predefined list of cases that have been manually reviewed.
#'
#' @param data `tibble` A tibble containing AFP data with classification columns
#'
#' @return The filtered tibble with invalid classifications removed
#'
#' @details
#' The function checks for the following issues:
#' 1. Cases with 'none' cdc.classification.all
#' 2. Specific cases with complex virus types that may need special handling
#' 3. Cases with unusual classification combinations
#'
#' Known issues are filtered against a predefined whitelist
#'
#' @export
s2_validate_classifications <- function(data) {
  cli::cli_process_start(
    paste0(
      "Verifying that classifications in data line up with ",
      "poliovirustypes and classification"
    )
  )

  # Define types and classifications to check
  vtype_tocheck <- c(
    "VDPV1andVDPV2", "VDPV1andVDPV3", "VDPV2andVDPV3", "VDPV12and3",
    "cVDPV1andVDPV2"
  )
  virus_tocheck <- c("cVDPV1, VACCINE1, VDPV1")
  classification_tocheck <- c("Circulating, Pending")
  cdc_classification_tocheck <- c("none")

  # Extract cases requiring manual review
  to_check <- data |>
    dplyr::filter(vtype %in% vtype_tocheck |
                    cdc.classification.all %in% cdc_classification_tocheck |
                    poliovirustypes %in% virus_tocheck |
                    classificationvdpv %in% classification_tocheck |
                    is.na(cdc.classification.all)) |>
    dplyr::select(
      epid, poliovirustypes, classification, classificationvdpv,
      nt.changes, vtype, vtype.fixed, cdc.classification.all
    )

  # Check specifically for 'none' classifications
  if (nrow(to_check |> dplyr::filter(cdc.classification.all == "none")) > 0) {
    epids <- to_check |>
      dplyr::filter(cdc.classification.all == "none") |>
      dplyr::pull(epid)

    # List of cases already flagged to POLIS that can be skipped
    flagged_to_polis <- c("MOZ-TET-TSA-22-006")

    # Remove known cases from consideration
    epids <- epids[!epids %in% flagged_to_polis]

    # If unknown "none" classifications remain, raise an error
    if (length(epids) > 0) {
      cli::cli_alert_danger(paste0(
        "There is a 'none' classification, flag for POLIS ",
        "and get guidance on proper classification"
      ))

      update_polis_log(
        .event = "NONE classification found, must be manually addressed",
        .event_type = "ERROR"
      )

      stop("Found 'none' classifications that must be addressed manually")
    }
  }

  cli::cli_process_done()

  # Filter out any remaining "none" classifications
  data_filtered <- data |>
    dplyr::filter(cdc.classification.all != "none") |>
    dplyr::distinct()

  invisible(gc(full = TRUE))

  return(data_filtered)
}

#' Fix administrative GUIDs in AFP data
#'
#' Corrects incorrect or missing GUIDs for administrative boundaries by
#' matching on administrative names when possible. This improves the spatial
#' accuracy of AFP data.
#'
#' @param data `tibble` A data frame containing AFP surveillance data with admin GUIDs
#' @param shape_data `sp` Spatial data containing administrative boundary info
#'
#' @return A data frame with corrected admin GUIDs and added validation columns
#' @export
s2_fix_admin_guids <- function(data, shape_data) {
  cli::cli_process_start("Checking and fixing GUIDs",
                         msg_done = "Checked and fixed GUIDs"
  )

  # Format GUIDs with proper brackets
  data_with_guids <- data |>
    dplyr::mutate(
      Admin2GUID = paste0("{", toupper(admin2guid), "}"),
      Admin1GUID = paste0("{", toupper(admin1guid), "}"),
      Admin0GUID = paste0("{", toupper(admin0guid), "}")
    )

  # Extract country level data
  shapes_country <- shape_data |>
    tibble::as_tibble() |>
    dplyr::select(ADM0_GUID, active.year.01) |>
    dplyr::distinct()

  # Add match indicator
  shapes_country$match <- 1

  # Check country GUIDs
  data_fixed_country <- dplyr::left_join(
    data_with_guids, shapes_country,
    by = c(
      "Admin0GUID" = "ADM0_GUID",
      "yronset" = "active.year.01"
    )
  ) |>
    dplyr::mutate(
      wrongAdmin0GUID = dplyr::if_else(is.na(match) & !is.na(admin0guid),
                                       "yes", "no"
      )
    ) |>
    dplyr::select(-match)

  # Extract province level data
  shapes_prov <- shape_data |>
    tibble::as_tibble() |>
    dplyr::select(ADM0_GUID, ADM1_GUID, active.year.01) |>
    dplyr::distinct()

  # Extract province names
  shapenames_prov <- shape_data |>
    tibble::as_tibble() |>
    dplyr::filter(
      !(ADM0_NAME == "SUDAN" & yr.st == 2000 & active.year.01 == 2011)
    ) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, ADM1_GUID, active.year.01) |>
    dplyr::distinct()

  # Add match indicators
  shapes_prov$match <- 1
  shapenames_prov$match01 <- 1

  # Fix province GUIDs
  data_fixed_prov <- dplyr::left_join(
    data_fixed_country, shapes_prov,
    by = c(
      "Admin0GUID" = "ADM0_GUID",
      "Admin1GUID" = "ADM1_GUID",
      "yronset" = "active.year.01"
    )
  ) |>
    dplyr::mutate(
      wrongAdmin1GUID = dplyr::if_else(is.na(match) & !is.na(admin1guid),
                                       "yes", "no"
      )
    )

  # Match by name and fix incorrect Admin1 GUIDs
  data_fixed_prov <- data_fixed_prov |>
    dplyr::left_join(shapenames_prov,
                     by = c(
                       "place.admin.0" = "ADM0_NAME",
                       "place.admin.1" = "ADM1_NAME",
                       "yronset" = "active.year.01"
                     ),
                     relationship = "many-to-many"
    ) |>
    dplyr::mutate(Admin1GUID = dplyr::if_else(
      wrongAdmin1GUID == "yes" & !is.na(match01),
      ADM1_GUID, Admin1GUID
    )) |>
    dplyr::select(-match, -match01)

  # Extract district level data
  shapes_dist <- shape_data |>
    tibble::as_tibble() |>
    dplyr::select(ADM0_GUID, ADM1_GUID, GUID, active.year.01) |>
    dplyr::distinct()

  # Extract district names
  shapenames_dist <- shape_data |>
    tibble::as_tibble() |>
    dplyr::filter(
      !(ADM0_NAME == "SUDAN" & yr.st == 2000 & active.year.01 == 2011)
    ) |>
    dplyr::select(ADM0_NAME, ADM1_NAME, ADM2_NAME, GUID, active.year.01) |>
    dplyr::distinct()

  # Add match indicators
  shapes_dist$match <- 1
  shapenames_dist$match01 <- 1

  # Fix district GUIDs
  data_fixed_dist <- dplyr::left_join(
    data_fixed_prov, shapes_dist,
    by = c(
      "Admin0GUID" = "ADM0_GUID",
      "Admin1GUID" = "ADM1_GUID",
      "Admin2GUID" = "GUID",
      "yronset" = "active.year.01"
    )
  ) |>
    dplyr::mutate(wrongAdmin2GUID = dplyr::if_else(
      is.na(match) & !is.na(admin2guid),
      "yes", "no"
    ))

  # Match by name and fix incorrect Admin2 GUIDs
  data_fixed_dist <- data_fixed_dist |>
    dplyr::left_join(shapenames_dist,
                     by = c(
                       "place.admin.0" = "ADM0_NAME",
                       "place.admin.1" = "ADM1_NAME",
                       "place.admin.2" = "ADM2_NAME",
                       "yronset" = "active.year.01"
                     ),
                     relationship = "many-to-many"
    ) |>
    dplyr::mutate(Admin2GUID = dplyr::if_else(
      wrongAdmin2GUID == "yes" & !is.na(match01),
      GUID, Admin2GUID
    )) |>
    dplyr::select(-match, -match01, -ADM1_GUID, -GUID)

  # Generate summary statistics on GUID issues
  issues_by_year <- data_fixed_dist |>
    dplyr::group_by(yronset) |>
    dplyr::summarise(
      numNAprovince = sum(is.na(place.admin.1)),
      numNAdist = sum(is.na(place.admin.2)),
      wrongCountryGUID = sum(wrongAdmin0GUID == "yes"),
      wrongProvGUID = sum(wrongAdmin1GUID == "yes"),
      wrongdistGUID = sum(wrongAdmin2GUID == "yes"),
      wrongbothGUID = sum(wrongAdmin1GUID == "yes" & wrongAdmin2GUID == "yes"),
      .groups = "drop"
    )

  # Also generate country-level issues summary as in the original code
  issues_by_country <- data_fixed_dist |>
    dplyr::filter(is.na(place.admin.1) | is.na(place.admin.2) |
                    wrongAdmin0GUID == "yes" | wrongAdmin1GUID == "yes" |
                    wrongAdmin2GUID == "yes") |>
    dplyr::group_by(place.admin.0) |>
    dplyr::summarise(
      numNAprovince = sum(is.na(place.admin.1)),
      numNAdist = sum(is.na(place.admin.2)),
      wrongCountryGUID = sum(wrongAdmin0GUID == "yes"),
      wrongProvGUID = sum(wrongAdmin1GUID == "yes"),
      wrongdistGUID = sum(wrongAdmin2GUID == "yes"),
      wrongbothGUID = sum(wrongAdmin1GUID == "yes" & wrongAdmin2GUID == "yes"),
      .groups = "drop"
    )

  # Add the summaries as attributes to the returned data
  attr(data_fixed_dist, "issues_by_year") <- issues_by_year
  attr(data_fixed_dist, "issues_by_country") <- issues_by_country

  cli::cli_process_done()

  gc(full = TRUE)

  return(data_fixed_dist)
}

#' Process GPS coordinates for AFP cases
#'
#' Handles missing coordinates, validates existing ones, and ensures that all
#' AFP cases have accurate geographic coordinates for mapping and spatial
#' analysis.
#'
#' @param data `tibble` A data frame containing AFP data with admin GUIDs
#' @param polis_data_folder `str` Path to the POLIS data folder containing
#'   Core_Ready_Files.
#' @param polis_folder `str` Path to the main POLIS folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return A tibble with processed coordinate data
#' @export
s2_process_coordinates <- function(data, polis_data_folder, polis_folder,
                                   output_folder_name) {

  cli::cli_process_start("Processing GPS coordinates for AFP cases",
                         msg_done = "Processed GPS coordinates for AFP cases"
  )

  invisible(capture.output(
    shape_data <- switch(Sys.getenv("POLIS_EDAV_FLAG"),
                         "TRUE" = {
                           sirfunctions::load_clean_dist_sp(fp = file.path("GID/PEB/SIR",
                                                                           polis_folder,
                                                                           "misc",
                                                                           "global.dist.rds"))
                         },
                         "FALSE" = {
                           sirfunctions::load_clean_dist_sp(fp = file.path(polis_folder,
                                                                           "misc",
                                                                           "global.dist.rds"),
                                                            edav = FALSE)
                         })
  ))

  # Rename latitude and longitude columns
  data_renamed <- data |>
    dplyr::rename(
      polis.latitude = x,
      polis.longitude = y
    ) |>
    dplyr::distinct()

  cli::cli_process_done()

  cli::cli_process_start("Checking for duplicate EPIDs",
                         msg_done = "Checked for duplicate EPIDs")

  # Check for duplicate EPIDs
  dup_epid <- data_renamed |>
    dplyr::group_by(epid) |>
    dplyr::mutate(dup_epid = dplyr::n()) |>
    dplyr::filter(dup_epid > 1) |>
    dplyr::ungroup() |>
    dplyr::select(
      epid, date.onset, yronset, diagnosis.final, classification,
      classificationvdpv, final.cell.culture.result, poliovirustypes,
      person.sex, place.admin.0, place.admin.1, place.admin.2
    ) |>
    dplyr::distinct() |>
    dplyr::arrange(epid)

  if (nrow(dup_epid) > 0) {
    # Export duplicates
    invisible(capture.output(
      tidypolis_io(
        obj = dup_epid,
        io = "write",
        file_path = paste0(
          polis_data_folder,
          "/", output_folder_name,
          "/duplicate_AFP_epids_Polis_",
          min(dup_epid$yronset, na.rm = TRUE),
          "_",
          max(dup_epid$yronset, na.rm = TRUE),
          ".csv"
        )
      )
    ))

    # Remove duplicates
    data_deduped <- data_renamed[!duplicated(data_renamed$epid), ]
    cli::cli_alert_warning(paste0(
      "Found and removed ", nrow(dup_epid), " duplicate EPIDs"
    ))
  } else {
    data_deduped <- data_renamed
  }

  cli::cli_process_done()

  cli::cli_process_start("Checking for cases with bad GUIDs",
                         msg_done = "Checked for cases with bad GUIDs")

  # Check for cases with bad GUIDs
  afp_noshape <- dplyr::anti_join(
    data_deduped,
    shape_data,
    by = c("Admin2GUID" = "GUID")
  )

  if (nrow(afp_noshape) > 0) {

    invisible(capture.output(
      tidypolis_io(
        obj = afp_noshape,
        io = "write",
        file_path = paste0(
          polis_data_folder, "/",
          output_folder_name,
          "/AFP_epids_bad_guid.csv"
        )
      )
    ))

    cli::cli_alert_warning(paste0(
      "Found ", nrow(afp_noshape), " cases with bad GUIDs"
    ))
  }

  cli::cli_process_done()

  # Check for cases with empty coordinates
  cli::cli_process_start("Checking for cases with missing coordinates",
                         msg_done = "Checked for cases with missing coordinates"
  )

  empty_coord_check <- data_deduped |>
    dplyr::filter(is.na(polis.latitude) | is.na(polis.longitude) |
                    (polis.latitude == 0 & polis.longitude == 0))

  if (nrow(empty_coord_check) > 0) {

    invisible(capture.output(
      tidypolis_io(
        io = "write",
        file_path = paste0(polis_data_folder, "/", output_folder_name,
                           "/afp_empty_coords.csv"),
        obj = empty_coord_check |>
          dplyr::select(polis.case.id, epid, date.onset,
                        place.admin.0, polis.latitude,
                        polis.longitude)
      )
    ))

    cli::cli_alert_warning(
      paste0(
        "Found ", nrow(empty_coord_check),
        " cases with missing coordinates"
      ))
  }

  cli::cli_process_done()

  # Process spatial coordinates using the existing function
  processed_data <- f.pre.stsample.01(
    df01 = data_deduped,
    global.dist.01 = shape_data
  )

  cli::cli_process_done()

  invisible(gc(full = TRUE))

  return(processed_data)
}

#" Create key AFP variables
#"
#' Creates and processes key variables used in AFP surveillance analysis, including
#' data quality flags, stool adequacy measures, and follow-up indicators.
#'
#' @param data `tibble` A data frame containing AFP data with spatial coordinates
#'
#' @return A data frame with additional AFP analysis variables
#' @export
s2_create_afp_variables <- function(data) {

  cli::cli_process_start("Creating key AFP variables")

  afp_data <- data |>
    dplyr::ungroup() |>
    dplyr::mutate(
      # Quality flag for onset date
      bad.onset = dplyr::case_when(
        is.na(dateonset) == TRUE ~ "Missing",
        is.na(datenotify) == TRUE |
          is.na(dateinvest) == TRUE ~ "Invest or Notify date missing",
        ontonot < 0 & ontoinvest < 0 &
          is.na(dateonset) == FALSE ~ "Onset after Notif and Invest",
        ontonot < 0 &
          is.na(dateonset) == FALSE ~ "Onset after Notification",
        ontoinvest < 0 &
          is.na(dateonset) == FALSE ~ "Onset after Investigation",
        ontonot >= 0 & ontoinvest >= 0 &
          is.na(dateonset) == FALSE ~ "Good Date"
      ),
      # Re-parse followup date to ensure consistency
      followup.date = lubridate::ymd(
        as.Date(followup.date, "%Y-%m-%dT%H:%M:%S")
      ),

      # Additional date quality flags
      bad.notify = f.datecheck.onset(datenotify, dateonset),
      bad.invest = f.datecheck.onset(dateinvest, dateonset),
      bad.stool1 = f.datecheck.onset(datestool1, dateonset),
      bad.stool2 = f.datecheck.onset(datestool2, dateonset),
      bad.followup = f.datecheck.onset(followup.date, dateonset),

      # Stool timeliness indicator
      timeliness.01 = dplyr::case_when(
        bad.stool1 == "data entry error" |
          bad.stool1 == "date before onset" |
          bad.stool1 == "date onset missing" ~ "Unable to Assess",
        bad.stool2 == "data entry error" |
          bad.stool2 == "date before onset" |
          bad.stool2 == "date onset missing" ~ "Unable to Assess",
        ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 &
          ontostool2 >= 1 & stool1tostool2 >= 1 ~ "Timely",
        is.na(datestool1) == TRUE | is.na(datestool2) == TRUE ~ "Not Timely",
        ontostool1 > 13 | ontostool1 < 0 | ontostool2 > 14 |
          ontostool2 < 1 | stool1tostool2 < 1 ~ "Not Timely"
      ),

      # Stool adequacy variables - 3 different methods
      # Method 1: Missing = Bad
      adequacy.01 = dplyr::case_when(
        bad.stool1 == "data entry error" | bad.stool1 == "date before onset" |
          bad.stool1 == "date onset missing" ~ 77,
        bad.stool2 == "data entry error" | bad.stool2 == "date before onset" |
          bad.stool2 == "date onset missing" ~ 77,
        ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 &
          ontostool2 >= 1 & stool1tostool2 >= 1 &
          is.na(stool1tostool2) == FALSE &
          stool.1.condition == "Good" & stool.2.condition == "Good" ~ 1,
        ontostool1 > 13 | ontostool1 < 0 | ontostool2 > 14 | ontostool2 < 1 |
          stool1tostool2 < 1 | is.na(stool1tostool2) == TRUE |
          stool.1.condition != "Good" | stool.2.condition != "Good" |
          is.na(stool.1.condition) == TRUE |
          is.na(stool.2.condition) == TRUE ~ 0
      ),

      # Method 2: Missing = Unknown (99)
      adequacy.02 = dplyr::case_when(
        bad.stool1 == "data entry error" | bad.stool1 == "date before onset" |
          bad.stool1 == "date onset missing" ~ 77,
        bad.stool2 == "data entry error" | bad.stool2 == "date before onset" |
          bad.stool2 == "date onset missing" ~ 77,
        ontostool1 > 13 | ontostool1 < 0 | is.na(stool1tostool2) == TRUE |
          ontostool2 > 14 | ontostool2 < 1 | stool1tostool2 < 1 |
          stool.1.condition == "Poor" | stool.2.condition == "Poor" ~ 0,
        is.na(stool.1.condition) == TRUE | is.na(stool.2.condition) == TRUE |
          stool.1.condition == "Unknown" | stool.2.condition == "Unknown" ~ 99,
        ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 &
          ontostool2 >= 1 & stool1tostool2 >= 1 &
          stool.1.condition == "Good" & stool.2.condition == "Good" ~ 1
      ),

      # Method 3: Missing = Good (POLIS method)
      adequacy.03 = dplyr::case_when(
        bad.stool1 == "data entry error" | bad.stool1 == "date before onset" |
          bad.stool1 == "date onset missing" ~ 77,
        bad.stool2 == "data entry error" | bad.stool2 == "date before onset" |
          bad.stool2 == "date onset missing" ~ 77,
        ontostool1 <= 13 & ontostool1 >= 0 & ontostool2 <= 14 &
          ontostool2 >= 1 &
          stool1tostool2 >= 1 & is.na(stool1tostool2) == FALSE &
          (stool.1.condition == "Good" | is.na(stool.1.condition) |
             stool.1.condition == "Unknown") &
          (stool.2.condition == "Good" | is.na(stool.2.condition) |
             stool.2.condition == "Unknown") ~ 1,
        ontostool1 > 13 | ontostool1 < 0 | ontostool2 > 14 | ontostool2 < 1 |
          stool1tostool2 < 1 | is.na(stool1tostool2) == TRUE |
          stool.1.condition == "Poor" | stool.2.condition == "Poor" ~ 0
      ),

      # Create stool missing indicators
      stool2missing = dplyr::if_else(
        is.na(datestool2) & is.na(stool.2.condition), 1, 0
      ),
      stool1missing = dplyr::if_else(
        is.na(datestool1) & is.na(stool.1.condition), 1, 0
      ),
      stoolmissing = dplyr::if_else(
        stool1missing == 1 & stool2missing == 1, 1, 0
      ),

      # Create categorical time interval variables
      ontostool2.03 = NA,
      ontostool2.03 = dplyr::if_else(ontostool2 > 14, ">14", ontostool2.03),
      ontostool2.03 = dplyr::if_else(ontostool2 <= 14, "<=14", ontostool2.03),
      ontonot.60 = NA,
      ontonot.60 = dplyr::if_else(ontonot > 60, ">60", ontonot.60),
      ontonot.60 = dplyr::if_else(ontonot <= 60, "<=60", ontonot.60),
      ontonot.14 = NA,
      ontonot.14 = dplyr::if_else(ontonot > 14, ">14", ontonot.14),
      ontonot.14 = dplyr::if_else(ontonot <= 14, "<=14", ontonot.14),

      # 60-day follow-up variables
      need60day = dplyr::if_else(timeliness.01 == "Not Timely", 1, 0),
      got60day = dplyr::case_when(
        need60day == 1 & is.na(followup.date) == FALSE ~ 1,
        need60day == 1 & is.na(followup.date) == TRUE ~ 0,
        need60day == 0 ~ 99
      ),
      timeto60day = followup.date - dateonset,
      ontime.60day = dplyr::case_when(
        need60day == 0 ~ 99, # excluded timely cases
        need60day == 1 & timeto60day >= 60 & timeto60day <= 90 ~ 1,
        (need60day == 1 & timeto60day < 60 | timeto60day > 90 |
           is.na(timeto60day) == TRUE) ~ 0
      ),

      # Format GUID strings
      adm0guid = paste("{",
                       stringr::str_to_upper(admin0guid), "}",
                       sep = ""
      ),
      adm0guid = dplyr::if_else(
        adm0guid == "{}" | adm0guid == "{NA}", NA, adm0guid
      ),
      adm1guid = paste("{", stringr::str_to_upper(admin1guid), "}", sep = ""),
      adm1guid = dplyr::if_else(
        adm1guid == "{}" | adm1guid == "{NA}", NA, adm1guid
      ),
      adm2guid = paste("{", stringr::str_to_upper(admin2guid), "}", sep = ""),
      adm2guid = dplyr::if_else(
        adm2guid == "{}" | adm2guid == "{NA}", NA, adm2guid
      )
    ) |>
    # Rename variables for consistency with existing naming conventions
    dplyr::rename(
      adequate.stool = stool.adequacy,
      datasetlab = dataset.lab,
      doses.total = doses,
      virus.cluster = `virus.cluster(s)`,
      emergence.group = `emergence.group(s)`
    ) |>
    dplyr::filter(!is.na(epid)) |>
    dplyr::select(-c(Admin2GUID, Admin1GUID, Admin0GUID))

  cli::cli_process_done()

  invisible(gc(full = TRUE))

  return(afp_data)
}

#' Export AFP data and related outputs
#'
#' Creates and exports various AFP data files including the main AFP linelist,
#' spatial data files, and comparison with previous datasets.
#'
#' @param data A data frame containing processed AFP data
#' @param latest_archive String containing the name of the latest archive folder
#' @param polis_data_folder String path to the POLIS data folder
#' @param col_afp_raw Names of original columns for comparison
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return Invisibly returns NULL
#' @export
s2_export_afp_outputs <- function(data, latest_archive, polis_data_folder,
                                  col_afp_raw, output_folder_name) {

  cli::cli_process_start("Exporting AFP outputs",
                         msg_done = "Exported AFP outputs")

  # Separate AFP from other surveillance types
  afp_data <- data |>
    dplyr::filter(surveillancetypename == "AFP") |>
    dplyr::distinct()

  not_afp <- data |>
    dplyr::filter(surveillancetypename != "AFP")

  unknown_afp <- data |>
    dplyr::filter(is.na(surveillancetypename))

  # Export missing GUIDs summary
  afp_missing_count <- afp_data |>
    dplyr::filter(is.na(admin1guid) | is.na(admin2guid)) |>
    dplyr::group_by(place.admin.0, yronset) |>
    dplyr::summarise(case.miss.guid = dplyr::n(), .groups = "drop") |>
    tidyr::pivot_wider(names_from = yronset, values_from = case.miss.guid)

  afp_missing_epids <- afp_data |>
    dplyr::filter(is.na(admin1guid) | is.na(admin2guid)) |>
    dplyr::select(
      polis.case.id, epid, place.admin.0, place.admin.1, place.admin.2,
      date.onset, yronset, adm0guid, adm1guid, admin2guid
    )

  # Export missing guid files
  invisible(capture.output(
    tidypolis_io(
      obj = afp_missing_count,
      io = "write",
      file_path = paste0(
        polis_data_folder, "/", output_folder_name,
        "/afp_missing_guid_count_",
        min(data$dateonset, na.rm = TRUE), "_",
        max(data$dateonset, na.rm = TRUE), ".csv"
      )
    )
  ))

  invisible(capture.output(
    tidypolis_io(
      obj = afp_missing_epids,
      io = "write",
      file_path = paste0(
        polis_data_folder, "/",  output_folder_name,
        "/afp_missing_guid_epids_",
        min(data$dateonset, na.rm = TRUE), "_",
        max(data$dateonset, na.rm = TRUE), ".csv"
      )
    )
  ))

  # Prepare data for export
  afp_data <- afp_data |>
    dplyr::mutate(
      polis.latitude = as.character(polis.latitude),
      polis.longitude = as.character(polis.longitude),
      doses.total = as.numeric(doses.total)
    )

  # Compare with archive using the dedicated comparison function
  comparison_results <- s2_compare_with_archive(
    data = afp_data,
    polis_data_folder = polis_data_folder,
    latest_archive = latest_archive,
    col_afp_raw = col_afp_raw,
    output_folder_name = output_folder_name
  )

  # Export AFP linelist
  invisible(capture.output(
    tidypolis_io(
      obj = afp_data,
      io = "write",
      file_path = paste0(
        polis_data_folder, "/",
        output_folder_name, "/afp_linelist_",
        min(afp_data$dateonset, na.rm = TRUE), "_",
        max(afp_data$dateonset, na.rm = TRUE), ".rds"
      )
    )
  ))


  # Export spatial data
  afp_latlong <- data |>
    dplyr::ungroup() |>
    dplyr::select(
      epid, dateonset, place.admin.0, place.admin.1, place.admin.2,
      adm0guid, adm1guid, adm2guid, cdc.classification.all, lat, lon
    )

  invisible(capture.output(
    tidypolis_io(
      obj = afp_latlong,
      io = "write",
      file_path = paste0(
        polis_data_folder, "/", output_folder_name, "/afp_lat_long_",
        min(afp_latlong$dateonset, na.rm = TRUE), "_",
        max(afp_latlong$dateonset, na.rm = TRUE), ".csv"
      )
    )
  ))


  # Create combined AFP dataset from multiple files
  cli::cli_process_start("Generating combined AFP dataset",
                         msg_done = "Generated combined AFP dataset")

  # Find all AFP files in Core_Ready_Files and core_files_to_combine
  afp_files_main <- dplyr::tibble("name" = tidypolis_io(
    io = "list",
    file_path = paste0(polis_data_folder, "/", output_folder_name),
    full_names = TRUE
  )) |>
    dplyr::filter(grepl("^.*(afp_linelist).*(.rds)$", name)) |>
    dplyr::pull(name)

  afp_files_combine <- dplyr::tibble("name" = tidypolis_io(
    io = "list",
    file_path = paste0(polis_data_folder, "/core_files_to_combine"),
    full_names = TRUE
  )) |>
    dplyr::filter(grepl("^.*(afp_linelist).*(.rds)$", name)) |>
    dplyr::pull(name)

  # Combine AFP files
  if (length(afp_files_combine) > 0) {
    invisible(capture.output(
      afp_to_combine <- purrr::map_df(afp_files_combine, ~ tidypolis_io(
        io = "read",
        file_path = .x
      )) |>
        dplyr::mutate(
          stool1tostool2 = as.numeric(stool1tostool2),
          datenotificationtohq = lubridate::parse_date_time(
            datenotificationtohq,
            c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")
          )
        )
    ))

    invisible(capture.output(
      afp_new <- purrr::map_df(
        afp_files_main,
        ~ tidypolis_io(io = "read", file_path = .x)
      )
    ))

    afp_combined <- dplyr::bind_rows(afp_new, afp_to_combine)

    # Export combined AFP dataset
    invisible(capture.output(
      tidypolis_io(
        obj = afp_combined,
        io = "write",
        file_path = paste0(
          polis_data_folder,
          "/", output_folder_name, "/",
          "afp_linelist_",
          min(afp_combined$dateonset, na.rm = TRUE),
          "_",
          max(afp_combined$dateonset, na.rm = TRUE),
          ".rds"
        )
      )
    ))


    # Create light AFP dataset for WHO (filtered to recent years)
    afp_light <- afp_combined |>
      dplyr::filter(yronset >= 2019)

    invisible(capture.output(
      tidypolis_io(
        obj = afp_light,
        io = "write",
        file_path = paste0(
          polis_data_folder,
          "/", output_folder_name,
          "/afp_linelist_",
          min(afp_light$dateonset, na.rm = TRUE),
          "_",
          max(afp_light$dateonset, na.rm = TRUE),
          ".rds"
        )
      )
    ))

  }

  cli::cli_process_done()

  # Process non-AFP data if needed
  if (nrow(not_afp) > 0 || nrow(unknown_afp) > 0) {
    cli::cli_process_start("Processing non-AFP surveillance data",
                           msg_done = "Processed non-AFP surveillance data")

    other_surv <- dplyr::bind_rows(not_afp, unknown_afp) |>
      dplyr::mutate(
        polis.latitude = as.character(polis.latitude),
        polis.longitude = as.character(polis.longitude),
        doses.total = as.numeric(doses.total)
      )

    # Export non-AFP data
    invisible(capture.output(
      tidypolis_io(
        obj = other_surv,
        io = "write",
        file_path = paste0(
          polis_data_folder,
          "/", output_folder_name,
          "/other_surveillance_type_linelist_",
          min(other_surv$yronset, na.rm = TRUE), "_",
          max(other_surv$yronset, na.rm = TRUE), ".rds"
        )
      )
    ))

    # Create combined AFP dataset from multiple files
    cli::cli_process_start("Generating combined Other Surveillance dataset",
                           msg_done = "Generated combined Other Surveillance dataset")

    # Find all other surveillance files in Core_Ready_Files and core_files_to_combine
    other_files_main <- dplyr::tibble("name" = tidypolis_io(
      io = "list",
      file_path = paste0(polis_data_folder, "/", output_folder_name),
      full_names = TRUE
    )) |>
      dplyr::filter(grepl("^.*(other_surveillance).*(.rds)$", name)) |>
      dplyr::pull(name)

    other_files_combine <- dplyr::tibble("name" = tidypolis_io(
      io = "list",
      file_path = paste0(polis_data_folder, "/core_files_to_combine"),
      full_names = TRUE
    )) |>
      dplyr::filter(grepl("^.*(other_surveillance).*(.rds)$", name)) |>
      dplyr::pull(name)

    # Combine other surveillance files
    if (length(other_files_combine) > 0) {
      invisible(capture.output(
        other_to_combine <- purrr::map_df(other_files_combine, ~ tidypolis_io(
          io = "read",
          file_path = .x
        ))
      ))

      other_to_combine <- other_to_combine |>
        dplyr::mutate(
          stool1tostool2 = as.numeric(stool1tostool2),
          datenotificationtohq = lubridate::parse_date_time(
            datenotificationtohq,
            c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")
          )
        )

      invisible(capture.output(
        other_new <- purrr::map_df(
          other_files_main,
          ~ tidypolis_io(io = "read", file_path = .x)
        )
      ))

      other_combined <- dplyr::bind_rows(other_new, other_to_combine)

      # Export combined other surveillance dataset
      invisible(capture.output(
        tidypolis_io(
          obj = other_combined,
          io = "write",
          file_path = paste0(
            polis_data_folder,
            "/", output_folder_name,
            "/other_surveillance_type_linelist_",
            min(other_combined$yronset, na.rm = TRUE),
            "_",
            max(other_combined$yronset, na.rm = TRUE),
            ".rds"
          )
        )
      ))

      cli::cli_process_done()
    }

    update_polis_log(
      .event = "AFP and Other Surveillance Linelists Finished",
      .event_type = "PROCESS"
    )

    cli::cli_process_done()

    gc(full = TRUE)

    invisible(NULL)
  }
}

#' Compare AFP data with archived version
#'
#' @description
#' Compares the new AFP dataset with the previous version from the archive,
#' identifying structural differences, new records, and modified records.
#'
#' @param data A data frame containing new AFP data
#' @param polis_data_folder String path to the POLIS data folder
#' @param latest_archive String name of the most recent archive folder
#' @param col_afp_raw Names of columns in the original raw AFP data
#' @param start_year Integer. Start year for filtering (default: 2020)
#' @param end_year Integer. End year for filtering (default: current year)
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @return A list containing comparison results:
#'   - metadata_comparison: Structural differences between datasets
#'   - new_records: Records in new data not in old data
#'   - removed_records: Records in old data not in new data
#'   - modified_records: Records that exist in both but were changed
#'
#' @details
#' This function performs a comprehensive comparison by:
#' 1. Loading the most recent archived AFP dataset
#' 2. Comparing metadata (variable names, types, values)
#' 3. Identifying added, removed, and modified records
#' 4. Generating a detailed report of differences
#'
#' @export
s2_compare_with_archive <- function(data,
                                    polis_data_folder,
                                    latest_archive,
                                    col_afp_raw,
                                    start_year = 2020,
                                    end_year = lubridate::year(Sys.Date()),
                                    output_folder_name) {
  cli::cli_process_start("Comparing data with last AFP dataset")

  # Prepare data for comparison by standardizing types
  data_prepared <- data |>
    dplyr::mutate(
      polis.latitude = as.character(polis.latitude),
      polis.longitude = as.character(polis.longitude),
      doses.total = as.numeric(doses.total)
    )

  # Find old AFP file in the archive
  invisible(capture.output(
    archive_files <- tidypolis_io(
      io = "list",
      file_path = paste0(
        polis_data_folder, "/", output_folder_name, "/Archive/",
        latest_archive
      ),
      full_names = TRUE
    )
  ))


  old_file <- archive_files[grepl("afp_linelist_2020", archive_files)]

  if (length(old_file) == 0) {
    cli::cli_alert_info("No previous AFP dataset found for comparison")
    cli::cli_process_done()
    return(NULL)
  }

  # Read old data and standardize format for comparison
  cli::cli_alert_info("Loading archived AFP dataset for comparison")
  invisible(capture.output(
    old_data <- tidypolis_io(io = "read", file_path = old_file) |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::mutate_all(as.character) |>
      dplyr::mutate(yronset = as.numeric(yronset)) |>
      dplyr::filter(dplyr::between(yronset, start_year, end_year)) |>
      dplyr::mutate_all(as.character)
  ))

  # Harmonize column selections
  data_prepared <- data_prepared |>
    dplyr::ungroup() |>
    dplyr::select(-c(setdiff(
      setdiff(colnames(data_prepared), col_afp_raw),
      colnames(old_data)
    )))

  # Compare metadata (structure, variables, values)
  new_table_metadata <- f.summarise.metadata(head(data_prepared, 1000))
  old_table_metadata <- f.summarise.metadata(head(old_data, 1000))
  metadata_comparison <- f.compare.metadata(
    new_table_metadata,
    old_table_metadata,
    "AFP"
  )

  # Compare individual records
  new_for_comparison <- data_prepared |>
    dplyr::mutate(epid = stringr::str_squish(epid)) |>
    dplyr::mutate_all(as.character)

  # Find new records
  in_new_not_old <- new_for_comparison[
    !(new_for_comparison$epid %in% old_data$epid),
  ]

  # Find removed records
  in_old_not_new <- old_data[
    !(old_data$epid %in% new_for_comparison$epid),
  ]

  # Find modified records
  potential_modifications <- new_for_comparison |>
    dplyr::filter(epid %in% old_data$epid) |>
    dplyr::select(-c(setdiff(colnames(new_for_comparison), colnames(old_data))))

  unchanged_records <- intersect(
    potential_modifications,
    old_data |> dplyr::select(-c(setdiff(
      colnames(old_data),
      colnames(potential_modifications)
    )))
  )

  modified_records <- setdiff(potential_modifications, unchanged_records)

  if (nrow(modified_records) > 0) {
    modified_details <- modified_records |>
      dplyr::inner_join(
        old_data |>
          dplyr::filter(epid %in% modified_records$epid) |>
          dplyr::select(-c(setdiff(
            colnames(old_data),
            colnames(modified_records)
          ))) |>
          setdiff(new_for_comparison |>
                    dplyr::select(-c(setdiff(
                      colnames(new_for_comparison),
                      colnames(old_data)
                    )))),
        by = "epid"
      ) |>
      # Convert wide to long for comparing differences
      tidyr::pivot_longer(cols = -epid) |>
      dplyr::mutate(source = dplyr::if_else(
        stringr::str_sub(name, -2) == ".x", "new", "old"
      )) |>
      dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
      # Convert back to wide for display
      tidyr::pivot_wider(names_from = source, values_from = value) |>
      dplyr::filter(new != old & !name %in% c("lat", "lon"))
  } else {
    modified_details <- data.frame()
  }

  # Log summary of changes
  update_polis_log(
    .event = paste0(
      "AFP New Records: ", nrow(in_new_not_old), "; ",
      "AFP Removed Records: ", nrow(in_old_not_new), "; ",
      "AFP Modified Records: ",
      length(unique(modified_records$epid))
    ),
    .event_type = "INFO"
  )

  cli::cli_process_done()

  invisible(gc(full = FALSE))

  # Return comparison results
  return(list(
    metadata_comparison = metadata_comparison,
    new_records = in_new_not_old,
    removed_records = in_old_not_new,
    modified_records = modified_records,
    modified_details = modified_details
  ))
}

###### Step 3 Private Functions ----


#' Process SIA Data Pipeline
#'
#' This function processes SIA data through multiple standardization and
#' validation steps, including checking for duplicates, standardizing dates,
#' clustering dates, and processing coordinates.
#'
#' @param long.global.dist.01 `sf` Global district lookup table for GUID
#'   validation.
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param latest_folder_in_archive `str` Name of the latest folder in the archive.
#' @param timestamp `str` The time stamp.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#'
#' @export
s3_fully_process_sia_data <- function(long.global.dist.01, polis_data_folder,
                                      latest_folder_in_archive, timestamp,
                                      output_folder_name){

  if (!tidypolis_io(io = "exists.dir",
                    file_path = file.path(polis_data_folder, output_folder_name))) {
    cli::cli_abort("Please run Step 1 and create a Core Ready folder before running this step.")
  }

  #dataset used to check mismatched guids and create sia.06
  sia.05 <- s3_sia_load_data(
    polis_data_folder = polis_data_folder,
    latest_folder_in_archive = latest_folder_in_archive, output_folder_name) |>
    s3_sia_create_cdc_vars() |>
    s3_sia_check_guids(long.global.dist.01 = long.global.dist.01)

  #used to write pre cluster data and create final output data
  sia.06 <- s3_sia_check_duplicates(sia.05 = sia.05)

  #checks metadata of near final output
  s3_sia_check_metadata(sia.06 = sia.06,
                        polis_data_folder = polis_data_folder,
                        latest_folder_in_archive = latest_folder_in_archive,
                        output_folder_name = output_folder_name)

  #writes our partially processed data into cache
  s3_sia_write_precluster_data(sia.06 = sia.06,
                               polis_data_folder = polis_data_folder,
                               output_folder_name = output_folder_name)

  #final clean dataset
  sia.clean.01 <- s3_sia_combine_historical_data(sia.new = sia.06, polis_data_folder)

  #creates cache from clustered SIA dates
  s3_sia_cluster_dates(sia.clean.01)

  #merged data with clustered data
  s3_sia_merge_cluster_dates_final_data(sia.clean.01 = sia.clean.01,
                                        output_folder_name = output_folder_name)

  #evaluate unmatched GUIDs
  s3_sia_evaluate_unmatched_guids(sia.05 = sia.05, polis_data_folder,
                                  output_folder_name = output_folder_name)

  update_polis_log(.event = "SIA Finished",
                   .event_type = "PROCESS")

  cli::cli_process_done("Clearing memory")
  rm(list = c("sia.05", "sia.06", "sia.clean.01"))
  invisible(gc())
  cli::cli_process_done()
  invisible()
}

#' Load SIA Data
#'
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param latest_folder_in_archive `str` Time stamp of latest folder in archive
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `tiblle` sia.01.new - the latest SIA data quality checked for variable
#' stability against the last download if it exists
#' @keywords internal
#'
s3_sia_load_data <- function(polis_data_folder,
                             latest_folder_in_archive,
                             output_folder_name){

  # Step 1: Read in "old" data file (System to find "Old" data file)
  x <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name, "Archive",
                                          latest_folder_in_archive),
                    full_names = T)

  y <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name),
                    full_names = T)


  old.file <- x[grepl("Activity",x)]

  new.file <- y[grepl("Activity", y)]


  cli::cli_process_start("Loading new SIA data")
  # Step 2: Read in "new" data file
  # Newest downloaded activity file, will be .rds located in Core Ready Files
  invisible(capture.output(
    sia.01.new <- tidypolis_io(io = "read", file_path = new.file) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~dplyr::na_if(.,"")))
  ))

  # Make the names small case and remove space bar from names
  names(sia.01.new) <- stringr::str_to_lower(names(sia.01.new))
  cli::cli_process_done()

  # QC CHECK
  # This is for checking data across different download options

  if(length(x) > 0){

    cli::cli_process_start("Loading old SIA data")
    # Old pre-existing download
    # This is previous Activity .rds that was preprocessed last week, it has
    # been moved to the archive, change archive subfolder and specify last weeks
    # Activity .rds
    invisible(capture.output(
      sia.01.old <- tidypolis_io(io = "read", file_path = old.file) |>
        dplyr::mutate_all(as.character) |>
        dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
        dplyr::mutate_all(list(~dplyr::na_if(.,"")))
    ))

    names(sia.01.old) <- stringr::str_to_lower(names(sia.01.old))
    cli::cli_process_done()

    # Are there differences in the names of the columns between two downloads?
    f.compare.dataframe.cols(sia.01.old, sia.01.new)

    # If okay with above then proceed to next step
    # First determine the variables that would be excluded from the comparison
    # this includes variables such as EPID numbers which would invariably change
    # in values

    var.list.01 <- c(
      "sia.code",
      "sia.sub-activity.code",
      "country",
      "country.iso3",
      "who.region",
      "ist",
      "activity.start.date",
      "activity.end.date",
      "last.updated.date",
      "sub-activity.start.date",
      "sub-activity.end.date",
      "admin1",
      "admin2",
      "delay.reason",
      "priority",
      "country.population.%",
      "unpd.country.population",
      "targeted.population",
      "immunized.population",
      "admin.coverage.%",
      "area.targeted.%",
      "area.population",
      "age.group.%",
      "wastage.factor",
      "number.of.doses",
      "number.of.doses.approved",
      "im.loaded",
      "lqas.loaded",
      "sub-activity.last.updated.date",
      "admin.2.targeted.population",
      "admin.2.immunized.population",
      "admin.2.accessibility.status",
      "admin.2.comments",
      "sub-activity.initial.planned.date",
      "activity.parent.children.inaccessible",
      "children.inaccessible",
      "admin2.children.inaccessible",
      "linked.outbreak(s)",
      "admin.0.guid",
      "admin.1.guid",
      "admin.2.guid"
    )

    cli::cli_process_start("Comparing downloaded variables")
    # Exclude the variables from
    sia.01.old.compare <- sia.01.old |>
      dplyr::select(
        dplyr::any_of(var.list.01)
      )

    sia.01.new.compare <- sia.01.new |>
      dplyr::select(
        dplyr::any_of(var.list.01)
      )

    new.var.sia.01 <- f.download.compare.01(sia.01.old.compare, sia.01.new.compare)

    new.df <- new.var.sia.01 |>
      dplyr::filter(is.na(old.distinct.01) | diff.distinct.01 >= 1) |>
      dplyr::filter(!variable %in% c("parentid", "id"))

    if (nrow(new.df) >= 1) {
      cli::cli_alert_danger("There is either a new variable in the SIA data or new value of an existing variable.
       Please run f.download.compare.02 to see what it is. Preprocessing can not continue until this is adressed.")

      sia.new.value <- f.download.compare.02(new.var.sia.01, sia.01.old.compare, sia.01.new.compare, type = "SIA")

    } else {

      cli::cli_alert_info("New SIA download is comparable to old SIA download")

    }

  }else{
    cli::cli_alert_info("No old SIA data found, will not perform comparisons")
  }

  cli::cli_process_done()

  return(sia.01.new)

}

#' Convert variables in POLIS download to CDC specific variables
#'
#' @param sia.01.new `tibble` The latest SIA download with variables checked
#' against the last download
#' @param startyr `int` The subset of years for which to process SIA data
#' @param endyr `int` The subset of years for which to process SIA data
#'
#' @returns `tibble` sia.02 SIA data with CDC variables enforced
#' @keywords internal
#'
s3_sia_create_cdc_vars <- function(sia.01.new,
                               startyr = 2020,
                               endyr = lubridate::year(format(Sys.time()))){

  # Get the date from 'Mon-year' format of parent start date as a first day of the month.
  # this would insert first of month if date is missing from the activity date. This
  # leads to errors if one is trying to guess duration of activity. For negative duration ignore it.
  # Active year at country level is start yr of activity
  # active year at prov and district level is start yr of sub-activity

  cli::cli_process_start("Create CDC variables")
  sia.02 <- sia.01.new |>
    #rename key variables to remove hyphens and standardize naming convention
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
      # Checked with Valentina at WHO. lqas and im loaded variables are not
      # correctly populated in the POLIS. Valentina will fix this issue.
      #status = ifelse((im.loaded == "yes" & is.na(status == T)) | (lqas.loaded == "yes" & is.na(status == T)),
      #                "Done", status),
      # Replacing NA with "Missing"
      status = tidyr::replace_na(status, "Missing"),
      vaccine.type = tidyr::replace_na(vaccine.type, "Missing")
    ) |>
    #Replace "undefined" with NA
    dplyr::mutate_at(
      dplyr::vars(sub.activity.end.date), ~ dplyr::na_if(., "undefined")
    ) |>
    #convert dates into standard format
    dplyr::mutate_at(
      dplyr::vars(
        sub.activity.initial.planned.date, sub.activity.last.updated.date,
        activity.start.date, activity.end.date,
        sub.activity.start.date, sub.activity.end.date,
        last.updated.date
      ), ~ lubridate::parse_date_time(., c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S"))
    ) |>
    #convert years and months into standard format
    dplyr::mutate(
      yr.sia = lubridate::year(activity.start.date),
      yr.subsia = lubridate::year(sub.activity.start.date),
      month.sia = lubridate::month(activity.start.date),
      month.subsia = lubridate::month(activity.start.date),
      `admin.coverage.%` = as.numeric(`admin.coverage.%`)
    ) |>
    #rename geographies into standard format
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
    #adjust GUIDS to be more easily searchable based on standard formats
    dplyr::mutate(
      adm2guid  = paste0("{", toupper(admin.2.guid), "}"),
      adm1guid  = paste0("{", toupper(admin.1.guid), "}"),
      adm0guid  = paste0("{", toupper(admin.0.guid), "}")
    ) |>
    dplyr::select(-admin.0.guid, -admin.1.guid, -admin.2.guid) |>
    dplyr::distinct()

  cli::cli_process_done()

  return(sia.02)

}

#' Check SIA GUIDS to ensure they match the standard spatial dataset
#'
#' @param sia.02 `tibble` The latest SIA download with variables checked and
#' against the last download and CDC variables created
#' @param long.global.dist.01 `sf` a cleaned global spatial dataset of districts
#' in "long" format for years meaning a district spatial object that is valid
#' from 2000-2005 will be represented 6 times, once for each year it is valid
#'
#' @returns `tibble` sia.03 SIA data with CDC variables enforced and GUIDs validated
#' @keywords internal
#'
s3_sia_check_guids <- function(sia.02, long.global.dist.01){

  cli::cli_process_start("Checking GUIDs")
  # SIA file with GUID
  # SIAs match with GUIDs in shapes
  sia.03 <- sia.02 |>
    #only keep variables of interest from spatial file
    dplyr::left_join(long.global.dist.01 |>
                       dplyr::select(GUID, ADM0_NAME, ADM1_NAME,
                                     ADM2_NAME, active.year.01),
                     by = c("adm2guid" = "GUID",
                            "yr.sia" = "active.year.01")) |>
    #flag if spatial files do not match
    dplyr::mutate(no_match=ifelse(is.na(ADM2_NAME), 1, 0)) |>
    dplyr::select(-dplyr::starts_with("shape"))

  # SIAs did not match with GUIDs in shapes.
  tofix <- sia.03 |>
    dplyr::select(-ADM0_NAME, -ADM1_NAME, -ADM2_NAME) |>
    #keep only districts that didn't match
    dplyr::filter(no_match==1) |>
    #left join based on location names and year
    dplyr::left_join(long.global.dist.01 |>
                       tibble::as_tibble() |>
                       dplyr::select(GUID, ADM0_NAME, ADM1_NAME,
                                     ADM2_NAME, active.year.01),
                     by = c("place.admin.0" = "ADM0_NAME",
                            "place.admin.1" = "ADM1_NAME",
                            "place.admin.2" = "ADM2_NAME",
                            "yr.sia" = "active.year.01")) |>
    #flag if guids still missing and merge in reference GUID
    dplyr::mutate(missing.guid = ifelse(is.na(GUID), 1, 0),
                  adm2guid = dplyr::if_else(missing.guid==0, GUID, adm2guid)) |>
    dplyr::select(-GUID, -no_match)

  # Combine SIAs matched with prov, dist with shapes
  sia.04 <- sia.03 |>
    dplyr::filter(no_match==0)|>
    dplyr::bind_rows(tofix)|>
    dplyr::mutate(place.admin.1=ifelse(is.na(place.admin.1), ADM1_NAME, place.admin.1),
                  place.admin.2=ifelse(is.na(place.admin.2), ADM2_NAME, place.admin.2)) |>
    dplyr::select(-no_match)

  # Next step is to remove duplicates:
  sia.05 <- dplyr::distinct(sia.04, adm2guid, sub.activity.start.date,
                            vaccine.type, age.group, status, lqas.loaded,
                            im.loaded, .keep_all= TRUE)

  cli::cli_process_done()

  return(sia.05)
}

#' Check SIA dataset for duplicates
#'
#' @param sia.05 `tibble` The latest SIA download with variables checked and
#' against the last download, CDC variables created and GUIDs validated
#'
#' @returns `tibble` sia.06 SIA data with CDC variables enforced, GUIDs validated
#' and duplicates removed
#' @keywords internal
#'
s3_sia_check_duplicates <- function(sia.05){

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
    dplyr::mutate(dup = dplyr::case_when(
      camp.diff.days == 0 & !is.na(place.admin.2) ~ 1,
      camp.diff.days > 0 | is.na(place.admin.2) | is.na(camp.diff.days) ~ 0)) |>

    dplyr::ungroup() |>

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
                  sub.activity.start.date = as.POSIXct(round(as.POSIXct(sub.activity.start.date)),
                                                       format="%Y-%m-%d %H:%M:%S")) |>
    dplyr::mutate(
      dplyr::across(
        dplyr::any_of(c("admin.1.id",
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
                        "admin.0.id" )),
        as.numeric))
  options(scipen = savescipen)

  cli::cli_process_done()

  return(sia.06)

}

#' Check SIA dataset metadata against the previous download for data type changes
#' or new types of entries
#'
#' @param sia.06 `tibble` The latest SIA download with variables checked and
#' against the last download, CDC variables created,  GUIDs validated and
#' deduplicated
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param latest_folder_in_archive `str` Time stamp of latest folder in archive
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#' @returns NULL
#' @keywords internal
#'
s3_sia_check_metadata <- function(sia.06, polis_data_folder, latest_folder_in_archive,
                                  output_folder_name){

  cli::cli_process_start("Checking metadata")
  # This is the final SIA file which would be used for analysis.
  # Compare the final file to last week's final file to identify any
  # differences in var_names, var_classes, or categorical responses

  x <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name,
                                          "Archive",
                                          latest_folder_in_archive),
                    full_names = T)

  y <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name),
                    full_names = T)

  sia.06 <- sia.06 |>
    dplyr::select(-dplyr::starts_with("Shape"))

  old.file <- x[grepl("sia_2020", x)]

  if(length(old.file) > 0){

    new_table_metadata <- f.summarise.metadata(dataframe = sia.06)

    invisible(capture.output(
      old <- tidypolis_io(io = "read", file_path = old.file)))
    old <- old |>
      dplyr::select(-dplyr::starts_with("Shape"))
    old_table_metadata <- f.summarise.metadata(dataframe = old)
    sia_metadata_comparison <- f.compare.metadata(new_table_metadata = new_table_metadata,
                                                  old_table_metadata = old_table_metadata,
                                                  table = "SIA")

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
                          dplyr::ungroup(),
                        by=c("sia.sub.activity.code", "adm2guid")) |>
      dplyr::select(-c(setdiff(colnames(new), colnames(old)))) |>
      tidyr::pivot_longer(cols=-c("sia.sub.activity.code", "adm2guid")) |>
      dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
      dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
      #long_to_wide
      tidyr::pivot_wider(names_from=source, values_from=value) |>
      dplyr::filter(new != old)

    update_polis_log(
      .event = paste0("SIA New Records: ", nrow(in_new_not_old), "; ",
                      "SIA Removed Records: ", nrow(in_old_not_new), "; ",
                      "SIA Modified Records: ",
                      length(unique(in_new_and_old_but_modified$sia.sub.activity.code))),
      .event_type = "INFO")

  }else{
    cli::cli_alert_info("No old SIA file identified, will not perform comparisons")
  }

  cli::cli_process_done()
  invisible()
}

#' Write out SIA data
#'
#' @param sia.06 `tibble` The latest SIA download with variables checked and
#' against the last download, CDC variables created, GUIDs validated and
#' deduplicated
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns NULL
#' @keywords internal
#'
s3_sia_write_precluster_data <- function(sia.06, polis_data_folder,
                                         output_folder_name){
  cli::cli_process_start("Writing out SIA file")
  # Write final SIA file to RDS file
  sia.file.path <- paste(polis_data_folder, "/", output_folder_name, "/", sep = "")

  invisible(capture.output(
    tidypolis_io(obj = sia.06, io = "write",
                 file_path = paste(
                   sia.file.path,
                   paste("sia", min(sia.06$yr.sia, na.rm = T),
                         max(sia.06$yr.sia, na.rm = T), sep = "_"),
                   ".rds",
                   sep = ""
                 ))
  ))

  cli::cli_process_done()
  invisible()
}

#' Read in SIA data pre 2020 and combine with current SIA data
#'
#' @param sia.06 `tibble` The latest SIA download with variables checked and
#' against the last download, CDC variables created, GUIDs validated and
#' deduplicated
#' @param polis_data_folder `str` Path to the POLIS data folder.
#'
#' @returns `tibble` sia.clean.01 All historical SIA data
#' @keywords internal
#'
s3_sia_combine_historical_data <- function(sia.new, polis_data_folder){

  #combine SIA pre-2020 with the current rds
  # read SIA and combine to make one SIA dataset

  sia.files.02 <- dplyr::tibble(
    "name" = tidypolis_io(io = "list",
                          file_path=paste0(polis_data_folder, "/core_files_to_combine"),
                          full_names=TRUE)) |>
    dplyr::filter(grepl("^.*(sia).*(.rds)$", name)) |>
    dplyr::pull(name)

  invisible(capture.output(
    sia.to.combine <- purrr::map_df(sia.files.02, ~ tidypolis_io(io = "read", file_path = .x))

  ))

  sia.to.combine <- sia.to.combine |>

    dplyr::mutate(

      sub.activity.initial.planned.date = lubridate::parse_date_time(
        sub.activity.initial.planned.date,
        c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),

      last.updated.date = lubridate::parse_date_time(
        last.updated.date,
        c("dmY", "bY", "Ymd", "%Y-%m-%d %H:%M:%S")),

      sub.activity.last.updated.date = as.Date(
        lubridate::parse_date_time(
          sub.activity.last.updated.date,
          c("dmY", "bY", "Ymd", "%d-%m-%Y %H:%M"))))

  sia.clean.01 <- dplyr::bind_rows(sia.new, sia.to.combine) |>

    mutate(
      sub.activity.last.updated.date = lubridate::as_date(sub.activity.last.updated.date),
      last.updated.date = lubridate::as_date(last.updated.date)) |>

    dplyr::select(sia.code, sia.sub.activity.code, everything())

  return(sia.clean.01)

}

#' Manager function for running cluster_dates() function
#' @description
#' manager function to run the cluster_dates() function using helper function s3_run_cluster_dates to cluster SIAs by type
#' @import dplyr
#' @param sia `tibble` tibble of SIAs to identify rounds by vaccine type
#' @returns `NULL` silently.
#' @keywords internal
#'
s3_sia_cluster_dates <- function(sia){

  sia.01 <- sia |>
    dplyr::filter(yr.sia >= 2010)

  #get all vax types in SIA data
  vax.types <- sia |>
    dplyr::pull(vaccine.type) |>
    unique()

  cli::cli_progress_bar("SIA clusters by vaccine type",
                        total = length(vax.types),
                        type = "tasks")

  for(i in vax.types) {
    sia.01 |>
      s3_sia_cluster_dates_by_vax_type(type = i)
    cli::cli_progress_update()
  }

  cli::cli_progress_done()
  invisible()

}

#' Wrapper function for cluster_dates, includes error checking and cluster cache management
#' @description
#' Wrapper around the cluster_dates function to do some error checking
#'
#' @import dplyr
#' @param data `tibble` dataframe on which to run cluster dates function
#' @param cache_folder `str` location of sia cluster cache on CDC EDAV
#' @param min_obs `int` the minumum number of dates needed to consider clustering
#' using k-means versus using the minimum date available, defaults to 4 for
#' global Polio SIAs
#' @param type `str` vaccine type
#' @returns `NULL` silently.
#'
s3_sia_cluster_dates_by_vax_type <- function(data,
                                 cache_folder = file.path(Sys.getenv("POLIS_DATA_FOLDER"),
                                                          "misc",
                                                          "sia_cluster_cache"),
                                 min_obs = 4,
                                 type){

  #check which locations meet minimum obs requirements
  in_data <- data |>
    dplyr::filter(vaccine.type == type) |>
    dplyr::group_by(adm2guid) |>
    dplyr::summarize(count = n())

  #check if cache exists
  cache_exists <- tidypolis_io(io = "exists.file",
                               file_path = paste0(cache_folder, "/", type, "_cluster_cache.rds"))

  if(cache_exists){

    invisible(capture.output(
      cache <- tidypolis_io(io = "read",
                            file_path = paste0(cache_folder, "/", type, "_cluster_cache.rds"))

    ))

    in_data <- setdiff(in_data, cache)

    cli::cli_alert_info(paste0(nrow(in_data),
                               " potentially new SIAs in [",
                               type,
                               "] found for clustering analysis"))

    #drop cache rows where the adm2guid is in in_data with a different count
    cache <- cache |>
      dplyr::filter(!(adm2guid %in% in_data$adm2guid))

    invisible(capture.output(
      tidypolis_io(obj = dplyr::bind_rows(in_data, cache),
                   io = "write",
                   file_path = paste0(cache_folder, "/", type,"_cluster_cache.rds"))
    ))

  }else{

    cli::cli_alert_info(paste0("No cache found for [",
                               type,
                               "], creating cache and running clustering for ",
                               nrow(in_data),
                               " SIAs"))

    invisible(capture.output(
      tidypolis_io(obj = in_data,
                   io = "write",
                   file_path = paste0(cache_folder, "/", type,"_cluster_cache.rds"))
    ))

  }

  if(nrow(in_data) > 0){
    cli::cli_process_start("Clustering new SIA data",
                           msg_done = "SIA clustering completed")
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

      out <- dplyr::bind_rows(out, out2)
    }

    #data cache
    data_cache_exists <- tidypolis_io(io = "exists.file",
                                      file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))

    if(data_cache_exists){

      #read in existing cache
      invisible(capture.output(
        data_cache <- tidypolis_io(io = "read",
                                   file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))
      ))

      #merge in cache with new run
      out <- filter(data_cache, !sia.sub.activity.code %in% unique(out$sia.sub.activity.code)) |>
        dplyr::bind_rows(out)

      #write out cache
      invisible(capture.output(
        tidypolis_io(obj = out,
                     io = "write",
                     file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))
      ))



    }else{
      #no cache found, write out a fresh data cache
      cli::cli_alert_info(paste0("No data cache found for [",
                                 type,
                                 "], creating data cache and saving clustering results for ",
                                 nrow(out),
                                 " SIAs"))

      invisible(capture.output(
        tidypolis_io(obj = out,
                     io = "write",
                     file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))

      ))
    }

  }else{
    #no new SIA data found for vaccine type, loading in cache
    cli::cli_alert_info(paste0("No new SIA data found for [",
                               type,
                               "], loading cached data!"))
    invisible(capture.output(
      out <- tidypolis_io(io = "read",
                          file_path = paste0(cache_folder, "/", type, "data_cluster_cache.rds"))

    ))

  }

  return(out)
}

#' Merge and add in all cached cluster data
#' @description
#' s3_sia_cluster_dates() writes out the output of previously cached
#' clustered data into the local cache. This functions reads in all cached
#' clustered data and merged it into the existing SIA data
#'
#' @param sia.clean.01 `tibble` all cleaned and historical SIA data without rounds
#' @param polis_folder `str` Path to the POLIS folder.
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `NULL` silently.
#' @keywords internal
#'
s3_sia_merge_cluster_dates_final_data <- function(
    sia.clean.01,
    polis_folder = Sys.getenv("POLIS_DATA_FOLDER"),
    polis_data_folder = file.path(polis_folder, "data"),
    output_folder_name
){

  cli::cli_process_start("Reading in cached SIA cluster data")
  sia.clusters <- dplyr::tibble(name = tidypolis_io(io = "list",
                                                    file_path = file.path(polis_folder,
                                                                          "misc",
                                                                          "sia_cluster_cache"),
                                                    full_names = TRUE)) |>
    dplyr::filter(grepl("data_cluster_cache", name)) |>
    dplyr::pull(name)

  invisible(capture.output(
    sia.rounds <- lapply(
      sia.clusters,
      function(x){
        tidypolis_io(io = "read", file_path = x)
      }
    )
  ))

  cli::cli_process_done()

  cli::cli_process_start("Merging SIA clustered round data into primary SIA output")

  sia.rounds <- sia.rounds |>
    dplyr::bind_rows() |>
    dplyr::arrange(adm2guid, sub.activity.start.date) |>
    dplyr::group_by(adm2guid, vaccine.type, cluster) |>
    dplyr::mutate(cdc.round.num = row_number()) |>
    dplyr::ungroup() |>
    dplyr::group_by(adm2guid) |>
    dplyr::mutate(cdc.max.round = max(sub.activity.start.date)) |>
    dplyr::ungroup() |>
    dplyr::mutate(cdc.last.camp = ifelse(cdc.max.round == sub.activity.start.date, 1, 0))

  sia.clean.02 <- dplyr::left_join(
    sia.clean.01,
    sia.rounds |>
      dplyr::select(
        sia.code, sia.sub.activity.code, adm2guid,
        cluster, cluster_method, cdc.round.num,
        cdc.max.round, cdc.last.camp),
    by = c("sia.code", "sia.sub.activity.code", "adm2guid"))

  cli::cli_process_done()

  cli::cli_process_start("Writing out final SIA dataset")

  invisible(capture.output(
    tidypolis_io(obj = sia.clean.02,
                 io = "write",
                 file_path = paste(
                   polis_data_folder,
                   "/", output_folder_name, "/",
                   paste("sia", min(sia.clean.02$yr.sia, na.rm = T),
                         max(sia.clean.02$yr.sia, na.rm = T),
                         sep = "_"),
                   ".rds",
                   sep = ""
                 ))
  ))
  cli::cli_process_done()
  invisible()

}

#' Evaluate SIA data with unmatched spatial data
#' @description
#' Looks at the output from the GUID matching for SIAs and evalutes
#' all the SIAs that did not have a matching GUID
#'
#' @param sia.05 `tibble` the output of s3_sia_check_guids()
#' @param polis_data_folder The POLIS data folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `NULL` silently.
#' @keywords internal
#'
s3_sia_evaluate_unmatched_guids <- function(sia.05, polis_data_folder, output_folder_name){

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

  invisible(capture.output(
    tidypolis_io(obj = cty.yr.mismatch,
                 io = "write",
                 file_path = paste(
                   polis_data_folder,
                   "/", output_folder_name, "/",
                   paste("ctry_sia_mismatch",
                         min(cty.yr.mismatch$yr.sia, na.rm = T),
                         max(cty.yr.mismatch$yr.sia, na.rm = T),
                         sep = "_"),
                   ".csv",
                   sep = ""
                 ))
  ))

  cli::cli_process_done()
  invisible()

}

###### Step 4 Private Functions ----

#' Process ES Data Pipeline
#'
#' This function processes ES data through multiple standardization and
#' validation steps, including checking for duplicates, validating
#' ES sites and checking against previous downloads
#'
#' @param polis_folder str: location of the POLIS data folder
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param latest_folder_in_archive `str` Name of the latest folder in the archive.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @export
s4_fully_process_es_data <- function(
    polis_folder,
    polis_data_folder, latest_folder_in_archive,
    output_folder_name){

  if (!tidypolis_io(io = "exists.dir",
                    file_path = file.path(polis_data_folder, output_folder_name))) {
    cli::cli_abort("Please run Step 1 and create a Core Ready folder before running this step.")
  }

  es.05 <- s4_es_load_data(polis_data_folder = polis_data_folder,
                           latest_folder_in_archive = latest_folder_in_archive,
                           output_folder_name = output_folder_name) |>
    s4_es_data_processing(output_folder_name = output_folder_name,
                          polis_data_folder = polis_data_folder) |>
    s4_es_validate_sites() |>
    s4_es_create_cdc_vars(
      polis_folder = polis_folder,
      output_folder_name = output_folder_name)

  s4_es_check_metadata(
    polis_data_folder = polis_data_folder,
    es.05 = es.05, output_folder_name = output_folder_name,
    latest_folder_in_archive)

  s4_es_write_data(
    polis_data_folder = polis_data_folder,
    es.05 = es.05, output_folder_name = output_folder_name)

  rm("es.05")

  invisible(gc())

}

#' Load and check ES data against previous downloads
#'
#' @param polis_data_folder `str` Path to the POLIS data folder.
#' @param latest_folder_in_archive `str` Time stamp of latest folder in archive
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `tiblle` es.01.new - the latest SIA data quality checked for variable
#' stability against the last download if it exists
#' @keywords internal
#'
s4_es_load_data <- function(polis_data_folder, latest_folder_in_archive,
                            output_folder_name){

  # Step 1: Read in "old" data file (System to find "Old" data file)
  x <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name,
                                          "Archive",
                                          latest_folder_in_archive),
                    full_names = T)

  y <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name),
                    full_names = T)

  old.file <- x[grepl("EnvSamples",x)]

  new.file <- y[grepl("EnvSamples", y)]

  invisible(capture.output(
    es.01.new <- tidypolis_io(io = "read", file_path = new.file) |>
      dplyr::mutate_all(as.character) |>
      dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
      dplyr::mutate_all(list(~dplyr::na_if(.,"")))
  ))

  names(es.01.new) <- stringr::str_to_lower(names(es.01.new))

  if(length(old.file) > 0){
    invisible(capture.output(
      es.01.old <- tidypolis_io(io = "read", file_path = old.file) |>
        dplyr::mutate_all(as.character) |>
        dplyr::rename_all(function(x) gsub(" ", ".", x)) |>
        dplyr::mutate_all(list(~dplyr::na_if(.,"")))
    ))

    # Modifing POLIS variable names to make them easier to work with
    names(es.01.old) <- stringr::str_to_lower(names(es.01.old))

    # Are there differences in the names of the columns between two downloads?
    f.compare.dataframe.cols(es.01.old, es.01.new)

    var.list.01 <- c(
      "env.sample.id", "env.sample.manual.edit.id", "sample.id",
      "worksheet.name", "labid", "site.comment", "y", "x", "collection.date",
      "npev", "under.process", "is.suspected","advanced.notification",
      "date.shipped.to.ref.lab", "region.id", "region.official.name",
      "admin.0.officialname", "admin.1.id", "admin.1.officialname",
      "admin.2.id", "admin.2.officialname", "updated.date", "publish.date",
      "uploaded.date", "uploaded.by",  "reporting.year",
      "date.notification.to.hq", "date.received.in.lab", "created.date",
      "date.f1.ref.itd", "date.f2.ref.itd", "date.f3.ref.itd",
      "date.f4.ref.itd","date.f5.ref.itd", "date.f6.ref.itd",
      "date.final.culture.result", "date.final.results.reported",
      "date.final.combined.result", "date.isol.sent.seq2", "date.isol.rec.seq2",
      "date.final.seq.result", "date.res.sent.out.vaccine2",
      "date.res.sent.out.vdpv2", "nt.changes"
    )

    ## Exclude the variables from
    es.02.old <- es.01.old |>
      dplyr::select(-dplyr::all_of(var.list.01))

    es.02.new <- es.01.new |>
      dplyr::select(-dplyr::all_of(var.list.01))

    new.var.es.01 <- f.download.compare.01(old.download = es.02.old, new.download = es.02.new)

    new.df <- new.var.es.01 |>
      dplyr::filter(is.na(old.distinct.01) | diff.distinct.01 >= 1) |>
      dplyr::filter(variable != "id")

    if (nrow(new.df) >= 1) {
      cli::cli_alert_danger("There is either a new variable in the ES data or new value of an existing variable.
          Please run f.download.compare.01 to see what it is. Preprocessing can not continue until this is adressed.")

      es.new.value <- f.download.compare.02(
        new.var.es.01 |>
          dplyr::filter(!(is.na(old.distinct.01)) & variable != "id"),
        es.02.old, es.02.new, type = "ES")

    }else{
      cli::cli_alert_info("No variable change errors")
    }

    remove("es.01.old", "es.02.old")
  }else{
    cli::cli_alert_info("No ES file found in archives")
  }

  return(es.01.new)

}

#' Convert variables in POLIS download to to cleaned outputs
#'
#' @param es.01.new `tibble` The latest ES download with variables checked
#' against the last download
#' @param startyr `int` The subset of years for which to process ES data
#' @param endyr `int` The subset of years for which to process ES data
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#' @param polis_data_folder `str` Path to the POLIS data folder.
#'
#' @returns `tibble` es.02 SIA data with outputs validated
#' @keywords internal
#'
s4_es_data_processing <- function(es.01.new, startyr, endyr,
                                  output_folder_name,
                                  polis_data_folder){

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
      ev.detect = dplyr::case_when((
        dplyr::if_any( c("vaccine.1", "vaccine.2", "vaccine.3", "nvaccine.2",
                         "vdpv.1", "vdpv.2", "vdpv.3",
                         "wild.1", "wild.3"), ~stringr::str_detect(., "Yes")) |
          npev==1 |
          stringr::str_detect(virus.type, "WILD") |
          stringr::str_detect(virus.type, "VDPV") |
          stringr::str_detect(virus.type, "VACCINE") |
          stringr::str_detect(virus.type, "NPE") |
          stringr::str_detect(final.combined.rtpcr.results, "PV") |
          stringr::str_detect(final.combined.rtpcr.results, "NPE") |
          stringr::str_detect(final.cell.culture.result, "Poliovirus") |
          stringr::str_detect(final.cell.culture.result, "NPENT")) ~ 1,
        TRUE ~ 0),
      ctry.guid = ifelse(
        is.na(ctry.guid) | ctry.guid == "",
        NA, paste("{", stringr::str_to_upper(ctry.guid), "}", sep = "")),
      prov.guid = ifelse(
        is.na(prov.guid) | prov.guid == "",
        NA,
        paste("{", stringr::str_to_upper(prov.guid), "}", sep = "")),
      dist.guid = ifelse(
        is.na(dist.guid) | dist.guid == "",
        NA,
        paste("{", stringr::str_to_upper(dist.guid), "}", sep = ""))
    ) |>
    dplyr::mutate_at(c("ctry", "province", "district"),
                     list(~stringr::str_trim(stringr::str_to_upper(.), "both"))) |>
    dplyr::distinct()


  # Make sure 'env.sample.maual.edit.id' is unique for each ENV sample
  es.00 <- es.02[duplicated(es.02$env.sample.manual.edit.id), ]

  # Script below will stop further execution if there is a duplicate ENV sample manual id
  if (nrow(es.00) >= 1) {
    cli::cli_alert_danger("Duplicate ENV sample manual ids. Flag for POLIS.
                            Output in duplicate_ES_sampleID_Polis.csv.")

    invisible(capture.output(
      tidypolis_io(obj = es.00, io = "write",
                   file_path =  paste0(polis_data_folder, "/", output_folder_name,
                                       "/duplicate_ES_sampleID_Polis.csv"))
    ))

  } else {
    cli::cli_alert_info("No duplicates identified")
  }


  # find out duplicate ES samples even though they have different
  # 'env.sample.manual.edit.id' from same site, same date, with same virus type

  es.dup.01 <- es.02 |>
    #dplyr::filter(sabin==1) |>
    dplyr::group_by(env.sample.id, virus.type, emergence.group,
                    nt.changes, site.id, collection.date, collect.yr) |>
    dplyr::mutate(es.dups=dplyr::n()) |>
    dplyr::filter(es.dups >=2)|>
    dplyr::select(env.sample.manual.edit.id, env.sample.id, sample.id,
                  site.id, site.code, site.name, sample.condition,
                  collection.date, virus.type, nt.changes, emergence.group,
                  ctry, collect.date, collect.yr, es.dups )

  # Script below will stop further execution if there is a duplicate ENV sabin
  # sample from same site, same date, with same virus type
  if (nrow(es.dup.01) >= 1) {
    cli::cli_alert_warning("Duplicate ENV sample. Check the data for duplicate
                             records. If they are the exact same, then contact POLIS")
    cli::cli_alert_warning(
      paste0("Writing out ES duplicates file: ",
             polis_data_folder,
             "/", output_folder_name, "/duplicate_ES_Polis.csv -",
             " please check, continuing processing"))
    es.dup.01 <- es.dup.01[order(es.dup.01$env.sample.id,es.dup.01$virus.type, es.dup.01$collect.yr),] |>
      dplyr::select(-es.dups)

    # Export duplicate viruses in the CSV file:
    invisible(capture.output(
      tidypolis_io(obj = es.dup.01, io = "write",
                   file_path = paste0(polis_data_folder, "/", output_folder_name,
                                      "/duplicate_ES_Polis.csv"))
    ))

  } else {
    cli::cli_alert_info("No duplicates identified")
  }

  remove("es.dup.01")

  cli::cli_process_start("Checking for missingness in key ES vars")
  check_missingness(data = es.02, type = "ES",
                    output_folder_name = output_folder_name)
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

  es.02 <- dplyr::right_join(
    es.space.03,
    es.02,
    by= c("env.sample.manual.edit.id"="env.sample.manual.edit.id")
  ) |>
    dplyr::select(-virus.type) |>
    dplyr::rename(virus.type=virus.type.01) |>
    dplyr::mutate(lat = as.numeric(lat),
                  lng = as.numeric(lng))

  # Check if na in guid of es country, province or district
  na.es.01 <- es.02 |>
    dplyr::summarise_at(dplyr::vars(ctry.guid, prov.guid, dist.guid, lat, lng),
                        list(~sum(is.na(.))))

  remove("es.space.02", "es.space.03", "es.01.new")
  gc(verbose = F)
  cli::cli_process_done()
  return(es.02)

}

#' Validate ES Site names
#'
#' @param es.02 `tibble` Tibble containing all ES data with variable contents
#' validated
#'
#' @returns `tibble` es.02 - the latest SIA data quality checked for variable
#' stability against the last download if it exists, data cleaned and site
#' names validated
#' @keywords internal
#'
s4_es_validate_sites <- function(es.02){

  cli::cli_process_start("Validating ES Sites")

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

  cli::cli_process_done()

  return(es.02)

}

#' Convert variables in POLIS download to to cleaned outputs
#'
#' @param es.02 `tibble` The latest ES download with variables checked
#' against the last download, variables validated and sites checked
#' @param polis_folder `str` Path to the main POLIS folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `tibble` es.05 SIA data with CDC variables enforced
#' @keywords internal
#'
s4_es_create_cdc_vars <- function(es.02, polis_folder, output_folder_name){


  cli::cli_process_start("Creating ES CDC variables")

  es.03 <-  es.02 |>
    dplyr::rename(ADM0_NAME = admin.0,
                  ADM1_NAME = admin.1,
                  ADM2_NAME = admin.2) |>
    dplyr::mutate(GUID = dist.guid) |>
    dplyr::select(-c("district", "ctry", "province"))

  #fix CIV
  es.03 <- es.03 |>
    dplyr::mutate(ADM0_NAME = ifelse(stringr::str_detect(ADM0_NAME, "IVOIRE"),
                                     "COTE D IVOIRE",ADM0_NAME))

  invisible(capture.output(
    global.ctry.01 <- switch(Sys.getenv("POLIS_EDAV_FLAG"),
                             "TRUE" = {
                               sirfunctions::load_clean_ctry_sp(fp = file.path(
                                 "GID/PEB/SIR",
                                 polis_folder,
                                 "misc",
                                 "global.ctry.rds"
                               ))
                             },
                             "FALSE" = {
                               sirfunctions::load_clean_ctry_sp(
                                 fp = file.path(
                                   polis_folder,
                                   "misc",
                                   "global.ctry.rds"
                                 ),
                                 edav = FALSE
                               )
                             })
  ))

  sf::sf_use_s2(F)
  shape.name.01 <- global.ctry.01 |>
    dplyr::select(ISO_3_CODE, ADM0_NAME.rep = ADM0_NAME) |>
    dplyr::distinct(.keep_all = T)
  sf::sf_use_s2(T)

  savescipen <- getOption("scipen")
  options(scipen = 999)

  es.04 <- dplyr::left_join(
    es.03,
    shape.name.01,
    by = c("country.iso3" = "ISO_3_CODE"),
    relationship = "many-to-many"
  ) |>
    dplyr::mutate(ADM0_NAME = ifelse(is.na(ADM0_NAME), ADM0_NAME.rep, ADM0_NAME)) |>
    dplyr::select(-c("ADM0_NAME.rep", "Shape")) |>
    dplyr::mutate(lat = as.character(lat),
                  lng = as.character(lng)) |>
    dplyr::mutate_at(c("admin.2.id", "region.id", "reporting.week",
                       "reporting.year", "env.sample.manual.edit.id",
                       "site.id", "sample.id", "admin.0.id", "admin.1.id"),
                     ~as.numeric(.)) |>
    dplyr::distinct()

  es.05 <- remove_character_dates(type = "ES", df = es.04,
                                  output_folder_name = output_folder_name)

  options(scipen = savescipen)

  cli::cli_process_done()

  return(es.05)

}

#' Compare ES outputs with metadata from previous output
#'
#' @inheritParams s4_fully_process_es_data
#' @param es.05 `tibble` The latest ES download with variables checked
#' against the last download, variables validated and sites checked and
#' CDC variables enforced
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `NULL` invisible return with write out to logs if necessary
#' @keywords internal
#'
s4_es_check_metadata <- function(polis_data_folder, es.05,
                                 output_folder_name,
                                 latest_folder_in_archive){

  cli::cli_process_start("Checking metadata with previous data")

  #Compare the final file to last week's final file to identify any
  #differences in var_names, var_classes, or categorical responses

  old.es.file <- tidypolis_io(
    io = "list",
    file_path = file.path(
      polis_data_folder,
      output_folder_name,
      "Archive",
      latest_folder_in_archive),
    full_names = T)

  old.es.file <- old.es.file[grepl("es_2001-01-08", old.es.file)]

  if(length(old.es.file) > 0){

    invisible(capture.output(
      old.es <- tidypolis_io(io = "read", file_path = old.es.file)
    ))

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
                                    dplyr::select(-c(setdiff(colnames(new), colnames(old))))),
                        by="env.sample.manual.edit.id") |>
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

    update_polis_log(.event =
                       paste0("ES New Records: ",
                              nrow(in_new_not_old), "; ",
                              "ES Removed Records: ",
                              nrow(in_old_not_new), "; ",
                              "ES Modified Records: ",
                              length(unique(in_new_and_old_but_modified$env.sample.manual.edit.id))),
                     .event_type = "INFO")


    cli::cli_process_done()
  }else{
    cli::cli_process_done()
    cli::cli_alert_info("No old ES file found")
  }

  invisible()

}

#' Write out final ES data
#'
#' @inheritParams s4_fully_process_es_data
#' @param es.05 `tibble` The latest ES download with variables checked
#' against the last download, variables validated and sites checked and
#' CDC variables enforced
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#' @returns `NULL` invisible return with write out to logs if necessary
#' @keywords internal
#'
s4_es_write_data <- function(polis_data_folder, es.05, output_folder_name){

  cli::cli_process_start("Writing out ES datasets")

  invisible(capture.output(
    tidypolis_io(
      obj = es.05,
      io = "write",
      file_path = paste(
        polis_data_folder,
        "/",
        output_folder_name,
        "/",
        paste("es", min(es.05$collect.date, na.rm = T),
              max(es.05$collect.date, na.rm = T), sep = "_"),
        ".rds",
        sep = ""
      ))
  ))

  cli::cli_process_done()

  update_polis_log(.event = paste0("ES finished"),
                   .event_type = "PROCESS")

}


###### Step 5 Private Functions ----

#' Process positives data pipeline
#'
#' @description
#' This function processes virus data through multiple standardization and
#' validation steps, including checking for duplicates, standardizing dates,
#' classifying cases
#'
#' @param polis_folder `str` Path to the main POLIS folder.
#' @param latest_folder_in_archive `str` Name of the latest folder in the archive, if one exists.
#' @param long.global.dist.01 `sf` Global district lookup table for GUID
#' @param polis_data_folder `str` The data folder within the POLIS folder.
#' @param output_folder_name str: Name of the output directory where processed
#'        files will be saved. Defaults to "Core_Ready_Files". For
#'        region-specific processing, this should be set to
#'        "Core_Ready_Files_[REGION]" (e.g., "Core_Ready_Files_AFRO").
#'
#'   validation.
#' @returns `NULL` quietly upon success.
#'
#' @export
s5_fully_process_pos_data <- function(polis_folder,
                                      latest_folder_in_archive,
                                      long.global.dist.01,
                                      polis_data_folder = file.path(polis_folder, "data"),
                                      output_folder_name) {

  virus.raw.new <- s5_pos_load_data(polis_data_folder, latest_folder_in_archive,
                                    output_folder_name)
  virus.01 <- s5_pos_create_cdc_vars(virus.raw.new, polis_folder, polis_data_folder)

  s5_pos_check_duplicates(virus.01, polis_data_folder, output_folder_name)
  s5_pos_write_missing_onsets(virus.01, polis_data_folder, output_folder_name)

  human.virus.05 <- s5_pos_process_human_virus(virus.01, polis_data_folder,
                                               output_folder_name)

  env.virus.04 <- s5_pos_process_es_virus(virus.01, polis_data_folder,
                                          output_folder_name)

  afp.es.virus.01 <- s5_pos_create_final_virus_data(human.virus.05, env.virus.04)
  afp.es.virus.02 <- remove_character_dates(type = "POS", df = afp.es.virus.01,
                                            output_folder_name = output_folder_name)
  afp.es.virus.03 <- create_response_vars(pos = afp.es.virus.02,
                                          output_folder_name = output_folder_name)

  rm(afp.es.virus.02)

  s5_pos_compare_with_archive(afp.es.virus.01, afp.es.virus.03,
                              polis_data_folder, latest_folder_in_archive,
                              output_folder_name = output_folder_name)

  s5_pos_evaluate_unmatched_guids(afp.es.virus.03, long.global.dist.01,
                                  polis_data_folder,
                                  output_folder_name = output_folder_name)

  update_polis_log(.event = "Positives file finished",
                   .event_type = "PROCESS")

  invisible(gc())

}

#' Loads the most recent positives dataset
#'
#' @description
#' Loads the most recent positives dataset and archives the previous
#' positives dataset.
#'
#' @inheritParams s5_fully_process_pos_data
#'
#' @returns `tibble` New virus dataset.
#' @keywords internal
#'
s5_pos_load_data <- function(polis_data_folder, latest_folder_in_archive,
                             output_folder_name) {
  update_polis_log(.event = "Creating Positives analytic datasets",
                   .event_type = "PROCESS")

  cli::cli_process_start("Loading new and old virus data")

  # Step 1: Read in "old" data file (System to find "Old" data file)
  x <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name, "Archive",
                                          latest_folder_in_archive),
                    full_names = T)

  y <- tidypolis_io(io = "list",
                    file_path = file.path(polis_data_folder,
                                          output_folder_name),
                    full_names = T)

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
                                        "pons.seq.date", "pons.administration.type", "pons.spec.type", "location", "country.iso2",
                                        "nt.changes", "location"))) # list of variables we want evaluated in 2nd QC function


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

      virus.new.value <- f.download.compare.02(new.var.virus.01, virus.raw.old.comp, virus.raw.new.comp, type = "POS")

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

  print(virus.types.names)

  return(virus.raw.new)
}

#' Create CDC variables
#'
#' @description
#' Create variables used in CDC data analysis.
#'
#'
#' @param virus.raw.new `tibble` Output of [s5_pos_load_data()].
#' @inheritParams s5_fully_process_pos_data
#'
#' @returns `tibble` Raw virus data with additional CDC columns.
#' @keywords internal
#'
s5_pos_create_cdc_vars <- function(virus.raw.new, polis_folder, polis_data_folder) {
  cli::cli_process_start("Creating CDC variables")

  #read in list of novel emergences supplied by ORPG
  nopv.emrg <- tidypolis_io(io = "read",
                            file_path = file.path(polis_folder,
                                                  "misc",
                                                  "nopv_emg.table.rds")) |>
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
    dplyr::select(epid, surveillance.type, virustype, virustypename,
                  ntchanges, classificationvdpv, vaccine.source)

  if (nrow(vaccine.6.plus) >= 1) {
    cli::cli_alert_warning("There is a potentially misclassified virus based on ntchanges, check in POLIS and flag on message board. Writing out viruses to check.")
    tidypolis_io(obj = vaccine.6.plus, io = "write",
                 file_path = paste0(polis_data_folder, "/Core_Ready_Files/virus_large_nt_changes.csv"))

    update_polis_log(.event = paste0("Vaccine viruses with 6+ NT changes, flag for POLIS"),
                     .event_type = "ALERT")
  } else {
    cli::cli_alert_info("Virus classifications are correct")
  }

  return(virus.01)

}

#' Check for duplicate entries in the virus table
#'
#' @description
#' Checks for potential duplicate positive records and outputs the result to
#' a csv in `polis_data_folder`
#'
#' @inheritParams s5_fully_process_pos_data
#' @param virus.01 `tibble` Output of [s5_pos_create_cdc_vars()].
#'
#' @returns `NULL` quietly upon success.
#' @keywords internal
#'
s5_pos_check_duplicates <- function(virus.01, polis_data_folder, output_folder_name) {
  virus.dup.01 <- virus.01 |>
    dplyr::select(epid, epid.in.polis, pons.epid, virus.id, polis.case.id, env.sample.id,
                  place.admin.0, surveillance.type, datasource, virustype,
                  dateonset, yronset, ntchanges, emergencegroup) |>
    dplyr::group_by(epid, virustype, dateonset, yronset, ntchanges, emergencegroup) |>
    dplyr::mutate(virus_dup=dplyr::n()) |>
    dplyr::filter(virus_dup>=2)

  # Script below will stop further execution if there is a duplicate virus, with same onset date, virus type, emergence group, ntchanges
  if (nrow(virus.dup.01) >= 1) {
    cli::cli_alert_warning("Duplicate viruses in the virus table data. Check the data for duplicate records.
          If they are the exact same, then contact Ashley")
    virus.dup.01 <- virus.dup.01[order(virus.dup.01$surveillance.type,virus.dup.01$virustype, virus.dup.01$yronset),] |>
      dplyr::select(-virus_dup)

    tidypolis_io(obj = virus.dup.01, io = "write",
                 file_path = paste0(polis_data_folder, "/", output_folder_name, "/duplicate_viruses_Polis_virusTableData.csv"))

    update_polis_log(.event = paste0("Duplicate viruses available in duplicate_viruses_Polis_virusTableData.csv"),
                     .event_type = "ALERT")

  } else {
    cli::cli_alert_info("If no duplicates found, then proceed")
  }
  invisible(gc())
}

#' Obtain positives records with missing onset date
#'
#' @description
#' Obtains the records in the positives dataset without onset date and outputs them to the
#' `polis_data_folder` as a csv.
#'
#' @inheritParams s5_pos_check_duplicates
#'
#' @returns `NULL` quietly upon success.
#' @keywords internal
#'
s5_pos_write_missing_onsets <- function(virus.01, polis_data_folder, output_folder_name) {
  tidypolis_io(io = "write", obj = virus.01 |>
                 dplyr::select(epid, dateonset) |>
                 dplyr::filter(is.na(dateonset)),
               file_path = paste0(polis_data_folder, "/", output_folder_name,
                                  "/virus_missing_onset.csv"))
}

#' Process and clean cases in the positives dataset
#'
#' @description
#' Performs deep cleaning of AFP/non-AFP cases in the positives dataset.
#'
#'
#' @inheritParams s5_pos_check_duplicates
#' @param startyr `int` Start year to process the positives dataset.
#' @param endyr `int` End year to process the positives dataset.
#'
#' @returns `tibble` Cleaned positives dataset containing human cases only.
#' @keywords internal
#'
s5_pos_process_human_virus <- function(virus.01, polis_data_folder, output_folder_name) {

  startyr <- 2000
  endyr <- year(format(Sys.time()))

  # Human virus dataset with sabin 2 and positive viruses only
  human.virus.01 <- virus.01 |>
    dplyr::filter(!surveillance.type=="ENV") |>
    dplyr::filter(!virustype %in% c("VACCINE 1", "VACCINE 3"))

  cli::cli_process_start("Processing and cleaning AFP/non-AFP files")
  afp.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path = file.path(polis_data_folder, output_folder_name), full_names = T)) |>
    dplyr::mutate(short_name = stringr::str_replace(name, paste0(polis_data_folder, "/",
                                                                 output_folder_name,
                                                                 "/"), "")) |>
    dplyr::filter(grepl("^(afp_linelist_2001-01-01_2025).*(.rds)$", short_name)) |>
    dplyr::pull(name)

  tryCatch({
    afp.01 <- lapply(afp.files.01, function(x) tidypolis_io(io = "read", file_path = x)) |>
      dplyr::bind_rows() |>
      dplyr::ungroup() |>
      dplyr::distinct(.keep_all = T)|>
      dplyr::filter(dplyr::between(yronset, startyr, endyr))
  }, error = \(e) {
    cli::cli_abort("Please run Step 2 of preprocessing before Step 5.")
  })


  non.afp.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path = file.path(polis_data_folder, output_folder_name), full_names = T)) |>
    dplyr::mutate(short_name = stringr::str_replace(name, paste0(polis_data_folder, "/", output_folder_name, "/"), "")) |>
    dplyr::filter(grepl("^(other_surveillance_type_linelist_2016_2025).*(.rds)$", short_name)) |>
    dplyr::pull(name)


  if (output_folder_name == "Core_Ready_Files" ||
      length(non.afp.files.01) > 0) {
    tryCatch({
      non.afp.01 <- purrr::map_df(
        non.afp.files.01,
        ~ tidypolis_io(io = "read", file_path = .x)
      ) |>
        dplyr::ungroup() |>
        dplyr::distinct(.keep_all = TRUE) |>
        dplyr::filter(dplyr::between(yronset, startyr, endyr))
    }, error = \(e) {
      cli::cli_abort("Please run Step 2 of preprocessing before Step 5.")
    })
  } else {
    region <- sub("^Core_Ready_Files_", "", output_folder_name)
    cli::cli_warn(
      "No contact or community sampling surveillance data found for {region}"
    )
    non.afp.01 <- tibble::tibble(
      epid = character(),
      lat = numeric(),
      lon = numeric(),
      datenotificationtohq = character()
    )
  }
  # deleting any duplicate records in afp.01.
  afp.01 <- afp.01[!duplicated(afp.01$epid), ] |>
    dplyr::select(epid, dateonset,  place.admin.0, place.admin.1, place.admin.2, admin0guid, yronset, admin1guid, admin2guid, cdc.classification.all,
                  whoregion, nt.changes, emergence.group, virus.cluster, surveillancetypename, lat, lon, vtype.fixed, datenotificationtohq)

  if (length(non.afp.files.01) > 0) {
    # deleting any duplicate records in non.afp.files.01.
    non.afp.01 <- non.afp.01[!duplicated(non.afp.01$epid), ] |>
      dplyr::select(epid, dateonset,  place.admin.0, place.admin.1, place.admin.2, admin0guid, yronset, admin1guid, admin2guid, cdc.classification.all,
                    whoregion, nt.changes, emergence.group, virus.cluster, surveillancetypename, lat, lon, vtype.fixed, datenotificationtohq)
  }


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
  cli::cli_process_done()

  cli::cli_process_start("Removing duplicates from virus table - human")

  human.virus.05 <- human.virus.04 |>
    dplyr::mutate(place.admin.0 =  stringi::stri_trim(place.admin.0, "left"),
                  place.admin.1 =  stringi::stri_trim(place.admin.1, "left"),
                  place.admin.2 =  stringi::stri_trim(place.admin.2, "left"))

  cli::cli_process_done()

  return(human.virus.05)

}

#' Process and clean cases in the positives dataset
#'
#' @description
#' Performs deep cleaning of environmental samples in the positives dataset.
#'
#'
#' @inheritParams s5_pos_check_duplicates
#'
#' @returns `tibble` Cleaned positives dataset containing environmental samples only.
#' @keywords internal
#'
s5_pos_process_es_virus <- function(virus.01, polis_data_folder, output_folder_name) {

  ### ENV data from virus table
  env.virus.01 <- virus.01 |>
    dplyr::filter(surveillance.type == "ENV" & !is.na(virustype) &
                    !virustype %in% c("VACCINE 1", "VACCINE 3", "NPEV"))

  cli::cli_process_start("Adding in ES data")

  # read in ES files from cleaned ENV linelist
  env.files.01 <- dplyr::tibble("name" = tidypolis_io(io = "list", file_path = file.path(polis_data_folder, output_folder_name), full_names = T)) |>
    dplyr::mutate(short_name = stringr::str_replace(name, paste0(polis_data_folder, "/", output_folder_name, "/"), "")) |>
    dplyr::filter(grepl("^(es).*(.rds)$", short_name)) |>
    dplyr::pull(name)

  tryCatch({
    es.01 <- purrr::map_df(env.files.01, ~ tidypolis_io(io = "read", file_path = .x)) |>
      dplyr::ungroup() |>
      dplyr::distinct(.keep_all = T)
  }, error = \(e) {
    cli::cli_abort("Please run Step 4 of preprocessing before Step 5.")
  })

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

  cli::cli_process_start("Removing duplicates from virus table - ES")

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

  cli::cli_process_done()

  return(env.virus.04)

}

#' Combine cleaned human and environmental samples positives dataset
#'
#' @description
#' Combines both the cleaned human and environmental surveillance samples positives
#' datasets into a final cleaned positives dataset.
#'
#' @param human.virus.05 `tibble` Output of [s5_pos_process_human_virus()].
#' @param env.virus.04 `tibble` Output of [s5_pos_process_es_virus()].
#'
#' @returns `tibble` Combined cleaned positives dataset.
#' @keywords internal
#'
s5_pos_create_final_virus_data <- function(human.virus.05, env.virus.04) {
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

  cli::cli_process_done()

  return(afp.es.virus.01)

}

#' Compares the old virus dataset in the latest archive folder with the new processed virus dataset
#'
#' @description
#' Compares the old virus dataset with the new processed virus dataset to determine
#' any changes, including the number of records that changed since the last run. Changes
#' are also added into the `polis_data_folder` as csv files.
#'
#' @param afp.es.virus.01 `tibble` Output of [s5_pos_create_final_virus()].
#' @param afp.es.virus.03 `tibble` Output of [s5_pos_create_final_virus()] but processed
#' further using [remove_character_dates()] and [create_response_vars()].
#' @inheritParams s5_fully_process_pos_data
#'
#' @returns `NULL` quietly upon success.
#' @keywords internal
#'
s5_pos_compare_with_archive <- function(afp.es.virus.01, afp.es.virus.03, polis_data_folder, latest_folder_in_archive, output_folder_name) {
  cli::cli_process_start("Checking for variables that don't match last weeks pull")

  #Compare the final file to last week's final file to identify any differences in var_names, var_classes, or categorical responses
  new_table_metadata <- f.summarise.metadata(afp.es.virus.03)

  x <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, output_folder_name, "Archive", latest_folder_in_archive), full_names = T)

  y <- tidypolis_io(io = "list", file_path = file.path(polis_data_folder, output_folder_name), full_names = T)

  old.file <- x[grepl("positives_2001-01-01", x)]

  if(length(old.file) > 0){

    old.pos <- tidypolis_io(io = "read", file_path = old.file)

    old_table_metadata <- f.summarise.metadata(old.pos)
    positives_metadata_comparison <- f.compare.metadata(new_table_metadata, old_table_metadata, "POS")

    new <- afp.es.virus.03 |>
      unique() |>
      dplyr::mutate(epid = stringr::str_squish(epid)) |>
      dplyr::group_by(epid)|>
      dplyr::slice(1) |>
      dplyr::ungroup() |>
      dplyr::mutate_all(as.character)

    old <- old.pos |>
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
                   file_path = paste0(polis_data_folder, "/", output_folder_name, "/Changed_virustype_virusTableData.csv"))

    }

    # list of records in new but not in old.
    in_new_not_old <- in_new_not_old |>
      dplyr::select(place.admin.0, epid, dateonset, yronset, source, virustype)

    if(nrow(in_new_not_old) > 0 ){
      # Export records for which virus type has changed from last week to this week in the CSV file:
      tidypolis_io(obj = in_new_not_old,
                   io = "write",
                   file_path = paste0(polis_data_folder, "/", output_folder_name, "/in_new_not_old_virusTableData.csv"))
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
                 file_path = paste0(polis_data_folder, "/",
                                    output_folder_name,
                                    "/virus_class_changed_date.csv"))
  }

  update_polis_log(.event = paste0("POS New Records: ", nrow(in_new_not_old), "; ",
                                   "POS Removed Records: ", nrow(in_old_not_new), "; ",
                                   "POS Modified Records: ", length(unique(in_new_and_old_but_modified)), "; ",
                                   "POS Class Changed Records: ", length(unique(class.updated$epid))),
                   .event_type = "INFO")

  tidypolis_io(obj = afp.es.virus.03,
               io = "write",
               file_path = paste(polis_data_folder, "/", output_folder_name, "/",
                                 paste("positives", min(afp.es.virus.03$dateonset, na.rm = T),
                                       max(afp.es.virus.03$dateonset, na.rm = T),
                                       sep = "_"
                                 ),
                                 ".rds",
                                 sep = ""
               ))


  cli::cli_process_done()

  invisible()
}

#'
#'
#' @inheritParams s5_fully_process_pos_data
#' @inheritParams s5_pos_compare_with_archive
#'
#' @returns `NULL` quietly upon success.
#' @keywords internal
#'
s5_pos_evaluate_unmatched_guids <- function(afp.es.virus.03, long.global.dist.01, polis_data_folder, output_folder_name) {
  cli::cli_process_start("Checking for positives that don't match to GUIDs")

  # AFP and ES that do not match to shape file
  unmatched.afp.es.viruses.01 <- dplyr::anti_join(afp.es.virus.03,
                                                  long.global.dist.01, by = c("admin2guid" = "GUID", "yronset" = "active.year.01"))

  # CSV file listing out unmatch virus

  tidypolis_io(obj = unmatched.afp.es.viruses.01,
               io = "write",
               file_path = paste(polis_data_folder, "/", output_folder_name, "/",
                                 paste("unmatch_positives", min(unmatched.afp.es.viruses.01$yronset, na.rm = T),
                                       max(unmatched.afp.es.viruses.01$yronset, na.rm = T),
                                       sep = "_"
                                 ),
                                 ".csv",
                                 sep = ""
               ))

  cli::cli_process_done()

  invisible()
}
