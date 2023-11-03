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

  #check if ID API works
  api_url <-
    paste0(base_url,
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

    cli::cli_process_start("Downloading data")
    out <- call_urls(urls)
    update_polis_log(.event = paste0(
      "Downloaded ",
      table_size,
      " rows of ",
      table_data$table,
      " data"
    ))
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
        .update_val = max(lubridate::as_datetime(dplyr::pull(out[table_data$polis_update_id])))
      )
    }

    cli::cli_process_done()

    cli::cli_process_start("Writing data cache")
    readr::write_rds(out, file = paste0(
      Sys.getenv("POLIS_DATA_CACHE"),
      "/",
      table_data$table,
      ".rds"
    ))
    update_polis_log(.event = paste0(table_data$table, " data saved locally"))
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
        )
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
          "rows of ",
          table_data$table,
          " data"
        ))
        cli::cli_process_done()

        #check ids and make list of ids to be deleted
        cli::cli_process_start("Getting table Ids")
        ids <-
          get_table_ids(.table = table_data$table, .id = table_data$polis_id)
        cli::cli_process_done()

        #load in cache
        cli::cli_process_start("Loading existing cache")
        old_cache <-
          readr::read_rds(paste0(
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
          )
        )

        #update cache
        old_cache <-
          old_cache |> dplyr::filter(!get(table_data$polis_id) %in% dplyr::pull(out[table_data$polis_id]))
        old_cache <-
          bind_and_reconcile(new_data = out, old_data = old_cache)

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
        readr::write_rds(old_cache,
                         file = paste0(
                           Sys.getenv("POLIS_DATA_CACHE"),
                           "/",
                           table_data$table,
                           ".rds"
                         ))
        update_polis_log(.event = paste0(table_data$table, " data saved locally"))
        cli::cli_process_done()

        #garbage clean
        gc()

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
#' @param .event str: event to be logged
#' @returns Return true if cache updated
update_polis_log <- function(log_file = Sys.getenv("POLIS_LOG_FILE"),
                             .time = Sys.time(),
                             .user = Sys.getenv("USERNAME"),
                             .event) {
  readr::read_rds(log_file) |>
    tibble::add_row(time = .time,
                    user = .user,
                    event = .event) |>
    readr::write_rds(log_file)

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
  cache <- readr::read_rds(cache_file)

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
  readr::read_rds(cache_file) |>
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
    readr::write_rds(cache_file)

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
    file_loc = file.path("","", "cdc.gov/project/CGH_GID_Active/PEB/SIR/",
                         "DATA/Core 2.0/preprocessing/GetPOLIS",
                         "api_web_core_crosswalk.xlsx")
  ){
  cli::cli_process_start("Import crosswalk")
  crosswalk <-
    rio::import(file_loc) |>
    #TrendID removed from export
    dplyr::filter(!API_Name %in% c("Admin0TrendId", "Admin0Iso2Code"))
  cli::cli_process_done()
  return(crosswalk)
}


#### Pre-processing ####


#' Preprocess data process data in the CDC style
#'
#' @description
#' Process POLIS data into analytic datasets needed for CDC
#' @import cli sirfunctions dplyr readr lubridate stringr rio tidyr
#' @param polis_data_folder str: location of the POLIS data folder
#' @return Outputs intermediary core ready files
preprocess_cdc <- function(polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")) {
  #Read in the updated API datasets
  cli::cli_h1("Loading data")

  cli::cli_process_start("Case")
  api_case_2019_12_01_onward <-
    readr::read_rds(paste0(polis_data_folder, "/case.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Environmental Samples")
  api_es_complete <-
    readr::read_rds(paste0(polis_data_folder, "/environmental_sample.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Sub-activity")
  api_subactivity_complete <-
    readr::read_rds(paste0(polis_data_folder, "/sub_activity.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Virus")
  api_virus_complete <-
    readr::read_rds(paste0(polis_data_folder, "/virus.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()

  cli::cli_process_start("Activity")
  api_activity_complete <-
    readr::read_rds(paste0(polis_data_folder, "/activity.rds")) |>
    dplyr::mutate_all(as.character)
  cli::cli_process_done()


  #Get geodatabase to auto-fill missing GUIDs
  cli::cli_process_start("Long district spatial file")
  long.global.dist.01 <-
    sirfunctions::load_clean_dist_sp(type = "long")
  cli::cli_process_done()


  cli::cli_h1("De-duplicating data")

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
  cli::cli_h1("Crosswalk and rename variables")

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

  cli::cli_h1("Modifying and reconciling variable types")
  #    Modify individual variables in the API files to match the coding in the web-interface downloads,
  #    and retain only variables from the API files that are present in the web-interface downloads.
  cli::cli_process_start("Step 1")
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

  cli::cli_process_start("Step 2")
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

  cli::cli_process_start("Step 3")
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

  cli::cli_process_start("Step 4")
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

  cli::cli_process_start("Step 5")

  cli::cli_h3("Case")
  api_case_sub3 <- remove_empty_columns(api_case_sub3)

  cli::cli_h3("Sub-activity")
  api_subactivity_sub4 <- remove_empty_columns(api_subactivity_sub4)

  cli::cli_h3("ES")
  api_es_sub3 <- remove_empty_columns(api_es_sub3)

  cli::cli_h3("Virus")
  api_virus_sub3 <- remove_empty_columns(api_virus_sub3)

  cli::cli_process_done()

  #13. Export csv files that match the web download, and create archive and change log
  cli::cli_h1("Creating change log and exporting data")

  cli::cli_process_start("Checking on requisite file structure")
  ts <- Sys.time()
  timestamp <- paste0(lubridate::date(ts),"_",lubridate::hour(ts),"-",lubridate::minute(ts),"-",round(lubridate::second(ts), 0))

  #create directory
  if(dir.exists(file.path(polis_data_folder, "Core_Ready_Files")) == FALSE){
    dir.create(file.path(polis_data_folder, "Core_Ready_Files"))
  }
  if(dir.exists(file.path(polis_data_folder, "Core_Ready_Files", "Archive")) == FALSE){
    dir.create(file.path(polis_data_folder, "Core_Ready_Files", "Archive"))
  }
  if(dir.exists(file.path(polis_data_folder, "Core_Ready_Files", "Archive", timestamp)) == FALSE){
    dir.create(file.path(polis_data_folder, "Core_Ready_Files", "Archive", timestamp))
  }
  if(dir.exists(file.path(polis_data_folder, "Core_Ready_Files", "Change Log")) == FALSE){
    dir.create(file.path(polis_data_folder, "Core_Ready_Files", "Change Log"))
  }
  if(dir.exists(file.path(polis_data_folder, "Core_Ready_Files", "Change Log", timestamp)) == FALSE){
    dir.create(file.path(polis_data_folder, "Core_Ready_Files", "Change Log", timestamp))
  }
  cli::cli_process_done()
  #Get list of most recent files
  most_recent_files <- list.files(file.path(polis_data_folder, "Core_Ready_Files"))[grepl(".rds", list.files(file.path(polis_data_folder, "Core_Ready_Files")))]

  if(length(most_recent_files)>0){

    for(i in 1:length(most_recent_files)){
      cli::cli_process_start(paste0("Processing data for: ", most_recent_files[i]))
      #compare current dataset to most recent and save summary to change_log
      old <- rio::import(file.path(polis_data_folder, "Core_Ready_Files", most_recent_files[i])) |>
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

      in_new_and_old_but_modified <- dplyr::inner_join(in_new_and_old_but_modified, setdiff(x, new |>
                               dplyr::select(-c(setdiff(colnames(new), colnames(old))))), by="Id") |>
        #wide_to_long
        tidyr::pivot_longer(cols=-Id) |>
        dplyr::mutate(source = ifelse(stringr::str_sub(name, -2) == ".x", "new", "old")) |>
        dplyr::mutate(name = stringr::str_sub(name, 1, -3)) |>
        #long_to_wide
        tidyr::pivot_wider(names_from=source, values_from=value)

      if(nrow(in_new_and_old_but_modified) >= 1){

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
      readr::write_rds(change_summary, file.path(polis_data_folder, "Core_Ready_Files", "Change Log", timestamp, paste0(substr(most_recent_files[i],1,nchar(most_recent_files[i])-4), ".rds")))
      #Move most recent to archive
      rio::export(rio::import(file.path(polis_data_folder, "Core_Ready_Files", most_recent_files[i])), file.path(polis_data_folder, "Core_Ready_Files", "Archive", timestamp, most_recent_files[i]))
      unlink(file.path(polis_data_folder, "Core_Ready_files", most_recent_files[i]))
    }
  }

  cli::cli_process_start("Writing all final Core Ready files")
  #Export files (as csv) to be used as pre-processing starting points
  readr::write_rds(api_case_sub3, file.path(polis_data_folder, "Core_Ready_Files", paste0("Human_Detailed_Dataset_",timestamp,"_from_01_Dec_2019_to_",format(ts, "%d_%b_%Y"),".rds")))
  readr::write_rds(api_subactivity_sub4, file.path(polis_data_folder, "Core_Ready_Files", paste0("Activity_Data_with_All_Sub-Activities_(1_district_per_row)_",timestamp,"_from_01_Jan_2020_to_",format(Sys.Date()+365/2, "%d_%b_%Y"),".rds")))
  readr::write_rds(api_es_sub3, file.path(polis_data_folder, "Core_Ready_Files", paste0("EnvSamples_Detailed_Dataset_",timestamp,"_from_01_Jan_2000_to_",format(ts, "%d_%b_%Y"),".rds")))
  readr::write_rds(api_virus_sub3, file.path(polis_data_folder, "Core_Ready_Files", paste0("Viruses_Detailed_Dataset_",timestamp,"_from_01_Dec_1999_to_",format(ts, "%d_%b_%Y"),".rds")))
  cli::cli_process_done()

  #14. Remove temporary files from working environment, and set scientific notation back to whatever it was originally


}
