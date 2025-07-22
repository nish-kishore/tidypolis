#' Check POLIS API Endpoints
#' @description Tests WHO POLIS API endpoints for connectivity and response validation
#' @param .table `str` Use "all" or no parameter set for all tables, input table "names" for specific endpoints to test (case-insensitive), input "help" for available options
#' @param api_key `str` validated API key
#' @returns tibble
#' @importFrom tibble tibble
#' @export

check_polis_api_endpoints <- function(...,
                                      .table = NULL,
                                      api_key = Sys.getenv("POLIS_API_KEY"),
                                      cache_file = Sys.getenv("POLIS_CACHE_FILE")) {

  # Get table info
  cache_processed <- readRDS(cache_file) |>
    dplyr::filter(!is.na(table))

  table_metadata <- cache_processed |>
    split(cache_processed$table)

  tables_list <- cache_processed |>
    dplyr::filter(!is.na(.data$polis_id) & !is.na(.data$endpoint)) |>
    dplyr::pull(table)

  rm(cache_processed)

  # User input to select table
  input_tables <- if (length(list(...)) > 0) {
    list(...) |> unlist() |> tolower()
  } else {
    (.table %||% "all") |> tolower()
  }

  # User input processing options
  selected_tables <- if (is.null(input_tables) || length(input_tables) == 0 ||
                         (length(input_tables) == 1 && input_tables == "all")) {
    tables_list

  } else if (length(input_tables) == 1 && input_tables == "help") {
    available_msg <- paste(tables_list, collapse = ", ")
    message("Available tables: ", available_msg)
    return(invisible(tables_list))

  } else {
    found_tables <- input_tables[input_tables %in% tables_list]
    missing_tables <- input_tables[!input_tables %in% tables_list]

    if (length(missing_tables) > 0) {
      if (length(missing_tables) == 1) {
        cli::cli_alert_info("Table '{missing_tables}' doesn't exist or has invalid API configuration (NA polis_id/endpoint). Use check_polis_api_endpoints(\"help\") to see all available tables")
      } else {
        missing_list <- paste(missing_tables, collapse = ", ")
        cli::cli_alert_info("Tables '{missing_list}' do not exist or have invalid API configuration (NA polis_id/endpoint). Use check_polis_api_endpoints(\"help\") to see all available tables")
      }
    }

    if (length(found_tables) > 0) {
      found_tables
    } else {
      return(invisible(NULL))
    }
  }

  results <- list()

  # Process each selected table
  for (.table in selected_tables) {
    start_time <- Sys.time()
    table_data <- table_metadata[[.table]]

    # Build API URL
    api_url <- "https://extranet.who.int/polis/api/v2/" |>
      paste0(table_data$endpoint, "?$inlinecount=allpages&$top=100")

    # Make HTTP request
    response <- httr::RETRY(
      verb = "GET",
      url = api_url,
      config = httr::add_headers("authorization-token" = api_key),
      times = 10,
      pause_min = 2,
      quiet = TRUE,
      terminate_on_success = TRUE
    )

    # Process timing
    check_datetime <- Sys.time()
    time_taken <- check_datetime |>
      difftime(start_time, units = "secs") |>
      as.numeric()

    # Extract HTTP request response
    status_info <- httr::http_status(response)
    success_flag <- status_info$category == "Success"
    status_note <- status_info$message

    # Parse and validate payload
    if (success_flag) {
      json_payload <- tryCatch({
        response$content |> rawToChar() |> jsonlite::fromJSON()
      }, error = function(e) {
        success_flag <<- FALSE
        NULL
      })

      if (!is.null(json_payload)) {
        print(response)
      }
    }

    # Store result
    results[[.table]] <- tibble::tibble(
      table_name = .table, success_flag = success_flag, status_code = response$status_code,
      status_note = status_note, time_taken_sec = round(time_taken, 2), checked_at = check_datetime
    )
  }

  # Return results
  if (length(results) > 0) {
    results |> dplyr::bind_rows()
  } else {
    tibble::tibble()
  }
}
