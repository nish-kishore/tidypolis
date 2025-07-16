#' This function checks one or more endpoint(s) in the POLIS API system.
#'
#' @details
#' Before running this function:
#' - Make sure the tidypolis package is installed.
#' - Set your POLIS API key as an environment variable.
#'
#' @param ... One or more endpoint names as character strings. Accepts names in snake_case, camelCase, or PascalCase; all inputs are internally converted to snake_case.
#' Use "all" or leave blank to check all available endpoints from cache.
#'
#' @return A tibble with status of called API, timing, and diagnostic notes for each endpoint.

check_polis_api_endpoints <- function(...) {
  base_url <- "https://extranet.who.int/polis/api/v2/"

  # Endpoint name in snake_case format: converts camelCase, PascalCase, or Kebab-Case to snake_case
  endpoint_name <- function(name) {
    name <- gsub("-", "_", name)
    tolower(
      gsub("([A-Z]+)([A-Z][a-z])", "\\1_\\2",
           gsub("([a-z0-9])([A-Z])", "\\1_\\2", name)
      )
    )
  }

  # Fetch all endpoint names that exist in the tidypolis cache
  get_valid_cached_tables <- function() {
    # Initialize call to POLIS API base URL for all or user input endpoints
    res <- tryCatch({
      tidypolis::call_single_url(base_url)
    }, error = function(e) {
      message("Failed to get tables: ", e$message)
      return(NULL)
    })

    if (is.null(res)) return(character(0))

    # Extract endpoint names from the API response to build full URLs by concatenating with base URL for each endpoint check
    tidypolis_api_endpoints <- res |>
      (\(x) {
        if (is.data.frame(x) && "endpoint" %in% colnames(x)) {
          x$endpoint
        } else if (is.list(x)) {
          unlist(x)
        } else {
          message("Invalid API structure response: verify if the API base URL or response format has changed.")
          character(0)
        }
      })()

    available_tables <- character(0)

    # Check which endpoints have cached data available in tidypolis cache
    for (.table in tidypolis_api_endpoints) {
      transformed_name <- endpoint_name(.table)

      success <- tryCatch({
        capture.output(
          data <- get_polis_cache(.table = transformed_name),
          type = "message"
        ) |>
          (\(msgs) !any(grepl("No entry found in the cache table", msgs)))()
      }, error = function(e) FALSE)

      if (success) {
        available_tables <- c(available_tables, transformed_name)
      }
    }

    unique(available_tables)
  }

  # Parse user input endpoints or fetch all cached endpoints in tidypolis by default
  tables <- c(...) |>
    (\(x) {
      if (length(x) == 0 || (length(x) == 1 && tolower(x[1]) == "all")) {
        get_valid_cached_tables()
      } else {
        endpoint_name(x)
      }
    })()

  results <- list()

  # Loop through user-specified or all endpoints and attempt to retrieve cached data
  for (.table in tables) {
    table_data <- tryCatch(
      get_polis_cache(.table = .table),
      error = function(e) NULL
    )

    # STATUS: Error when the cache does not contain data for this endpoint
    if (is.null(table_data) || !is.data.frame(table_data) || is.null(table_data$endpoint)) {
      results[[.table]] <- tibble::tibble(
        table = .table,
        url = NA_character_,
        status = "error",
        note = "No entry found in cache table for this endpoint",
        time_taken_sec = NA_real_,
        checked_at = Sys.time()
      )
      next
    }

    # Build full API URL
    table_url <- paste0(base_url, table_data$endpoint)

    # Time the API call and handle request with timeout, capturing duration
    start_time <- Sys.time()

    res <- tryCatch({
      setTimeLimit(elapsed = 300, transient = TRUE)
      on.exit(setTimeLimit(cpu = Inf, elapsed = Inf, transient = FALSE), add = TRUE)
      tidypolis::call_single_url(table_url)
    }, error = function(e) e)

    time_taken <- difftime(Sys.time(), start_time, units = "secs") |>
      as.numeric() |>
      round(2)

    # STATUS: Failed API calls due to server errors or timeouts
    if (inherits(res, "error")) {
      results[[.table]] <- tibble::tibble(
        table = .table,
        url = table_url,
        status = "down_api",
        note = "API call failed (server unavailable or timed out)",
        time_taken_sec = time_taken,
        checked_at = Sys.time()
      )

      # STATUS: API called but returned no records to validate data presence
    } else if (nrow(res) == 0) {
      results[[.table]] <- tibble::tibble(
        table = .table,
        url = table_url,
        status = "no_data_api_call",
        note = "API responded but returned no data",
        time_taken_sec = time_taken,
        checked_at = Sys.time()
      )

      # STATUS: Successful API call and data retrieved for validation
    } else {
      results[[.table]] <- tibble::tibble(
        table = .table,
        url = table_url,
        status = "success_api_called",
        note = "Successful API call with data presenece validated",
        time_taken_sec = time_taken,
        checked_at = Sys.time()
      )

      # Preview first few rows of successful API calls with returned data
      dplyr::slice(res, 1:min(3, nrow(res))) |> invisible()
      rm(res)
      gc()
    }
  }

  # Combine each endpoint's API call result into one table and print for user
  dplyr::bind_rows(results) |> print()
}
