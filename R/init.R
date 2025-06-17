#' A function to intitialize a POLIS data folder
#'
#' @description
#' Initialize API Key and local data cache for tidypolis. Inspired by the
#' tidycensus process for managing their API secrets.
#'
#' @import cli yaml tibble dplyr readr lubridate sirfunctions
#' @param polis_folder `str` Location of folder where to store all information from POLIS.
#' @param edav `bool` Should the system use EDAV as it's cache; default `FALSE`.
#' @param api_debug boolean: if true will log all api calls
#' @returns Messages on process.
#' @examples
#' \dontrun{
#' init_tidypolis(polis_folder = "POLIS", data_folder = "Data", edav = T) #create a polis data cache on CDC EDAV
#' init_tidypolis("C:/Users/user_name/Desktop/polis", "C:/Users/user_name/Desktop/data", edav = F)
#' #create a polis data cache on your local desktop
#' }
#' @export
init_tidypolis <- function(
    polis_folder = "POLIS",
    edav = F,
    api_debug = F
    ){

  if(api_debug){
    Sys.setenv("API_DEBUG" = TRUE)
  }else{
    Sys.setenv("API_DEBUG" = FALSE)
  }

  cli::cli_alert_info(paste0("API Debug set to: ", Sys.getenv("API_DEBUG")))

  if(edav){
    tryCatch(
      expr = {
        x <- sirfunctions::edav_io(io = "list")
        rm(x)
        cli::cli_alert_success("EDAV connection verified!")
        },
      error = function(e){
        cli::cli_abort("EDAV connection failed, please try again.")
        }
      )
    Sys.setenv(POLIS_EDAV_FLAG = T)
  }else{
    Sys.setenv(POLIS_EDAV_FLAG = F)
  }

  #check if polis folder exists, if not create it after asking for validation
  if(tidypolis_io(io = "exists.dir", file_path = polis_folder)){
    cli::cli_alert_success("POLIS data folder found!")
  }else{
    val <- request_input(
      request = paste0("Confirm creation of POLIS data folder at '",polis_folder),
      vals = c("Y", "N")
    )


    if(val == "Y"){
      tidypolis_io(io = "create", file_path = polis_folder)
      cli::cli_alert_success(paste0("- POLIS data folder created at'", polis_folder, "'"))
    }else{
      stop(paste0("Please run function again with updated folder location"))
    }
  }
  Sys.setenv(POLIS_DATA_FOLDER = polis_folder)
  #check if sub-folders exist, if not create them

  cli::cli_alert_info("Checking POLIS data folder structure")
  if("data" %in% tidypolis_io(io = "list", file_path = polis_folder)){
    cli::cli_alert_success("Data folder identified")
  }else{
    cli::cli_alert_success("Creating 'data' folder")
    tidypolis_io(io = "create", file_path = paste0(polis_folder, "/data"))
  }
  Sys.setenv(POLIS_DATA_CACHE = paste0(polis_folder,"/data"))

  # Check for required files
  if (!tidypolis_io(io = "exists.dir", file_path = file.path(polis_folder, "misc"))) {
    cli::cli_alert_info("No misc folder inside POLIS folder, creating...")
    tidypolis_io(io = "create", file_path = file.path(polis_folder, "misc"))
    } else {
    cli::cli_alert_success("misc folder found! Checking for required files")
    }

  required_files <- c(
    "crosswalk.rds",
    "env_sites.rds",
    "nopv_emg.table.rds",
    "global.dist.rds",
    "global.prov.rds",
    "global.ctry.rds"
  )
  misc_folder_files <- tidypolis_io(io = "list", file_path = file.path(polis_folder, "misc"))
  missing_files <- setdiff(required_files, misc_folder_files)

  if (length(missing_files > 0)) {
    cli::cli_alert(paste0("Following required files not found in the misc folder: ",
                          paste0(missing_files, collapse = ", ")))
    cli::cli_abort("Please request these files from the CDC PEB SIR Team")
  } else {
    cli::cli_alert_success("All required files present!")
  }

  optional_folder <- "sia_cluster_cache"
  if (!optional_folder %in% misc_folder_files) {
    cli::cli_alert_info(paste0("  - SIA cluster cache not found in the misc folder. ",
                          "SIA clustering algorithm for SIA round numbers will not be performed during preprocessing. ",
                          "\n   - NOTE: Although optional, if you wish to perform this operation, please contact the CDC PEB SIR team."))
    Sys.setenv(SIA_CLUSTERING_FLAG = FALSE)
  } else {
    Sys.setenv(SIA_CLUSTERING_FLAG = TRUE)
  }

  #check if cache exists, if not, create it

  cache_file <- file.path(polis_folder, "cache.rds")

  if(tidypolis_io(io = "exists.file", file_path = cache_file)){
    cli::cli_alert_success("Previous cache located!")
    invisible(capture.output(
      cache <- tidypolis_io(io = "read", file_path = cache_file)
    ))
    if("pop" %in% dplyr::pull(cache, table)){
      cli::cli_alert_success("Cache version is up to date!")
    }else{
      cli::cli_alert_info("Updating cache version")

      invisible(capture.output(
        cache |>
          dplyr::bind_rows(
            dplyr::tibble(
              "table" = "pop",
              "endpoint" = "Population",
              "polis_id" = "Id"
            )
          )|>
          tidypolis_io(io = "write", file_path = cache_file)
      ))
    }
  }else{

    cache_tibble <- tibble::tibble(
      "table" = c("cache", "virus", "case", "human_specimen", "environmental_sample",
                  "activity", "sub_activity", "lqas", "im", "population", "geography",
                  "synonym", "indicator", "reference_data", "pop"),
      "endpoint" = c("cache", "Virus", "Case", "LabSpecimen", "EnvSample", "Activity",
                     "SubActivity", "Lqas", "Im", "Population", "Geography", "Synonym", "IndicatorValue",
                     "RefData", "Population"),
      "polis_id" = c(NA, "VirusId", "EPID", "SpecimenId", "EnviroSampleManualEditId", "SubActivityId", "SubActivityByAdmin2Id",
                     "LqasId", "ImId", "FK_GeoplaceId", "PlaceId", NA, NA, NA, "Id"),
      "polis_update_id" = c(NA, "UpdatedDate", "LastUpdateDate", "LastUpdateDate", "LastUpdateDate", "LastUpdateDate", "UpdatedDate",
                            NA, NA, "UpdatedDate", "UpdatedDate", NA, NA, NA, NA),
      "nrow" = NA
    ) |>
      dplyr::mutate(last_sync = ifelse(table == "cache", Sys.time(), NA),
                    last_sync = lubridate::as_datetime(last_sync),
                    polis_update_value = ifelse(table == "cache", Sys.time(), NA),
                    polis_update_value = lubridate::as_datetime(polis_update_value),
                    nrow = ifelse(table == "cache", 14, NA),
                    last_user = Sys.getenv("USERNAME"))

    invisible(capture.output(
      tidypolis_io(obj = cache_tibble, io = "write", file_path = cache_file)
    ))
    cli::cli_alert_success("No cache located, creating cache file.")
  }
  Sys.setenv(POLIS_CACHE_FILE = cache_file)

  #check to see if log exists, if not use it

  log_file <- file.path(polis_folder, "log.rds")

  if(tidypolis_io(io = "exists.file", file_path = log_file)){
    cli::cli_alert_success("Previous log located!")
  }else{
    invisible(capture.output(
      tibble::tibble(
        "time" = Sys.time(),
        "user" = Sys.getenv("USERNAME"),
        "event" = "Log created"
      ) |>
        tidypolis_io(io = "write", file_path = log_file)
    ))
    cli::cli_alert_success("No log located, creating log file.")
  }

  api_log_file <- log_file |>
    stringr::str_replace("/log.rds", "/api_call_log.rds")

  log_exists <- tidypolis_io(io = "exists.file", file_path = api_log_file)

  if(!log_exists){
    cli::cli_alert_success("No API call log located, creating log file.")
    tibble::tibble(
      time = Sys.time(),
      call = "INIT",
      event = "INIT"
    ) |>
      tidypolis_io(io = "write", file_path = api_log_file)
  } else {
    cli::cli_alert_success("Previous API call log located!")
  }

  Sys.setenv(POLIS_LOG_FILE = log_file)
  Sys.setenv(POLIS_API_LOG_FILE = api_log_file)

  #check if key details exist, if not ask for them, test them and store them
  #if they exist, check the key
  #if key doesn't work ask for another one
  #check to see if a previous yaml version exists that needs to
  #be converted into an rds
  yaml_cred_file <- file.path(polis_folder, "creds.yaml")
  yaml_cred_file_exists <- tidypolis_io(io = "exists.file", file_path = yaml_cred_file)
  cred_file <- file.path(polis_folder, "creds.rds")
  cred_file_exists <- tidypolis_io(io = "exists.file", file_path = cred_file)

  if(yaml_cred_file_exists & !cred_file_exists){
    yaml::read_yaml(yaml_cred_file) |>
      readr::write_rds(cred_file)
    cli::cli_alert_info("Cred file updated to RDS from YAML")
  }

  if(cred_file_exists){
    cli::cli_alert_success("POLIS API Key found - validating key")

    invisible(capture.output(
      api_key <- tidypolis_io(io = "read", file_path = cred_file)$polis_api_key
    ))


    if(test_polis_key(api_key)){
      Sys.setenv(POLIS_API_KEY = api_key)
      cli::cli_alert_success("POLIS API Key validated!")
      invalid_key <- F
    }else{
      invalid_key <- T
    }
  }else{
    invalid_key <- T
  }

  if(invalid_key){

    cli::cli_alert_warning("POLIS API Key is not valid or not found - enter a new key (recommended) or skip validation? (yes/skip)")
    while (TRUE) {
      response <- stringr::str_to_lower(stringr::str_trim(readline("Response: ")))
      if (!response %in% c("yes", "skip")) {
        cli::cli_alert_warning("Invalid response. Please try again.")
      } else if (response == "skip") {
        break
      } else if (response == "yes") {
        cli::cli_alert_warning("Please enter new key")

        i <- 1
        fail <- T
        while(i < 3 & fail){

          cli::cli_alert_info(paste0("Attempt - ", i, "/3"))


          key <- readline(prompt = "Please enter API Key (without any quotation marks or wrappers): ")

          if(test_polis_key(key)){

            fail <- F

            list(
              "polis_api_key" = key,
              "polis_data_folder" = polis_folder
            ) |>
              tidypolis_io(io = "write", file_path = cred_file)

            Sys.setenv(POLIS_API_KEY = key)

            cli::cli_alert_success("POLIS API Key validated!")
            break
          }

          i <- i + 1

        }

        if(i == 3){
          stop("Please verify your POLIS API key and try again")
        }
      }
    }
  }

  flag <- c("POLIS_DATA_FOLDER", "POLIS_API_KEY", "POLIS_DATA_CACHE",
    "POLIS_CACHE_FILE", "POLIS_LOG_FILE") |>
    sapply(Sys.getenv) |>
    sapply(function(x) nchar(x) == 0) |>
    sum()

  if(flag > 0){
    update_polis_log(.event = "INIT error - flag > 0", .event_type = "ERROR")
    stop("Cache and environment could not be reconciled, please delete and reinitate folder")
  }else{
    update_polis_log(.event = "INIT completed successfully",
                     .event_type = "INIT")
    cli::cli_alert_success("POLIS data folder initiated, key validated and environment setup completed!")
  }

}



#' Manager function to get and update POLIS data
#'
#' @description This function iterates through all tables and loads POLIS data. It
#' checks to ensure that new rows are created, data are updated accordingly and
#' deleted rows are reflected in the local system.
#' @param type choose to download population data ("pop") or all other data. Default's to "all"
#' @param output_format str: output_format to save files as.
#'    Available formats include 'rds' 'rda' 'csv' 'qs' and 'parquet', Defaults is
#'    'rds'.
#' @import dplyr
#' @examples
#' \dontrun{
#' get_polis_data() #must be run after using init_tidypolis and providing a valid API key
#' }
#' @export
get_polis_data <- function(type = "all", output_format = "rds"){

  if(type == "all"){

    tables <- c("virus", "case", "human_specimen", "environmental_sample",
                "activity", "sub_activity", "lqas", "im")

    update_polis_log(.event = paste0("Start POLIS download of: ", paste(tables, collapse = ", ")),
                     .event_type = "START")

    sapply(tables, function(x) get_table_data(.table = x, output_format))

  }

  if(type == "pop"){

    update_polis_log(.event = "Start POLIS pop download",
                     .event_type = "START")

    get_table_data(.table = "pop", output_format)

    update_polis_log(.event = "POLIS Pop file donwloaded",
                     .event_type = "END")
  }


}


#' Run diagnostic test on polis connections
#'
#' @description Run diagnostics of API connection with POLIS
#' @import dplyr
#' @returns tibble with diagnostic results
#' @export
run_diagnostics <- function(){

  tables <- c("virus", "case", "human_specimen", "environmental_sample",
              "activity", "sub_activity", "lqas", "im")

  lapply(tables, function(x) run_single_table_diagnostic(.table = x)) |>
    dplyr::bind_rows()

}

#' Build a freeze of POLIS data on a specific date
#'
#' @description Zip all POLIS data that is used to work on a specific date
#' @import cli
#' @returns Location of zipped and frozen POLIS data
#' @export
freeze_polis_data <- function(){
  time <- Sys.Date()
  cli::cli_process_start(paste0("Creating new freeze file for ", time))
  files <- list.files(Sys.getenv("POLIS_DATA_CACHE"))
  freeze_dir <- paste0(Sys.getenv("POLIS_DATA_FOLDER"),"/freeze")
  if(!dir.exists(freeze_dir)){
    dir.create(freeze_dir)
  }
  freeze_file <- paste0(freeze_dir, "/", time)
  if(!dir.exists(freeze_file)){
    dir.create(freeze_file)
  }
  for(file in files){
    file.copy(
      from = paste0(Sys.getenv("POLIS_DATA_CACHE"),"/",file),
      to = paste0(freeze_file,"/",file),
      overwrite = T
      )
  }
  cli::cli_process_done(msg_done = paste0("Freeze file created in: ", freeze_file))
}


#' Preprocess data retrieved from POLIS
#'
#' @description
#' Create standard analytic datasets from raw POLIS data
#'
#' @param type str: specify the type of preprocessing to complete
#' @param who_region str: optional WHO region to filter data
#'      Available inputs include AFRO, AMRO, EMRO, EURO, SEARO and  WPRO.
#'
#' @param output_format str: output_format to save files as.
#'    Available formats include 'rds' 'rda' 'csv' 'qs' and 'parquet', Defaults is
#'    'rds'.
#'
#' @import cli
#' @returns Analytic rds files
#' @examples
#' \dontrun{
#' preprocess_data(type = "cdc") #must run init_tidypolis to specify POLIS data location first
#' }
#' @export
preprocess_data <- function(type = "cdc", who_region = NULL, output_format = "rds"){

  types <- c("cdc")
  outputs <- c("rds", "rda", "csv", "qs",  "parquet")

  if (!(type %in% types)) {
    cli::cli_abort(message = paste0("'", type, "'", " is not one of the accepted values for 'type'"))
  }

  if (!(output_format %in% outputs)) {
    cli::cli_abort(paste0("'", output_format, "'", " is not one of the accepted values for 'output_format'"))
  }

  #CDC pre-processing steps
  if (type == "cdc") {
    preprocess_cdc(who_region = who_region, output_format = output_format)
  }



}
