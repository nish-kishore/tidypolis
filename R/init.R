#' A function to intitialize polis
#'
#' @description Initialize API Key and local data cache for tidypolis. inspiration from
#' tidycensus process for managing their API
#' @param polid_data_folder str: location of folder where to store all information
#' @returns Messages on process
init_tidypolis <- function(
    polis_data_folder
    ){

  #check if folder exists, if not create it after asking for validation
  if(dir.exists(polis_data_folder)){
    cli::cli_alert_success("POLIS data folder found!")
  }else{

    val <- request_input(
      request = paste0("Confirm creation of POLIS data folder at '",polis_data_folder),
      vals = c("Y", "N")
    )


    if(val == "Y"){
      dir.create(polis_data_folder)
      cli::cli_alert_success(paste0("- POLIS data folder created at'", polis_data_folder, "'"))
    }else{
      stop(paste0("Please run function again with updated folder location"))
    }
  }
  Sys.setenv(POLIS_DATA_FOLDER = polis_data_folder)
  #check if sub-folders exist, if not create them

  cli::cli_alert_info("Checking POLIS data folder structure")
  if("data" %in% list.files(polis_data_folder)){
    cli::cli_alert_success("- Data folder identified")
  }else{
    cli::cli_alert_success("- Creating 'data' folder")
    dir.create(paste0(polis_data_folder,"/data"))
  }
  Sys.setenv(POLIS_DATA_CACHE = paste0(polis_data_folder,"/data"))

  #check if key details exist, if not ask for them, test them and store them
  #if they exist, check the key
  #if key doesn't work ask for another one
  cred_file <- file.path(polis_data_folder, "creds.yaml")
  cred_file_exists <- file.exists(cred_file)
  if(cred_file_exists){
    cli::cli_alert_success("POLIS API Key found - validating key")

    if(test_polis_key(yaml::read_yaml(cred_file)$polis_api_key)){
      Sys.setenv(POLIS_API_KEY = yaml::read_yaml(cred_file)$polis_api_key)
      cli::cli_alert_success("POLIS API Key validated!")
      invalid_key <- F
    }else{
      invalid_key <- T
    }
  }else{
    invalid_key <- T
  }

  if(invalid_key){

    cli::cli_alert_warning("POLIS API Key is not valid or not found - please enter new key")

    i <- 1
    fail <- T
    while(i < 3 & fail){

      cli::cli_alert_info(paste0("Attempt - ", i, "/3"))


      key <- readline(prompt = "Please enter API Key (without any quotation marks or wrappers): ")

      if(test_polis_key(key)){

        fail <- F

        list(
          "polis_api_key" = key,
          "polis_data_folder" = polis_data_folder
        ) |>
          yaml::write_yaml(cred_file)

        Sys.setenv(POLIS_API_KEY = key)

        cli::cli_alert_success("POLIS API Key validated!")

      }

      i <- i + 1

    }

    if(i == 3){
      stop("Please verify your POLIS API key and try again")
    }

  }

  #check if cache exists, if not, create it

  cache_file <- file.path(polis_data_folder, "cache.rds")

  if(file.exists(cache_file)){
    cli::cli_alert_success("Previous cache located!")
  }else{
    tibble::tibble(
      "table" = c("cache", "virus", "case", "human_specimen", "environmental_sample",
                  "activity", "sub_activity", "lqas", "im", "population", "geography",
                  "synonym", "indicator", "reference_data"),
      "endpoint" = c("cache", "Virus", "Case", "LabSpecimen", "EnvSample", "Activity",
                     "SubActivity", "Lqas", "Im", "Population", "Geography", "Synonym", "IndicatorValue",
                     "RefData"),
      "polis_id" = c(NA, "VirusId", "EPID", "SpecimenId", "EnviroSampleId", "SubActivityId", "SubActivityByAdmin2Id",
                     "LqasId", "ImId", "FK_GeoplaceId", "PlaceId", NA, NA, NA),
      "polis_update_id" = c(NA, "UpdatedDate", "LastUpdateDate", "LastUpdateDate", "LastUpdateDate", "LastUpdateDate", "UpdatedDate",
                            NA, NA, "UpdatedDate", "UpdatedDate", NA, NA, NA),
      "nrow" = NA
    ) |>
      dplyr::mutate(last_sync = ifelse(table == "cache", Sys.time(), NA),
                    last_sync = lubridate::as_datetime(last_sync),
                    polis_update_value = ifelse(table == "cache", Sys.time(), NA),
                    polis_update_value = lubridate::as_datetime(polis_update_value),
                    nrow = ifelse(table == "cache", 14, NA),
                    last_user = Sys.getenv("USERNAME")) |>
      readr::write_rds(cache_file)
    cli::cli_alert_success("No cache located, creating cache file.")
  }
  Sys.setenv(POLIS_CACHE_FILE = cache_file)

  #check to see if log exists, if not use it

  log_file <- file.path(polis_data_folder, "log.rds")

  if(file.exists(log_file)){
    cli::cli_alert_success("Previous log located!")
  }else{
    tibble::tibble(
      "time" = Sys.time(),
      "user" = Sys.getenv("USERNAME"),
      "event" = "Log created"
    ) |>
      readr::write_rds(log_file)
    cli::cli_alert_success("No log located, creating log file.")
  }
  Sys.setenv(POLIS_LOG_FILE = log_file)

  flag <- c("POLIS_DATA_FOLDER", "POLIS_API_KEY", "POLIS_DATA_CACHE",
    "POLIS_CACHE_FILE", "POLIS_LOG_FILE") |>
    sapply(Sys.getenv) |>
    sapply(function(x) nchar(x) == 0) |>
    sum()

  if(flag > 0){
    update_polis_log(.event = "INIT error - flag > 0")
    stop("Cache and environment could not be reconciled, please delete and reinitate folder")
  }else{
    update_polis_log(.event = "INIT completed successfully")
    cli::cli_alert_success("POLIS data folder initiated, key validated and environment setup completed!")
  }

}



#' @description Test POLIS API Key
#' @param key str: POLIS API Key
#' @returns boolean
test_polis_key <- function(key){

  # Variables: URL, Token, Filters, ...
  polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

  api_url <- paste0(polis_api_root_url, "$metadata")

  # connect to the API and Get data
  get_result <- httr::GET(api_url, httr::add_headers("authorization-token" = key))

  # Display the status which should be 200 (OK)
  return(httr::status_code(get_result) == 200)

}

#' @description Create creds file
#' @param polis_data_folder str: location of POLIS data folder
#' @returns boolean for folder creation
create_cred_file <- function(
    polis_data_folder
){
  list(
    "polis_api_key" = "",
    "polis_data_folder" = ""
  ) |>
    yaml::write_yaml(file = file.path(polis_data_folder,"creds.yaml"))
}

