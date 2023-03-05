# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'


#' @description Initialize API Key and local data cache for tidypolis. inspiration from
#' tidycensus process for managing their API
#' @param polid_data_folder str: location of folder where to store all information
#' @returns Messages on process
init_tidypolis <- function(
    polis_data_folder
    ){

  #check if folder exists, if not create it after asking for validation
  if(dir.exists(polis_data_folder)){
    message("POLIS data folder found!")
  }else{

    val <- request_input(
      request = paste0("Creating POLIS data folder at '",polis_data_folder),
      vals = c("Y", "N")
    )


    if(val == "Y"){
      dir.create(polis_data_folder)
      message(paste0("- POLIS data folder created at'", polis_data_folder, "'"))
    }else{
      stop(paste0("Please run function again with updated folder location"))
    }
  }
  #check if sub-folders exist, if not create them

  message("Checking POLIS data folder structure")
  if("data" %in% list.files(polis_data_folder)){
    message("- Data folder identified")
  }else{
    message("- Creating 'data' folder")
    dir.create(paste0(polis_data_folder,"/data"))
  }

  #check if key details exist, if not ask for them, test them and store them
  #if they exist, check the key
  #if key doesn't work ask for another one
  cred_file <- file.path(polis_data_folder, "creds.yaml")
  if(file.exists(cred_file)){
    message("POLIS API Key found - validating key")

    if(test_polis_key(yaml::read_yaml(cred_file)$polis_api_key)){
      message("POLIS API Key validated!")
    }else{
      message("POLIS API Key is not valid - please enter new key")

      i <- 1
      fail <- T
      while(i < 3 | fail){

        message(paste0("Attempt - ", i, "/3"))


        key <- readline(prompt = "Please enter API Key: ")

        if(test_polis_key(key)){

          fail <- F

          list(
            "polis_api_key" = key,
            "polis_data_folder" = polis_data_folder
          )

          message("POLIS API Key validated!")

        }

      }

      if(i == 3){
        stop("Please verify your POLIS API key and try again")
      }

    }
  }

  #check if cache exists, if not, create it

  cache_file <- file.path(polis_data_folder, "cache.rds")

  if(file.exists(cache_file)){
    message("Previous cache located!")
  }else{
    tibble::tibble(
      "table" = c("cache", "virus", "case", "human_specimen", "environmental_sample",
                  "activity", "sub_activity", "lqas", "im", "population", "geography",
                  "synonym", "indicator", "reference_data"),
      "nrow" = NA
    ) |>
      dplyr::mutate(last_updated = Sys.time(),
                    nrow = ifelse(table == "cache", 14, NA)) |>
      readr::write_rds(cache.file)
  }

  if(test_polis_key(key)){


    home <- Sys.getenv("HOME")
    renv <- file.path(home, ".Renviron")

    if(!file.exists(renv)){
      file.create(renv)
    }

    key_exists <- any(grepl("POLIS_API_KEY", readLines(renv)))

    if(key_exists){
      message("An API key alread exists for POLIS")
    }

    else{
      if(isTRUE(overwrite)){
        message("Your original .Renviron will be backed up and stored in your R HOME directory if needed.")
        oldenv = read.table(renv, stringsAsFactors = FALSE)
        newenv <- oldenv[-grep("CENSUS_API_KEY", oldenv),
        ]
        write.table(newenv, renv, quote = FALSE, sep = "\n",
                    col.names = FALSE, row.names = FALSE)
      }
      else{
        tv <- readLines(renv)
        if(any(grepl("CENSUS_API_KEY", tv))) {
          stop("A CENSUS_API_KEY already exists. You can overwrite it with the argument overwrite=TRUE",
               call. = FALSE)
        }
      }
    }
    keyconcat <- paste0("CENSUS_API_KEY='", key, "'")
    write(keyconcat, renv, sep = "\n", append = TRUE)
    message("Your API key has been stored in your .Renviron and can be accessed by Sys.getenv(\"CENSUS_API_KEY\"). \nTo use now, restart R or run `readRenviron(\"~/.Renviron\")`")
    return(key)

  }else{

    stop(paste0("Please verify POLIS API Key, key provided returned an error.\nKey: ", key))

  }

}



#' @description Test POLIS API Key
#' @param key str: POLIS API Key
#' @returns boolean
test_polis_key <- function(key){

  # Variables: URL, Token, Filters, ...
  polis_api_root_url <- "https://extranet.who.int/polis/api/v2/"

  api_url <- paste0(polis_api_root_url, "Virus?$top=1")

  # connect to the API and Get data
  get_result <- httr::GET(api_url, httr::add_headers("authorization-token" = key))
  httr::content(get_result, type="application/json", encoding = "UTF-8")

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

