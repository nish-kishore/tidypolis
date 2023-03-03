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
  #check if cache exists, if not, create it

  if(dir.exists(cache_loc))

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
  get_result <- GET(api_url, add_headers("authorization-token" = key))
  content(get_result, type="application/json", encoding = "UTF-8")

  # Display the status which should be 200 (OK)
  return(status_code(get_result) == 200)

}
