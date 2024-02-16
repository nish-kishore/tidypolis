#DATA ACCESS LAYER

#' Tiypolis input and output
#'
#' @description
#' Manages read/write/list/create/delete functions for tidypolis
#' @import sirfunctions dplyr AzurStor readr
#' @param obj str: object to be loaded into EDAV
#' @param io str: read/write/list/exists/create/delete
#' @param file_path str: absolute path of file
#' @param edav boolean defaults to FALSE
#' @param azcontainer AZ container object returned by
#' @param full_names boolean: If you want to include the full reference path in the response, dfault FALSE
#' @returns conditional on `io`
tidypolis_io <- function(
    obj = NULL,
    io,
    file_path,
    edav = Sys.getenv("POLIS_EDAV_FLAG"),
    azcontainer = sirfunctions::get_azure_storage_connection(),
    full_names = F
                         ){

  opts <- c("read", "write", "delete", "list", "exists.dir", "exists.file", "create")

  if(!io %in% opts){
    stop("io: must be 'read', 'write', 'delete', 'create', 'exists.dir', 'exists.file' or 'list'")
  }

  if(io == "write" & is.null(obj)){
    stop("Need to supply an object to be written")
  }

  if(io == "list"){

    if(edav){
      out <- sirfunctions::edav_io(io = "list", file_loc = file_path, azcontainer = azcontainer)

      if(full_names){
        return(
          out |>
            dplyr::mutate(name = stringr::str_replace(name, paste0("GID/PEB/SIR/"), "")) |>
            dplyr::pull(name))
      }else{
        return(
          out |>
            dplyr::mutate(name = stringr::str_replace(name, paste0("GID/PEB/SIR/",file_path,"/"), "")) |>
            dplyr::pull(name)
        )
      }

    }else{
      return(list.files(file_path, full.names = full_names))
    }

  }

  if(io == "exists.dir"){

    if(edav){
      return(sirfunctions::edav_io(io = "exists.dir", file_loc = file_path, azcontainer = azcontainer))
    }else{
      return(dir.exists(file_path))
    }


  }

  if(io == "exists.file"){

    if(edav){
      return(sirfunctions::edav_io(io = "exists.file", file_loc = file_path, azcontainer = azcontainer))
    }else{
      return(file.exists(file_path))
    }


  }

  if(io == "read"){
    if(edav){
      tryCatch(
        {
          return(sirfunctions::edav_io(io = "read", file_loc = file_path, azcontainer = azcontainer))
          corrupted.rds <<- FALSE
          },
        error = function(e){
          cli::cli_alert_warning("RDS download from EDAV was corrupted, downloading directly...")
          corrupted.rds <<- TRUE
        }
      )

      if(corrupted.rds){
        dest <- tempfile()
        AzureStor::storage_download(container = azcontainer, paste0("GID/PEB/SIR/",file_path), dest)
        x <- readRDS(dest)
        unlink(dest)
        return(x)
      }

    }else{
      if(!grepl(".rds|.rda|.csv",file_path)){
        stop("At the moment only 'rds' 'rda' and 'csv' are supported for reading.")
      }

      if(grepl(".rds")){
        readr::read_rds(file_path)
      }

      if(grepl(".rda")){
        load(file_path)
      }

      if(grepl(".csv")){
        readr::read_csv(file_path)
      }

    }
  }

  if(io == "write"){

    if(is.null(obj)){
      stop("You need to include an object to be written.")
    }

    if(edav){
      sirfunctions::edav_io(io = "write", file_loc = file_path, obj = obj, azcontainer = azcontainer)
    }else{

      if(!grepl(".rds|.rda|.csv",file_path)){
        stop("At the moment only 'rds' 'rda' and 'csv' are supported for reading.")
      }

      if(grepl(".rds")){
        readr::write_rds(x = obj, file = file_path)
      }

      if(grepl(".rda")){
        save(list = obj, file = file_path)
      }

      if(grepl(".csv")){
        readr::write_csv(x = obj, file = file_path)
      }


    }
  }

  if(io == "delete"){
    if(edav){
      sirfunctions::edav_io(io = "delete", file_loc = file_path, force_delete = T, azcontainer = azcontainer)
    }else{
      file.remove(file_path)
    }
  }

  if(io == "create"){
    if(edav){
      sirfunctions::edav_io(io = "create", file_loc = file_path)
    }else{
      dir.create(file_path)
    }
  }

}
