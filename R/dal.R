#DATA ACCESS LAYER

#' Tiypolis input and output
#'
#' @description
#' Manages read/write/list/create/delete functions for tidypolis
#' @import sirfunctions
#' @param obj str: object to be loaded into EDAV
#' @param io str: read/write/list/exists/create/delete
#' @param file_path str: absolute path of file
#' @param edav boolean defaults to FALSE
#' @param azcontainer AZ container object returned by
#' @returns conditional on `io`
tidypolis_io <- function(obj = NULL, io, file_path, edav = Sys.getenv("POLIS_EDAV_FLAG"), azcontainer = sirfunctions::get_azure_storage_connection()){

  opts <- c("read", "write", "delete", "list", "exists.dir", "exists.file", "create")

  if(!io %in% opts){
    stop("io: must be 'read', 'write', 'delete', 'create', 'exists.dir', 'exists.file' or 'list'")
  }

  if(io == "write" & is.null(obj)){
    stop("Need to supply an object to be written")
  }

  if(io == "list"){

    if(edav){
      return(sirfunctions::edav_io(io = "list", file_loc = file_path, azcontainer = azcontainer) |>
               dplyr::mutate(name = str_replace(name, paste0("GID/PEB/SIR/",file_path,"/"), "")) |>
               dplyr::pull(name))
    }else{
      return(list.files(file_path))
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
      return(sirfunctions::edav_io(io = "read", file_loc = file_path, azcontainer = azcontainer))
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