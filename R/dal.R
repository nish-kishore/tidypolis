#DATA ACCESS LAYER

#' Tiypolis input and output
#'
#' @description
#' Manages read/write/list/create/delete functions for tidypolis
#'
#' @import sirfunctions dplyr AzureStor readr stringr cli
#' @param obj `str` Object to be loaded into EDAV.
#' @param io `str` read/write/list/exists/create/delete
#' @param file_path `str` Absolute path of file.
#' @param edav `bool` Whether to interact with EDAV files. Defaults to `FALSE`.
#' @param azcontainer `Azure container` An Azure container object.
#' @param full_names `bool` If you want to include the full reference path in the response, default `FALSE`.
#' @param edav_default_dir `str` If `edav = TRUE`, specify the default directory used for `sirfunctions::edav_io()`.
#' By default, this is `"GID/PEB/SIR"`.
#'
#' @returns Conditional on `io`. If `io` is "read", then it will return a tibble. If `io` is `"list"`, it will return a
#' list of file names. Otherwise, the function will return `NULL`. `exists.dir` and `exists.file` will return a `bool`.
#' @keywords internal
tidypolis_io <- function(
    obj = NULL,
    io,
    file_path,
    edav = as.logical(Sys.getenv("POLIS_EDAV_FLAG")),
    azcontainer = suppressMessages(sirfunctions::get_azure_storage_connection()),
    full_names = F,
    edav_default_dir = "GID/PEB/SIR") {
  opts <- c("read", "write", "delete", "list", "exists.dir", "exists.file", "create")

  if (!io %in% opts) {
    stop("io: must be 'read', 'write', 'delete', 'create', 'exists.dir', 'exists.file' or 'list'")
  }

  if (io == "write" & is.null(obj)) {
    stop("Need to supply an object to be written")
  }

  if (io == "list") {
    if (edav) {
      out <- sirfunctions::edav_io(
        io = "list",
        default_dir = edav_default_dir,
        file_loc = file_path, azcontainer = azcontainer
      )

      if (full_names) {
        return(
          out |>
            dplyr::mutate(name = stringr::str_replace(name, paste0("GID/PEB/SIR/"), "")) |>
            dplyr::pull(name)
        )
      } else {
        return(
          out |>
            dplyr::mutate(name = stringr::str_replace(name, paste0("GID/PEB/SIR/", file_path, "/"), "")) |>
            dplyr::pull(name)
        )
      }
    } else {
      return(list.files(file_path, full.names = full_names))
    }
  }

  if (io == "exists.dir") {
    if (edav) {
      return(sirfunctions::edav_io(
        io = "exists.dir",
        default_dir = edav_default_dir,
        file_loc = file_path, azcontainer = azcontainer
      ))
    } else {
      return(dir.exists(file_path))
    }
  }

  if (io == "exists.file") {
    if (edav) {
      return(sirfunctions::edav_io(
        io = "exists.file",
        default_dir = edav_default_dir,
        file_loc = file_path, azcontainer = azcontainer
      ))
    } else {
      return(file.exists(file_path))
    }
  }

  if (io == "read") {
    if (edav) {
      if (!requireNamespace("AzureStor", quietly = TRUE)) {
            stop('Package "AzureStor" must be installed to read from EDAV.',
            .call = FALSE)
            }

      corrupted.rds <- NULL
      tryCatch(
        {
          return(sirfunctions::edav_io(
            io = "read",
            default_dir = edav_default_dir,
            file_loc = file_path, azcontainer = azcontainer
          ))
          corrupted.rds <<- FALSE
        },
        error = function(e) {
          cli::cli_alert_warning("RDS download from EDAV was corrupted, downloading directly...")
          corrupted.rds <<- TRUE
        }
      )

      if (corrupted.rds) {
        dest <- tempfile()
        AzureStor::storage_download(
          container = azcontainer,
          paste0(edav_default_dir, file_path),
          dest
        )
        x <- readRDS(dest)
        unlink(dest)
        return(x)
      }
    } else {
      if (!grepl("\\.rds$|\\.rda$|\\.csv$|\\.parquet$", file_path)) {
        stop("At the moment only 'rds' 'rda' 'csv' and 'parquet' are supported for reading.")
      }

      if (grepl("\\.rds$", file_path)) {
        return(readr::read_rds(file_path))
      }

      if (grepl("\\.rda$", file_path)) {
        return(load(file_path))
      }

      if (grepl("\\.csv$", file_path)) {
        return(readr::read_csv(file_path, show_col_types = FALSE))
      }

      if (grepl("\\.parquet$", file_path)) {
          if (!requireNamespace("arrow", quietly = TRUE)) {
            stop('Package "arrow" must be installed to read parquet files.',
            .call = FALSE)
            }
        return(arrow::read_parquet(file_path))
      }

    }
  }

  if (io == "write") {
    if (is.null(obj)) {
      stop("You need to include an object to be written.")
    }

    if (edav) {
      sirfunctions::edav_io(
        io = "write",
        default_dir = edav_default_dir,
        file_loc = file_path, obj = obj,
        azcontainer = azcontainer
      )
    } else {
      if (!grepl("\\.rds$|\\.rda$|\\.csv$|\\.parquet$", file_path)) {
        stop("At the moment only 'rds' 'rda' 'csv' and 'parquet' are supported for writing")
      }

      if (grepl("\\.rds$", file_path)) {
        readr::write_rds(x = obj, file = file_path)
      }

      if (grepl("\\.rda$", file_path)) {
        save(list = obj, file = file_path)
      }

      if (grepl("\\.csv$", file_path)) {
        readr::write_csv(x = obj, file = file_path)
      }

      if (grepl("\\.parquet$", file_path)) {
        if (!requireNamespace("arrow", quietly = TRUE)) {
          stop('Package "arrow" must be installed to write parquet files.',
          .call = FALSE)
          }

        arrow::write_parquet(obj, file_path)
      }
    }
  }

  if (io == "delete") {
    if (edav) {
      sirfunctions::edav_io(
        io = "delete",
        default_dir = edav_default_dir,
        file_loc = file_path,
        force_delete = T,
        azcontainer = azcontainer
      )
    } else {
      file.remove(file_path)
    }
  }

  if (io == "create") {
    if (edav) {
      sirfunctions::edav_io(
        io = "create",
        default_dir = edav_default_dir,
        file_loc = file_path
      )
    } else {
      dir.create(file_path)
    }
  }

  return(NULL)
}


#' Transfer preprocessed files to active EDAV download
#'
#' @description
#' The function will perform an archiving operation of the previous preprocessed
#' files in the data folder used when recreating `raw.data` with updated dataset.
#' It will also copy the newly preprocessed files to the data folder.
#'
#' @import sirfunctions dplyr AzureStor readr cli stringr
#' @param core_ready_folder `str` Local folder with CDC processed files.
#' @param azcontainer `Azure container` Azure Token Container Object.
#' @param output_folder `str` Location to write out Core Files.
#' @returns `NULL`.
#' @export
upload_cdc_proc_to_edav <- function(
    core_ready_folder = file.path(Sys.getenv("POLIS_DATA_CACHE"), "Core_Ready_Files"),
    azcontainer = sirfunctions::get_azure_storage_connection(),
    output_folder = "Data/polis"){

  #check to see if folders exist
  if (tidypolis_io(io = "exists.dir", file_path = core_ready_folder)) {
    cli::cli_alert_info("Core File Identified")
  } else {
    cli::cli_abort("No core file identified, please make sure to initialize tidypolis and run CDC preprocessing")
  }

  #check to see if all files exist

  x <- dplyr::tibble(
    "full_name" = tidypolis_io(io = "list", file_path = core_ready_folder, full_names = T),
    "name" = tidypolis_io(io = "list", file_path = core_ready_folder)
  ) |>
    dplyr::filter(stringr::str_ends(name, ".rds"))

  files <- c("afp_linelist_2001-01-01_",
             "afp_linelist_2019-01-01_",
             "es_2001-01-08_",
             "other_surveillance_type_linelist_2016_",
             "positives_2001-01-01_",
             "sia_2000")

  out.table <- lapply(files, function(y) x |> dplyr::filter(stringr::str_starts(name, pattern = y))) |>
    dplyr::bind_rows() |>
    dplyr::mutate(source = full_name,
                  dest = paste0(output_folder, "/",name))

  # Archive previous versions in the core polis data folder
  cli::cli_process_start("Archiving previous data in the polis folder")
  if (!tidypolis_io(io = "exists.dir", file_path = file.path(output_folder, "archive"))) {
    tidypolis_io(io = "create", file_path = file.path(output_folder, "archive"))
  }

  # Move previous files to archive in polis data folder
  previous_files <- dplyr::tibble(
    "file_path" = tidypolis_io(io = "list",
                               file_path = file.path(output_folder),
                               full_names = TRUE),
    "file_name" = tidypolis_io(io = "list",
                               file_path = file.path(output_folder))) |>
    dplyr::filter(stringr::str_ends(file_name, ".rds"))

  # Explicitly create since Sys.Date() could potentially vary when ran overnight
  archive_folder_name <- Sys.Date()

  tidypolis_io(io = "create",
               file_path = file.path(output_folder,"archive", archive_folder_name))

  lapply(1:nrow(previous_files), \(i) {
    local <- tidypolis_io(io = "read", file_path = previous_files$file_path[i])
    tidypolis_io(obj = local, io = "write",
                 file_path = file.path(output_folder, "archive", archive_folder_name,
                                       previous_files$file_name[i]))
  })
  cli::cli_process_done()

  cli::cli_process_start("Adding updated data to polis data folder")

  # Delete previous files
  lapply(1:nrow(previous_files), \(i) {
    tidypolis_io(io = "delete", file_path = previous_files$file_path[i])
  })

  # Move all files to core polis data folder
  lapply(1:nrow(out.table), function(i){
    local <- tidypolis_io(io = "read", file_path = dplyr::pull(out.table[i,], source))
    tidypolis_io(obj = local, io = "write", file_path = dplyr::pull(out.table[i,], dest))
  })

  cli::cli_process_done()

  return(NULL)

}
