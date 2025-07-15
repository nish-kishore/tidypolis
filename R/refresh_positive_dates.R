refresh_positive_dates <- function(polis_folder = Sys.getenv("POLIS_DATA_FOLDER"),
                                   edav = Sys.getenv("POLIS_EDAV_FLAG"),
                                   who_region = NULL,
                                   output_format = "rds",
                                   lookback_months = 3) {

  # Only pull data from the last 6 months
  date_filter <- lubridate::floor_date(Sys.Date() %m-% months(lookback_months), unit = "months")

  # Point to the correct core ready folder
  CORE_READY_FOLDER <- "Core_Ready_Files"
  if (!is.null(who_region)) {
    # 2a. Validate region code
    valid <- c("AFRO", "AMRO", "EMRO", "EURO", "SEARO", "WPRO")
    if (!who_region %in% valid) {
      cli::cli_abort("\u2018{who_region}\u2019 is not a valid WHO region.")
    }

    # Set region-specific folder name
    CORE_READY_FOLDER <- paste0("Core_Ready_Files_", who_region)
  }

  # Get list of outputs in polis folder
  dl_list <- tidypolis_io(io = "list",
                          file_path = file.path(polis_folder, "data", CORE_READY_FOLDER),
                          edav = edav,
                          full_names = TRUE)
  AFP_PATH <- dl_list[grepl(paste0("*afp_linelist_2019.*", output_format, "$"), dl_list)]
  POS_PATH <- dl_list[grepl(paste0("*positives_.*", output_format, "$"), dl_list)]

  # Read truncated AFP linelist
  afp <- tidypolis_io(io="read", file_path = AFP_PATH, edav = edav)

  # Read positives linelist
  pos <- tidypolis_io(io="read", file_path = AFP_PATH, edav = edav)

  # Read ES as well?

  # Gather positives with missing dates
  pos_epids <- pos |>
    dplyr::select(epid, datenotificationtohq, created.date) |>
    dplyr::mutate(datecreated = as.Date(created.date)) |>
    dplyr::filter(is.na(datenotificationtohq) &
                    datecreated >= date_filter)

  not_in_afp <- setdiff(pos_epids$epid, afp$epid)
  if (length(not_in_afp) > 0) {
    cli::cli_alert_info(paste0("Positives that are not AFP: ", length(not_in_afp)))
  }

  # Set up URLs
  base_url <- "https://extranet.who.int/polis/api/v2"
  afp_url_filter <- "&$select=EPID,CaseManualEditId,DateNotificationtoHQ"
  pos_url_filter <- "&$select=EPID,VirusId,VdpvClassificationChangeDate"

  # Re-pull from POLIS case
  afp_table_urls <- create_table_urls(file.path(base_url, "Case"),
                                      nrow(pos_epids), type = "full")
  afp_table_urls <- paste0(afp_table_urls,
                           afp_url_filter)

  afp_resp <- purrr::map(afp_table_urls, \(x) call_single_url(x), .progress = TRUE) |>
    dplyr::bind_rows()


  # Re-pull from POLIS virus
  pos_table_urls <- create_table_urls(file.path(base_url, "Virus"),
                                      nrow(pos_epids), type = "full")
  pos_table_urls <- paste0(pos_table_urls,
                           pos_url_filter)
  pos_resp <- purrr::map(pos_table_urls, \(x) call_single_url(x), .progress = TRUE) |>
    dplyr::bind_rows()
  pos_resp <- pos_resp |>
    dplyr::mutate(VdpvClassificationChangeDate = as.Date(VdpvClassificationChangeDate))

  # Coalesce to replace missing notification dates in AFP
  pos_corrected <- pos |>
    dplyr::left_join(pos_resp) |>
    dplyr::mutate(date)

  # Coalesce to replace missing notification and report dates in Positives

  # Reupload new AFP linelist

  # Reupload new Positives file

}
