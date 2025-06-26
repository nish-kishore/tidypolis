test_that("Testing s3_fully_process_sia_data()", {
  skip()
  init_tidypolis("Sandbox/POLIS", edav = TRUE)

  # Load static global variables used in some part of our code
  polis_folder <- "Sandbox/POLIS"
  polis_data_folder <- file.path(polis_folder, "data")
  ts <- Sys.time()
  timestamp <- paste0(
    lubridate::date(ts), "_",
    lubridate::hour(ts), "-",
    lubridate::minute(ts), "-",
    round(lubridate::second(ts), 0)
  )
  long.global.dist.01 <- sirfunctions::load_clean_dist_sp(type = "long")

  latest_folder_in_archive <- s2_find_latest_archive(
    polis_data_folder = polis_data_folder, timestamp = timestamp
  )

  expect_null(s3_fully_process_sia_data(
    long.global.dist.01,
    polis_data_folder,
    latest_folder_in_archive,
    timestamp
  ))
})
