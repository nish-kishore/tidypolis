test_that("Testing f.pre.st.sample()", {
  skip()
  cli::cli_process_start("Loading required data for testing")

  # Load example dataset, which is afp.linelist.fixed.04 in preprocessing
  file_path <- file.path(
    "Software_Development/Test_Data",
    "afp.linelist.fixed.04.simplified.sample.xlsx"
  )
  afp.linelist.fixes.04 <- sirfunctions::edav_io("read",
    file_loc = file_path
  )
  # Load district shape file
  global.dist.01 <- sirfunctions::load_clean_dist_sp()
  global.dist.01 <- global.dist.01 |>
    dplyr::filter(ADM0_GUID %in% unique(afp.linelist.fixes.04$orig$Admin0GUID))

  cli::cli_process_done()

  cli::cli_process_start("Testing missing coordinate imputation")
  # Data where all records have lat/lon and Admin2GUIDs
  case_1 <- f.pre.stsample.01(
    afp.linelist.fixes.04$all_with_pts,
    global.dist.01
  )
  with_coords <- case_1 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(with_coords, 10)

  # Data where some records are missing lat/lon but have Admin2GUIDs
  case_2 <- f.pre.stsample.01(
    afp.linelist.fixes.04$some_empty_with_adm2guid,
    global.dist.01
  )

  with_coords <- case_2 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(with_coords, 10)

  # Data where all records are missing lat/lon and have Admin2GUIDs
  case_3 <- f.pre.stsample.01(
    afp.linelist.fixes.04$empty_with_adm2guid,
    global.dist.01
  )

  with_coords <- case_3 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(with_coords, 10)

  # Data where all records are missing both lat/lon and Admin2GUIDs
  case_4 <- f.pre.stsample.01(
    afp.linelist.fixes.04$empty_no_adm2guid,
    global.dist.01
  )

  with_coords <- case_4 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(with_coords, 0)

  rm(case_1, case_2, case_3, case_4)

  cli::cli_process_done()

  cli::cli_process_start("Testing the geocorrection function")

  # All records are correct
  no_correction <- afp.linelist.fixes.04$orig |>
    dplyr::filter(epid != "INDMPRWA20034")
  case_1 <- f.pre.stsample.01(no_correction, global.dist.01)
  geo_corrected <- case_1 |>
    dplyr::filter(geo.corrected == 1) |>
    nrow()

  expect_equal(geo_corrected, 0)

  # One record where the ADM2GUID is not correct
  needs_correction <- afp.linelist.fixes.04$orig |>
    dplyr::filter(epid == "INDMPRWA20034")
  case_2 <- f.pre.stsample.01(needs_correction, global.dist.01)

  geo_corrected <- case_2 |>
    dplyr::filter(geo.corrected == 1) |>
    nrow()

  expect_equal(geo_corrected, 1)
  cli::cli_process_done()
})

test_that("Testing check_missing_static_files()", {
  test_folder_path <-
    "Software_Development/Test_Data/check_static_file_test_folder"

  # All static files are present
  expect_length(check_missing_static_files(
    file.path(
      test_folder_path,
      "core_files_present"
    ),
    TRUE
  ), 0)
  # All static files are absent
  expect_length(check_missing_static_files(
    file.path(
      test_folder_path,
      "core_files_absent"
    ),
    TRUE
  ), 6)

  # One file is missing
  expect_length(check_missing_static_files(
    file.path(
      test_folder_path,
      "one_file_absent"
    ),
    TRUE
  ), 1)
})
