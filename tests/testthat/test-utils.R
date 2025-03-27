test_that("Testing f.pre.st.sample()", {

  # Load example dataset, which is afp.linelist.fixed.04 in preprocessing
  file_path <- file.path("Software_Development/Test_Data",
                         "afp.linelist.fixed.04.simplified.sample.xlsx")
  afp.linelist.fixes.04 <- sirfunctions::edav_io("read",
                                                 file_loc = file_path)
  # Load district shape file
  global.dist.01 <- sirfunctions::load_clean_dist_sp()

  # Data where all records have lat/lon and Admin2GUIDs
  case_1 <- f.pre.stsample.01(afp.linelist.fixes.04$all_with_pts,
                              global.dist.01)
  geo_corrected <- case_1 |>
    dplyr::filter(geo.corrected == 1) |>
    nrow()

  expect_equal(geo_corrected, 10)

  # Data where some records are missing lat/lon and Admin2GUIDs
  case_2 <- f.pre.stsample.01(afp.linelist.fixes.04$some_empty_with_adm2guid,
                              global.dist.01)
  geo_corrected <- case_2 |>
    dplyr::filter(geo.corrected == 1) |>
    nrow()
  with_coords <- case_2 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(geo_corrected, 5)
  expect_equal(with_coords, 10)

  # Data where all records are missing lat/lon and have Admin2GUIDs
  case_3 <- f.pre.stsample.01(afp.linelist.fixes.04$empty_with_adm2guid,
                              global.dist.01)

  geo_corrected <- case_3 |>
    dplyr::filter(geo.corrected == 1) |>
    nrow()
  with_coords <- case_2 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(geo_corrected, 10)
  expect_equal(with_coords, 10)

  # Data where all records are missing both lat/lon and Admin2GUIDs
  case_4 <- f.pre.stsample.01(afp.linelist.fixes.04$empty_no_adm2guid,
                              global.dist.01)

  geo_corrected <- case_4 |>
    dplyr::filter(geo.corrected == 1) |>
    nrow()
  with_coords <- case_2 |>
    dplyr::filter(!is.na(lat), !is.na(lon)) |>
    nrow()

  expect_equal(geo_corrected, 0)
  expect_equal(with_coords, 0)

})
