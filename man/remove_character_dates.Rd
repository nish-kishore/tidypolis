% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{remove_character_dates}
\alias{remove_character_dates}
\title{function to remove original character formatted date vars from data tables}
\usage{
remove_character_dates(
  type,
  df,
  polis_data_folder = Sys.getenv("POLIS_DATA_CACHE")
)
}
\arguments{
\item{type}{str: the table on which to remove original date vars, "AFP", "ES", "POS"}

\item{df}{tibble: the dataframe from which to remove character formatted dates}

\item{polis_data_folder}{str:  location of user's polis data folder}
}
\value{
outputs a saved reference table of original date vars and a smaller
core ready file without character dates
}
\description{
remove original date variables from POLIS tables
}
