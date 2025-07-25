% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{s3_sia_create_cdc_vars}
\alias{s3_sia_create_cdc_vars}
\title{Convert variables in POLIS download to CDC specific variables}
\usage{
s3_sia_create_cdc_vars(
  sia.01.new,
  startyr = 2020,
  endyr = lubridate::year(format(Sys.time()))
)
}
\arguments{
\item{sia.01.new}{\code{tibble} The latest SIA download with variables checked
against the last download}

\item{startyr}{\code{int} The subset of years for which to process SIA data}

\item{endyr}{\code{int} The subset of years for which to process SIA data}
}
\value{
\code{tibble} sia.02 SIA data with CDC variables enforced
}
\description{
Convert variables in POLIS download to CDC specific variables
}
\keyword{internal}
