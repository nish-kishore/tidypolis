% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{s3_sia_evaluate_unmatched_guids}
\alias{s3_sia_evaluate_unmatched_guids}
\title{Evaluate SIA data with unmatched spatial data}
\usage{
s3_sia_evaluate_unmatched_guids(sia.05, polis_data_folder, output_folder_name)
}
\arguments{
\item{sia.05}{\code{tibble} the output of s3_sia_check_guids()}

\item{polis_data_folder}{The POLIS data folder.}

\item{output_folder_name}{\code{str} Name of the output directory where processed
files will be saved. Defaults to "Core_Ready_Files". For
region-specific processing, this should be set to
"Core_Ready_Files_REGION" (e.g., "Core_Ready_Files_AFRO").}
}
\value{
\code{NULL} silently.
}
\description{
Looks at the output from the GUID matching for SIAs and evalutes
all the SIAs that did not have a matching GUID
}
\keyword{internal}
