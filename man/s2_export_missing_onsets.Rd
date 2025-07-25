% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{s2_export_missing_onsets}
\alias{s2_export_missing_onsets}
\title{Export EPIDs with missing onset dates}
\usage{
s2_export_missing_onsets(data, polis_data_folder, output_folder_name)
}
\arguments{
\item{data}{\code{tibble} A tibble containing AFP surveillance data}

\item{polis_data_folder}{\code{str} String path to the POLIS data folder}

\item{output_folder_name}{\code{str} Name of the output directory where processed
files will be saved. Defaults to "Core_Ready_Files". For
region-specific processing, this should be set to
"Core_Ready_Files_REGION" (e.g., "Core_Ready_Files_AFRO").}
}
\value{
Invisibly returns the filtered data frame of records with missing
onset dates
}
\description{
Identifies AFP cases with missing date of onset and exports them to a CSV
file for further review and investigation.
}
\keyword{internal}
