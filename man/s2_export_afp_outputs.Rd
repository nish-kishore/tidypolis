% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{s2_export_afp_outputs}
\alias{s2_export_afp_outputs}
\title{Export AFP data and related outputs}
\usage{
s2_export_afp_outputs(
  data,
  latest_archive,
  polis_data_folder,
  col_afp_raw,
  output_folder_name,
  output_format,
  archive = TRUE
)
}
\arguments{
\item{data}{A data frame containing processed AFP data}

\item{latest_archive}{String containing the name of the latest archive folder}

\item{polis_data_folder}{String path to the POLIS data folder}

\item{col_afp_raw}{Names of original columns for comparison}

\item{output_folder_name}{\code{str} Name of the output directory where processed
files will be saved. Defaults to "Core_Ready_Files". For
region-specific processing, this should be set to
"Core_Ready_Files_REGION" (e.g., "Core_Ready_Files_AFRO").}

\item{output_format}{\code{str} output_format to save files as.
Available formats include 'rds' 'rda' 'csv' and 'parquet', Defaults is
'rds'.}

\item{archive}{Logical. Whether to archive previous output directories
before overwriting. Default is \code{TRUE}.}
}
\value{
Invisibly returns NULL
}
\description{
Creates and exports various AFP data files including the main AFP linelist,
spatial data files, and comparison with previous datasets.
}
\keyword{internal}
