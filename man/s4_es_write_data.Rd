% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/utils.R
\name{s4_es_write_data}
\alias{s4_es_write_data}
\title{Write out final ES data}
\usage{
s4_es_write_data(polis_data_folder, es.05, output_folder_name, output_format)
}
\arguments{
\item{polis_data_folder}{\code{str} Path to the POLIS data folder.}

\item{es.05}{\code{tibble} The latest ES download with variables checked
against the last download, variables validated and sites checked and
CDC variables enforced}

\item{output_folder_name}{\code{str} Name of the output directory where processed
files will be saved. Defaults to "Core_Ready_Files". For
region-specific processing, this should be set to
"Core_Ready_Files_REGION" (e.g., "Core_Ready_Files_AFRO").}

\item{output_format}{\code{str} output_format to save files as.
Available formats include 'rds' 'rda' 'csv' and 'parquet', Defaults is
'rds'.}
}
\value{
\code{NULL} invisible return with write out to logs if necessary
}
\description{
Write out final ES data
}
\keyword{internal}
