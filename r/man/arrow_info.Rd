% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/arrow-package.R
\name{arrow_info}
\alias{arrow_info}
\title{Report information on the package's capabilities}
\usage{
arrow_info()
}
\value{
A list including version information, boolean "capabilities", and
statistics from Arrow's memory allocator, and also Arrow's run-time
information.
}
\description{
This function summarizes a number of build-time configurations and run-time
settings for the Arrow package. It may be useful for diagnostics.
}
