% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generics.R, R/build.R
\name{dlt_add_column}
\alias{dlt_add_column}
\alias{dlt_add_column,DeltaTableBuilder,character,character-method}
\title{Add column to the build table}
\usage{
dlt_add_column(dtb, col_name, data_type, ...)

\S4method{dlt_add_column}{DeltaTableBuilder,character,character}(
  dtb,
  col_name,
  data_type,
  nullable = TRUE,
  generated_always_as = NULL,
  comment = NULL
)
}
\arguments{
\item{dtb}{DeltaTableBuilder}

\item{col_name}{character}

\item{data_type}{character}

\item{...}{unused}

\item{nullable}{optional, logical}

\item{generated_always_as}{optional, character}

\item{comment}{optional, character}
}
\value{
DeltaTableBuilder
}
\description{
Add column to the build table
}
\note{
dlt_add_column, since 1.0.0
}
\seealso{
\linkS4class{DeltaTableBuilder}
}
