% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generics.R, R/merge.R
\name{dlt_when_matched_delete}
\alias{dlt_when_matched_delete}
\alias{dlt_when_matched_delete,DeltaMergeBuilder,missing-method}
\alias{dlt_when_matched_delete,DeltaMergeBuilder,Column-method}
\alias{dlt_when_matched_delete,DeltaMergeBuilder,character-method}
\title{Delete row from the table}
\usage{
dlt_when_matched_delete(dmb, condition)

\S4method{dlt_when_matched_delete}{DeltaMergeBuilder,missing}(dmb)

\S4method{dlt_when_matched_delete}{DeltaMergeBuilder,Column}(dmb, condition)

\S4method{dlt_when_matched_delete}{DeltaMergeBuilder,character}(dmb, condition)
}
\arguments{
\item{dmb}{DeltaMergeBuilder}

\item{condition}{optional, character or Column}
}
\value{
DeltaMergeBuilder
}
\description{
Delete row from the table
}
\note{
dlt_when_matched_delete, since 1.0.0
}
\seealso{
\linkS4class{DeltaMergeBuilder}
}
