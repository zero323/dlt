% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generics.R, R/merge.R
\name{dlt_when_matched_update_all}
\alias{dlt_when_matched_update_all}
\alias{dlt_when_matched_update_all,DeltaMergeBuilder,missing-method}
\alias{dlt_when_matched_update_all,DeltaMergeBuilder,Column-method}
\alias{dlt_when_matched_update_all,DeltaMergeBuilder,character-method}
\title{Update all columns of the matched table}
\usage{
dlt_when_matched_update_all(dmb, condition)

\S4method{dlt_when_matched_update_all}{DeltaMergeBuilder,missing}(dmb)

\S4method{dlt_when_matched_update_all}{DeltaMergeBuilder,Column}(dmb, condition)

\S4method{dlt_when_matched_update_all}{DeltaMergeBuilder,character}(dmb, condition)
}
\arguments{
\item{dmb}{DeltaMergeBuilder}

\item{condition}{optional, character or Column}
}
\value{
DeltaMergeBuilder
}
\description{
Update all columns of the matched table
}
\note{
dlt_when_matched_update_all, since 1.0.0
}
\seealso{
\linkS4class{DeltaMergeBuilder}
}
