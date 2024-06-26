% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/join_reduce.R
\name{join_reduce}
\alias{join_reduce}
\alias{join.reduce}
\title{Join-Reduce Multiple Datasets}
\usage{
join_reduce(
  join_map,
  out_name,
  x_names,
  i_names,
  dt_key,
  source_env = attr(join_map, "env"),
  assign_env = attr(join_map, "env"),
  clean = FALSE
)
}
\arguments{
\item{join_map}{The input join-map (see \code{\link{join_mapper}})}

\item{out_name}{(symbol|string) The name of the output object}

\item{x_names}{(string[]) Names or REGEX patterns indicating the outer table: multiple values will be concatenated into a delimited string}

\item{i_names}{(string[]) Names or REGEX patterns indicating the inner table to be joined: multiple values will be concatenated into a delimited string}

\item{dt_key}{(string[], symbol[]) Names that participate in creating the \code{\link[data.table]{key}} for the output}

\item{source_env}{The environment from which the input should be sourced.  If empty, it defaults to slot \code{env} in \code{jmap}}

\item{assign_env}{The environment in which the output should be assigned.  If empty, it defaults to slot \code{env} in \code{jmap}}

\item{clean}{(logical) \code{TRUE} indicates that the target object should be removed from \code{env} before creating the output}
}
\value{
Combined datasets assigned to the designated environment
}
\description{
\code{join_reduce} leverages \code{\link[data.table]{data.table}} fast joins and \code{\link[purrr]{reduce}} from package \code{purr}
}
\section{Reduction}{

Datasets can be joined in different orders producing different results. \code{join_reduce} uses pairwise joins ranked by similarity (high-to-low) of common features and unique values in those features. Each joined pair is row-wise reduced and then finalized by taking only unique records.
}

\seealso{
Other semantic architecture: 
\code{\link{join_mapper}()}
}
\concept{semantic architecture}
