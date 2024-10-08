% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/join_mapper.R
\name{join_mapper}
\alias{join_mapper}
\alias{join.mapper}
\title{Create a Data Join Mapper (experimental)}
\usage{
join_mapper(
  map_name = "new_join_map",
  env = parent.frame(),
  obj_names,
  field_names = "*",
  clean = FALSE
)
}
\arguments{
\item{map_name}{The output map name to use in assignment}

\item{env}{The environment where source objects are found; this is also the environment of the assigned output.}

\item{obj_names}{(string[]) The names of objects to join}

\item{field_names}{(string[]) One or more strings and REGEX patterns used to define the field names to use for possible joins. Matching is either \emph{regex}-based or identity: equi-joins (e.g., col_a==col_b) are not supported at this time. Specifying datasets in \code{obj_names} that do not have fields matched by \code{field_names} will result in an error. Each object in \code{obj_names} must have a field matched in \code{field_names}}

\item{clean}{(logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output}
}
\value{
An object of class \code{\link{jmap}}
}
\description{
\code{join_mapper} Creates a 'field-to-dataset' map with which datasets can be joined using \code{\link[data.table]{data.table}} methods. Cross-environment objects are not supported including attached environments.
}
\note{
A poorly-specified map or incompatible dataset structure will result in an empty map.
}
\seealso{
Other semantic architecture: 
\code{\link{join_reduce}()}
}
\concept{semantic architecture}
