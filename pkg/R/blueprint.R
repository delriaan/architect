#' Definition Blueprint (EXPERIMENTAL)
#'
#' Class \code{blueprint} is set as an attribute of the output from each successfully completed invocation of \code{\link{define}}().  Blueprints execute before the \code{...} arguments and are appended with each call.  The idea is to be able to recreate a series of operations by using the \code{blueprint} as a starting point with a compatible dataset (i.e., contains enough features to satisfy the blueprint).
#'
#' @docType class
#' @name blueprint
#' @slot schema A list of \code{\link{define}} operation expressions
#' @export
blueprint <- setClass(Class = "blueprint", package = "architect", slots = c(schema = "list"))

