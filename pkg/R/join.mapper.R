join.mapper <- function(map_name = "new_join_map", env = parent.frame(), obj_names, field_names = "*", clean = FALSE){
#' Create a Data Join Map
#'
#' \code{join.mapper} Creates a map with which datasets can be joined using \code{\link[data.table]{data.table}} methods.  Cross-environment objects are not supported, but objects in attached environment \emph{might} work.  Also, the output is given attribute \code{"env"} to store the value of \code{env}
#'
#' @param map_name The output map name to use in assignment
#' @param env The environment where source objects are found; this is also the environment of the assigned output.
#' @param obj_names (string[]) The names of objects to join
#' @param field_names (string[]) One or more strings and REGEX patterns used to define the field names to use for possible joins: matching is either REGEX or identity, and support for aliasing via \code{"primary_col==alias_col"} (note the quotes) is supported.
#' @param clean (logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output
#'
#' @return If a \code{\link[DBOE]{DBOE}} DSN environment is passed to \code{env}, a local environment is used to contain the output of special code that transforms \code{env$metamap} into environment objects that can be used for creating the map: this should be stored or piped into \code{\link{join.reduce}}; otherwise, a \code{\link[data.table]{data.table}} object used join datasets via \code{\link{join.reduce}}
#'
#' @importFrom book.of.features logic_map
#' @importFrom utils hasName
#' @export

  # :: Argument Handling ----
  # map_name
  map_name <- rlang::as_label(rlang::enexpr(map_name))

  # env: Special case of DBOE dsn environment
  if (utils::hasName(env, "metamap")){ data.table::setattr(env, "DBOE", TRUE) }

  # field_names
  fun <- \(x) list(
				natural_joins = x[!grepl("[=$\\^()]", x)] |> stringi::stri_split_regex(pattern = "[, |]", simplify = TRUE)
				, equi_joins	= x[grepl("[=]{2}", x)] 		|> stringi::stri_split_regex(pattern = "[, |]", simplify = TRUE)
				, fuzzy_joins	= x[grepl("[$\\^()]", x)] 	|> stringi::stri_split_regex(pattern = "[, |]", simplify = TRUE)
				);

  obj_names <- ls(pattern = paste(obj_names, collapse = "|"), envir = env);

  field_names <- fun(field_names) |>
  	purrr::imap(\(x, y){
  		vars <- stringi::stri_replace_all_fixed(x, " ", "", vectorize_all = FALSE)

  		if (rlang::is_empty(vars)){ NULL } else {
				rlang::exprs(
					natural_joins = intersect(names(.this), !!vars)
          , equi_joins = !!vars
          , fuzzy_joins = purrr::keep(names(.this), \(j) grepl(!!vars, j, ignore.case = TRUE))
					)[[y]]
  			}
		}) |>
  	purrr::compact();

  # :: Output ----
  if (clean){ rm(list = purrr::keep(map_name, exists, envir = env), envir = env) }

  assign(map_name, {
  	mget(obj_names, envir = env) |>
    purrr::imap_dfr(\(.this, .nm){
      # field_names has one or more of the following names: natural_joins, equi_joins, fuzzy_joins
      .fields <- purrr::map(field_names, \(x){
      		if (!rlang::is_empty(x)){
      			purrr::keep(eval(x), \(j){
    						(stringi::stri_split_regex(j, "[ =]", simplify = TRUE, tokens_only = TRUE) %in%
    						 	names(get(.nm, envir = env))) |>
    							any()
    					})
      		}
      	}) |>
      	magrittr::freduce(list(purrr::compact, unlist, unique));

      data.table::data.table(obj_name = .nm, field_names = .fields);
    }) |>
		define(
    	list(.SD[, .(field_names)], book.of.features::logic_map(obj_name))
    	, purrr::map(.SD, max, na.rm = TRUE) ~ field_names
    	, unique(.SD)
    	, idx = order(.SD[, !"field_names"] |> apply(1, sum))
    	) |>
  		data.table::setkey(idx) |>
  		data.table::setattr("env", env)
    }, envir = env);
}


