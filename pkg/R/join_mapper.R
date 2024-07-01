join_mapper <- function(map_name = "new_join_map", env = parent.frame(), obj_names, field_names = "*", clean = FALSE){
#' Create a Data Join Mapper
#'
#' \code{join_mapper} Creates a map with which datasets can be joined using \code{\link[data.table]{data.table}} methods.  Cross-environment objects are not supported, but objects in attached environment \emph{might} work.  Also, the output is given attribute \code{"env"} to store the value of \code{env}
#'
#' @param map_name The output map name to use in assignment
#' @param env The environment where source objects are found; this is also the environment of the assigned output.
#' @param obj_names (string[]) The names of objects to join
#' @param field_names (string[]) One or more strings and REGEX patterns used to define the field names to use for possible joins: matching is either REGEX or identity, and support for aliasing via \code{"primary_col==alias_col"} (note the quotes) is supported.
#' @param clean (logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output
#'
#' @return An object of class \code{\link{jmap}}
#'
#' @importFrom book.of.features logic_map
#' @importFrom utils hasName
#'
#' @aliases join.mapper
#' @family semantic architecture
#' @export

  # :: Argument Handling ----
  # map_name
  map_name <- rlang::as_label(rlang::enexpr(map_name))

  obj_names <- ls(pattern = paste(obj_names, collapse = "|"), envir = env)

  # field_names
  field_names <- (\(x){
			natural_joins <- x[!grepl("[=$\\^()]", x)]
			equi_joins	<- x[grepl("[=]{2}", x)] %>% .[!. %in% natural_joins]
			fuzzy_joins	<- x[grepl("[$\\^()]", x)] %>% .[!. %in% c(equi_joins, natural_joins)]
			res <- mget(ls()) |> lapply(stringi::stri_split_regex, pattern = "[, |]", simplify = TRUE)

			return(res)
	  })(field_names) |>
  	purrr::imap(\(field, join_type){
  		vars <- stringi::stri_replace_all_fixed(field, " ", "", vectorize_all = FALSE)

  		if (rlang::is_empty(vars)){
  			NULL
  		} else {
  			# `.this` is a placeholder to be evaluated later (almost a promise):
				rlang::exprs(
					natural_joins = intersect(names(.this), !!vars)
          , equi_joins = !!vars
          , fuzzy_joins = names(.this) |>
							purrr::keep(\(j) grepl(!!paste(vars, collapse = "|"), j, ignore.case = TRUE))
					)[[join_type]]
  			}
		}) |>
  	# Remove empty elements:
  	purrr::compact();

  # :: Output ----
  if (clean){ rm(list = purrr::keep(map_name, exists, envir = env), envir = env) }

  assign(map_name, {
  	# browser()
  	res <- { mget(obj_names, envir = env) |>
	    purrr::imap(\(.this, .obj){
	      # field_names has one or more of the following names:
	    	# - natural_joins
	    	# - equi_joins
	    	# - fuzzy_joins
	      .fields <- purrr::map(field_names, \(x){
	      		if (!rlang::is_empty(x)){
    					ref_obj <- get(.obj, envir = env)
	      			f <- eval(x) |>
	      				purrr::keep(\(j){
	    						(stringi::stri_split_regex(j, "[ =]", simplify = TRUE, tokens_only = TRUE) %in%
	    						 	names(ref_obj)) |>
	    							any()
	    					})
	      			f
	      		}
	      	}) |>
	      	magrittr::freduce(list(purrr::compact, unlist, unique));

	      # Output for the current loop:
	      data.table::data.table(
	      	obj_name = .obj
	      	, field_names = .fields
	      	# `n_vals`: Unique number of values in obj[[f]]
	      	, n_vals = sapply(.fields, \(f){
		      		data.table::uniqueN(env[[.obj]][[f]])
	      		})
	      	)
	    }) |>
  		data.table::rbindlist(fill = TRUE) |>
	  	na.omit() |>
  		purrr::modify_at("n_vals", unlist) |>
  		architect::define(
				# This section creates a simple one-hot encoding of possible object names:
		  	cbind(
		  		# Element 1
		  		.SD[, .(field_names)]
		  		# Element 2
		  		, book.of.features::logic_map(
		  				fvec = obj_name
			      	# `n_vals`: Unique number of values in obj_name[[field_names]]
		  				, avec = n_vals
		  				, bvec = unique(obj_name) |> rlang::set_names()
		  				)
					)
			# Encoded values are reduce by `field_names`:
			, purrr::map(.SD, max, na.rm = TRUE) ~ field_names
			, na.omit(unique(.SD))
			# `wgt`: a simple similarity weight:
			, wgt = apply(.SD[, !"field_names"], 1, \(x)  -sum(x/norm(matrix(x), "2")))
			, setorder(.SD, wgt)
			# # Create a row index based on the row-wise summation of encoded values:
			, idx = .GRP ~ wgt
			, data.table::setcolorder(.SD, "idx") |>
		  		# Enforce row sorting based on 'idx':
					data.table::setorder(idx)
			, ~. -wgt
			)
  	}

		jmap(map = res, env = env)
  }, envir = env);
}

#' @keywords internal
#' @export
join.mapper <- join_mapper

#' A Join Map
#'
#' Class \code{jmap} contains the results of a call to \code{\link{join_mapper}}
#'
#' @docType class
#' @name jmap
#' @slot map The map produced as a result of calling \code{\link{join_mapper}}
#' @slot env The environment of assignment
#' @export
jmap <- setClass(
	Class = "jmap"
	, package = "architect"
	, slots = c(map = "ANY", env = "environment")
	)
