join_reduce <- function(join_map, out_name, x_names, i_names, dt_key, source_env = attr(join_map, "env"), assign_env = attr(join_map, "env"), clean = FALSE){
#' Join-Reduce Multiple Datasets
#'
#' \code{join_reduce} leverages \code{\link[data.table]{data.table}} fast joins and \code{\link[purrr]{reduce}} from package \code{purr}
#'
#' @param join_map The input join-map (see \code{\link{join_mapper}})
#' @param out_name (symbol|string) The name of the output object
#' @param x_names (string[]) Names or REGEX patterns indicating the outer table: multiple values will be concatenated into a delimited string
#' @param i_names (string[]) Names or REGEX patterns indicating the inner table to be joined: multiple values will be concatenated into a delimited string
#' @param dt_key (string[], symbol[]) Names that participate in creating the \code{\link[data.table]{key}} for the output
#' @param source_env The environment from which the input should be sourced.  If empty, it defaults to slot \code{env} in \code{jmap}
#' @param assign_env The environment in which the output should be assigned.  If empty, it defaults to slot \code{env} in \code{jmap}
#' @param clean (logical) \code{TRUE} indicates that the target object should be removed from \code{env} before creating the output
#'
#' @importFrom magrittr %>% %<>% %T>%
#' @importFrom data.table %like% %ilike% .SD .N .GRP .I
#' @return Combined datasets assigned to the designated environment
#'
#' @section Reduction:
#' Datasets can be joined in different orders producing different results. \code{join_reduce} uses pairwise joins ranked by similarity (high-to-low) of common features and unique values in those features. Each joined pair is row-wise reduced and then finalized by taking only unique records.
#'
#' @aliases join.reduce
#' @family semantic architecture
#' @export

  # :: Argument Handling ----
	assertive::assert_all_are_true(c(
		is(join_map, "jmap")
		, is(join_map@map, "data.table")
		))

	out_name <- rlang::enexpr(out_name) |> as.character()

	if (missing(source_env)){
		source_env <- join_map@env
	}

	if (missing(assign_env)){
		assign_env <- source_env
	}
  if (!missing(dt_key)){
	    .tmp_key <- as.character(substitute(dt_key));
	    dt_key <- if (rlang::has_length(.tmp_key, 1)){ .tmp_key } else { .tmp_key[-1] }
  } else { dt_key <- NULL }

  # join_map
	join_map <- join_map@map

  # x_names
  if (!rlang::has_length(x_names, 1)){
  	x_names <- paste(x_names, collapse = "|")
  }

  # i_names
  if (rlang::is_empty(i_names)){
    i_names <- names(join_map) |>
		 	purrr::discard(\(x) grepl(x_names, x, ignore.case =TRUE)) |>
    	c("field_names") |>
    	paste(collapse = "|")
  } else {
  	if (!rlang::has_length(i_names, 1)){
  		i_names <- paste(i_names, collapse = "|")
  	} else { i_names }
  }

  if (!utils::hasName(join_map, "field_names")){
    stop("Column 'field_names' not found: exiting ...")
  } else {
  	data.table::setcolorder(join_map, "field_names")
  }

	# Remove rows from `join_map` with all zeroed entries:
	is_zero <- apply(
		X = join_map[, mget(ls(pattern = x_names))]
		, MARGIN = 1
		, FUN = function(i){ sum(i, na.rm = TRUE) > 0 })

	join_map <- join_map[(is_zero)];

	cos_sim <- \(x, y){
		(x %*% y)/prod(norm(as.matrix(x), "2"), norm(as.matrix(y), "2"))
	}

  # :: Output ----
	tmp_map <- join_map[, -c(1,2)] |>
		as.matrix() %>%
		magrittr::set_attr("dimnames", list(join_map$field_names, colnames(.)))

	tmp_env <- purrr::array_branch(tmp_map, 2) |>
		rlang::set_names(colnames(tmp_map)) |>
		list2env(envir = new.env())

	jplan <- tmp_env %$%
		combinat::combn(x = ls(), m = 2, fun = c()) |>
		t() |>
		data.table::as.data.table() |>
		data.table::setnames(c("outer","inner")) %>% {
			.[, `:=`(
					score = purrr::pmap(.SD, ~{
						rlang::list2(!!glue::glue("{.x}[{.y}]") := cos_sim(tmp_env[[.x]], tmp_env[[.y]]))
					}) |> unlist()
					, jcols = purrr::map2(outer, inner, \(o, i){
						res <- tmp_map[, c(o,i), drop = FALSE]
						res[apply(res, 1, \(x) length(x[x > 0]) == 2), ] |> rownames()
					})
					)
			][
			, expr := sprintf("%s[%s, on = %s, allow.cartesian = TRUE]", .SD$outer, inner, jcols)
			] |>
			data.table::setorderv(c("score", "outer"), order = c(-1, 1))
		}

	res <- jplan$expr |>
		purrr::map(\(x) str2lang(x) |> eval()) |>
		data.table::rbindlist(fill = TRUE) |>
		unique() |>
		purrr::discard(\(x) all(is.na(x)))

	if (!rlang::is_empty(res)){
		if (clean){
			spsUtil::quiet(rm(list = out_name, envir = assign_env))
		}
		assign(out_name, res, envir = assign_env)
	} else {
		cli::cli_alert_danger("Nothing to assign: all records filterd out!")
	}
}

#' @keywords internal
#' @export
join.reduce <- join_reduce
