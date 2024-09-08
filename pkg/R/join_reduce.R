join_reduce <- function(join_map, out_name, x_names, i_names, source_env = attr(join_map, "env"), assign_env = attr(join_map, "env"), clean = FALSE, finalizer = eval){
#' Join-Reduce Multiple Datasets (experimental)
#'
#' \code{join_reduce} leverages \code{\link[data.table]{data.table}} fast joins and \code{\link[purrr]{reduce}} from package \code{purr}
#'
#' @param join_map The input join-map (see \code{\link{join_mapper}})
#' @param out_name (symbol|string) The name of the output object
#' @param x_names (string[]) Names or REGEX patterns indicating the outer table(s): multiple values will be concatenated into a delimited string
#' @param i_names (string[]) Names or REGEX patterns indicating the inner table to be joined: multiple values will be concatenated into a delimited string
#' @param source_env The environment from which the input should be sourced.  If empty, it defaults to slot \code{env} in \code{jmap}
#' @param assign_env The environment in which the output should be assigned.  If empty, it defaults to slot \code{env} in \code{jmap}
#' @param clean (logical) \code{TRUE} indicates that the target object should be removed from \code{env} before creating the output
#' @param finalizer A function that operates on the result just before exiting \code{join_reduce}. Note that the function must take the object as the first argument and be \code{\link[data.table]{data.table}}-compatible.
#'
#' @importFrom magrittr %>% %<>% %T>%
#' @importFrom data.table %like% %ilike% .SD .N .GRP .I
#'
#' @return A combined dataset or list of such assigned to \code{assign_env[[out_name]]}
#'
#' @section Reduction Order:
#' Datasets can be joined in different orders producing different results according to \code{\link[data.table]{data.table}}'s join rules. \code{join_reduce} uses pairwise joins ranked by similarity (high-to-low) of common features and unique values in those features. Each joined pair is collapsed into a sequential join order, so it is important to choose \code{x_names} and \code{i_names} carefully based on desired results.
#' When \code{x_names} has multiple matched object names, reduction occurs for each object yielding potentially similar information across multiple objects. At this stage, one could take the resultant list and perform the operation again or use explicit \code{\link[data.table]{data.table}} join definitions over common fields (\emph{recommended}).
#'
#' @aliases join.reduce
#' @family semantic architecture
#' @export

	# :: Functions ----
	cos_sim <- \(x, y) (x %*% y)/prod(norm(as.matrix(x), "2"), norm(as.matrix(y), "2"))

	xion <- \(x, i, on) glue::glue("x[i, on = .({on}), allow.cartesian = TRUE]") |> str2lang()

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

	join_map <- join_map@map

  # x_names:
  if (!rlang::has_length(x_names, 1)){
  	x_names <- paste(x_names, collapse = "|")
  }

  # i_names:
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

	# browser()
	# Remove rows from `join_map` with all zeroed entries:
	is_zero <- apply(
		X = join_map[, mget(ls(pattern = x_names))]
		, MARGIN = 1
		, FUN = function(i){ sum(i, na.rm = TRUE) == 0 })

	join_map <- join_map[!(is_zero)];

  # :: Output ----
	tmp_map <- join_map[, -c(1,2)] |>
		as.matrix() %>%
		magrittr::set_attr("dimnames", list(join_map$field_names, colnames(.)))

	tmp_env <- purrr::array_branch(tmp_map, 2) |>
		rlang::set_names(colnames(tmp_map)) |>
		list2env(envir = new.env())

	# The join plan:
	jplan <- tmp_env %$% { combinat::combn(x = ls(), m = 2, fun = c()) } %>%
		t() |>
		# Swap positions by row if `outer` does not align with values matched by `x_names`:
    apply(1, \(oi) if (grepl(x_names, oi[1])){ oi } else {rev(oi) }) |>
    t() |>
		data.table::as.data.table() |>
		data.table::setnames(c("outer","inner")) %>%
		.[, score := purrr::map2_dbl(outer, inner, \(o, i) cos_sim(tmp_env[[o]], tmp_env[[i]]))] %>%
		.[, jcols := purrr::map2(outer, inner, \(o, i){
					res <- tmp_map[, c(o,i), drop = FALSE]

					res <- res[apply(res, 1, \(x) length(x[x > 0]) == 2), , drop = FALSE] |> rownames()

					if (rlang::is_empty(res)){	NA } else {	paste(res, collapse = ", ") }
				})] %>%
		.[(!grepl("NULL", jcols))] |>
		na.omit() |>
		data.table::setorderv(c("score", "outer"), order = c(-1, 1)) |>
		# Split by the outer object label in the case where multiple values for `x_name` were provided:
		split(by = "outer")

	# `join_template` is a pre-formatting string for use with glue::glue() and
	# produces a parsable expression once populated:
	join_template <- "[{j$inner}, on = .({j$jcols}), allow.cartesian = TRUE]"

	res <- jplan |>
		purrr::imap(\(j, x){
			# `next_join` is a list of column names that are common between the
			# current `inner` and the next `inner`. Since there already exists a
			# join plan, this sequence can check for common fields that exist in the
			# next dataset that can be used to join. This effectively combines
			# prescribed natural joins with inherent ones:
			next_join <- if (length(j$inner) > 1){
						j$inner |>
						rlang::syms() |>
						purrr::map(\(x) eval(x, envir = source_env) |> names()) |>
						purrr::accumulate(\(cur, nxt) table(c(cur, nxt)) %>% .[. > 1] |> names()) |>
						purrr::modify_at(1, ~character())
					} else { NULL }

			if (!rlang::is_empty(next_join)){
				j$jcols <- purrr::map2_chr(j$jcols, next_join, \(x, y) paste(c(x, unlist(y)), collapse = ","))
			}

			jp <- str2lang(paste0(x, paste(glue::glue(join_template), sep = "", collapse = "")))

			eval(jp, envir = source_env) |>
				unique() |>
				purrr::discard(\(x) all(is.na(x))) |>
				finalizer() |>
				data.table::setattr("join_plan", jp)
		})

	if (!rlang::is_empty(res)){
		if (clean) spsUtil::quiet(rm(list = out_name, envir = assign_env))
		if (length(res) == 1){ res <- res[[1]] }
		assign(out_name, res, envir = assign_env)
	} else {
		cli::cli_alert_danger("Nothing to assign: all records filterd out!")
	}
}

#' @keywords internal
#' @export
join.reduce <- join_reduce
