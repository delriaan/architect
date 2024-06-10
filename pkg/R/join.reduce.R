join.reduce <- function(join_map, out_name, x_names, i_names, filters = TRUE, dt_key, source_env = attr(join_map, "env"), assign_env = attr(join_map, "env"), clean = FALSE){
#' Join-Reduce Multiple Datasets
#'
#' \code{join.reduce} leverages \code{\link[data.table]{data.table}} fast joins and \code{\link[purrr]{reduce}} from package \code{purr}
#'
#' @param join_map The input join-map (see \code{\link{join.mapper}})
#' @param out_name The name of the output object
#' @param x_names (string[]) Names or REGEX patterns indicating the outer table: multiple values will be concatenated into a delimited string
#' @param i_names (string[]) Names or REGEX patterns indicating the inner table to be joined: multiple values will be concatenated into a delimited string
#' @param filters  (expression[[]]) \code{data.table}-friendly expression to limit the rows of the output. A length-2 list indicates \emph{row} and \emph{column} expressions respectively.
#' @param dt_key (string[], symbol[]) Names that participate in creating the \code{\link[data.table]{key}} for the output
#' @param source_env The environment from which the input should be sourced.  If empty, it defaults to \code{attr(jmap, "env")}
#' @param assign_env The environment in which the output should be assigned.  If empty, it defaults to \code{attr(jmap, "env")}
#' @param clean (logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output
#'
#' @importFrom magrittr %>% %<>% %T>%
#' @importFrom data.table %like% %ilike% .SD .N .GRP .I
#' @return Combined datasets assigned to the designated environment
#'
#' @export

  # :: Argument Handling ----
  # x_names
  if (!rlang::has_length(x_names, 1)){ x_names <- paste(x_names, collapse = "|") }

  # i_names
  if (rlang::is_empty(i_names)){
    i_names <- paste(names(join_map) |> purrr::discard(\(x) grepl(x_names, x, ignore.case =TRUE)) |> c("field_names"), collapse = "|")
  } else {
  	if (!rlang::has_length(i_names, 1)){ i_names <- paste(i_names, collapse = "|") } else { i_names }
  }

  # join_map
  if (!data.table::is.data.table(join_map)){ join_map %<>% data.table::as.data.table() }

  if (!utils::hasName(join_map, "field_names")){
    stop("Column 'field_names' not found: exiting ...")
  } else {
  	data.table::setcolorder(join_map, "field_names")
  }
  if (!all(join_map[, !c("field_names", "idx")] |> unlist() |> unique() %in% c(0, 1))){
    stop("Join map contains values other than zero (0) or (1) after the first column: exiting ...")
  }

	# Remove rows from `join_map` with all zeroed entries:
	.all_zeroes <- purrr::pmap_lgl(join_map[, mget(ls(pattern = x_names))], function(...){ sum(c(...)) > 0 })
	join_map <- join_map[.all_zeros];

  # filters
  if (!is.list(filters)){ filters <- as.list(filters) }

  if (rlang::has_length(filters, 1)){
  	filters <- append(filters, list(quote(mget(ls()))))
  } else if (!rlang::has_length(2)){
  	filters <- filters[c(1:2)]
  }

  # dt_key
  if (!missing(dt_key)){
  	dt_key <- {
	    .tmp_key = as.character(substitute(dt_key));
	    if (rlang::has_length(.tmp_key, 1)){ .tmp_key } else { .tmp_key[-1] }
	  }
  } else { dt_key <- NULL}

  # :: Definitions ----
  xion.func <- function(...){
    # Creates a list that is 'data.table'-join-ready
    ._j = list(...);
    ._i = ...names()[(._j == 1) & (grepl(i_names, ...names(), ignore.case = TRUE))];
    if (length(._i) == 0){ return(NULL) };

    ._x = ...names()[(._j == 1) & (grepl(x_names, ...names(), ignore.case = TRUE))];
    ._on = ._j[[1]];

    list(x = c(._x), i = c(._i), on = c(._on)) |> expand.grid()
  }

  join.func <- function(cur, nxt){ sprintf(
    fmt = "%s[%s, on = c(%s), allow.cartesian = TRUE]"
    , cur
    , nxt[1]
    , paste(nxt[2] |> unlist() |> sprintf(fmt = "'%s'"), collapse = ", ")
	  )
  }

  .alpha <- local({
	  	._x <- data.table::rbindlist(purrr::pmap(join_map, xion.func) |> purrr::compact());
	  	._x[!grepl("source", on, ignore.case = TRUE)
	  			, list(on = list(I(on)))
	  			, by = list(x, i)
	  			]
	  })

  .beta <- { .alpha[
  		, list(rlang::new_box(.SD[, .(i, on)] |> apply(1, I)))
  		, by = x
	    ][
	    , purrr::imap(purrr::set_names(V1, x), ~{
			    .this = .y; .that = .x;
			    purrr::reduce(.x = .that, .f = join.func, .init = .this)
				 })
	    ]
	  }

  # :: Output ----
  .output = purrr::map(.beta, ~{
	    row_filter = filters[[1]]

	    col_filter = filters[[2]]

	    .out = eval(rlang::parse_expr(.x), envir = source_env)

	    .out %<>% {
	    	.[eval(row_filter), eval(col_filter)
	  		][, mget(purrr::discard(ls(), \(x) grepl("strptime|^i[.]", x, ignore.case = TRUE)))
	  		] |>
	      unique() |>
	      data.table::setnames("full_dt", "event_date", skip_absent = TRUE)
	    }

	    if (rlang::is_empty(dt_key)){ .out } else {
	    	data.table::setkeyv(.out, dt_key) |> data.table::setcolorder()
	    }
	  }) %>%
  	purrr::set_names(paste0(
  		"J_data_"
  		, stringi::stri_pad_left(seq_along(.), width = stringi::stri_length(as.character(length(.))))
  		))

  if (clean){ rm(list = names(.output) |> purrr::keep(\(x) hasName(assign_env, x)), envir = assign_env) }

  assign(out_name, if (rlang::has_length(.output, 1)){ .output[[1]] } else { output }, envir = assign_env)
}
