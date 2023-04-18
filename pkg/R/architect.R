define <- function(data, ...){
#' Define a Data Operation
#'
#' \code{define} allows one to operate on data using one or more formula-based definitions. Data transformation and selection can be achieved with formulas or using standard \code{\link[data.table]{data.table}} syntax in a procedural manner with a single function call.
#'
#' @details
#' If \code{x} is a \code{\link[smart.data]{smart.data}} object, the taxonomical references to field names can be accessed by using the \code{use()} syntax in the right-hand side of the formula (e.g., \code{~use(term1, term2) + otherTerm1 + ...})
#'
#' @param data The input data (see 'Details')
#' @param ... (\code{\link[rlang]{dots_list}}): Formulae for which the left-hand side (LHS) is an expression containing the operation, and the right-hand side (RHS) contains column names that form a grouping set for the operation (i.e., \code{<expression> ~ col_1 + col_2 + ...}):
#' \itemize{
#' \item{If the form \code{<LHS>~ .} is given, the LHS executes using all columns as the grouping set}
#' \item{If the form \code{<LHS>~ 1} is given, the LHS executes without grouping}
#' \item{If no LHS is given, the operation defaults to selection based on the RHS}
#' }
#'
#' @return The data modified
#' @export
  force(data);
  .smartData <- fun_expr <- by_args <- NULL;

	if ("smart.data" %in% utils::installed.packages()[, "Package"]){
		if (smart.data::is.smart(data)){
			.smartData <- data$clone(deep = TRUE)
			data <- data.table::copy(.smartData$data)
		}
	}
	if (!data.table::is.data.table(data)){ data <- data.table::as.data.table(data, keep.rownames = TRUE) }

  .ops <- rlang::enexprs(...);

  .func <- function(x, y){
    if (!rlang::is_formula(x)){
      fun_expr <- x;
      by_args <- character()
    } else {
	    f <- eval(unlist(x, recursive = FALSE)) |> data.table::setattr(".Environment", as.environment(data));

      fun_expr <- rlang::f_lhs(f = f);

      by_args <- stats::terms(f, data = data) |> attr("term.labels");

      if (!all(by_args %in% names(data))){ by_args <- character() }
    }

    .op <- if (!rlang::is_empty(fun_expr)){
	    	# Operation Branch
	    	.out_expr <- if (y != ""){
		      # Assignment branch
	          rlang::expr(data[, `:=`(!!y, !!fun_expr)])
	        } else {
		      # Operation-only branch
	          rlang::expr(data[, !!fun_expr])
	        }

	    	# data.table grouping "by"
	      if (!(rlang::is_empty(by_args) | identical(character(), by_args))){
	      	.out_expr$by <- by_args
	      }

	    	.out_expr
	    } else if (!rlang::is_empty(by_args)){
	    	# Selection Branch
	      rlang::expr(data[, c(!!!by_args)])
	    } else {
	    	# Identity
	    	rlang::expr(data)
	    }

    data <<- eval(.op);
  };

  purrr::iwalk(.ops, .func);
  return(invisible(data))
}
#
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
				)

  field_names <- purrr::imap(fun(field_names), \(x, y){
  		vars <- stringi::stri_replace_all_fixed(x, " ", "", vectorize_all = FALSE)

  		if (rlang::is_empty(vars)){ NULL } else {
				rlang::exprs(
					natural_joins = intersect(names(.this), !!vars)
          , equi_joins = !!vars
          , fuzzy_joins = purrr::keep(names(.this), \(j) grepl(!!vars, j, ignore.case = TRUE))
					)[[y]]
  			}
		}) |> purrr::compact()

  # :: Output ----
  if (clean){ rm(list = purrr::keep(map_name, exists, envir = env), envir = env) }

  assign(map_name, {
  	mget(ls(pattern = paste(obj_names, collapse = "|"), envir = env), envir = env) |>
    purrr::imap_dfr(~{
    	.this <- .x
      obj_name <- .y

      field_names <- purrr::map(purrr::flatten(field_names), \(x){
      		if (!rlang::is_empty(x)){
      			keep(eval(x), \(j) any(stringi::stri_split_regex(j, "[ =]", simplify = TRUE, tokens_only = TRUE) %in% names(get(obj_name, envir = env))))
      		}
      	}) |>
      	magrittr::freduce(list(purrr::compact, unlist, unique));

      data.table::data.table(obj_name, field_names);
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
#
join.reduce <- function(join_map, out_name, x_names, i_names, filters = TRUE, dt_key, source_env = attr(join_map, "env"), assign_env = attr(jmap, "env"), clean = FALSE){
#' Join-Reduce Multiple Datasets
#'
#' \code{join.reduce} leverages \code{\link[data.table]{data.table}} fast joins and \code{\link[purrr]{reduce}} from package \code{purr}
#'
#' @param join_map The input join-map (see \code{link{join.mapper}})
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

	# Remove rows from `join_map` with all zeroed entries
	join_map <- join_map[purrr::pmap_lgl(join_map[, mget(ls(pattern = x_names))], function(...){ sum(c(...)) > 0 })];

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

  .alpha <- { data.table::rbindlist(purrr::pmap(join_map, xion.func) |> purrr::compact())[
      !(on %ilike% "source"), .(on = list(I(on))), by = .(x, i)]
	  }

  .beta <- { .alpha[, list(rlang::new_box(.SD[, .(i, on)] |> apply(1, I))), by = x
	    ][
	    , purrr::imap(purrr::set_names(V1, x), ~{
			    .this = .y; .that = .x;
			    purrr::reduce(.x = .that, .f = join.func, .init = .this)
				 })
	    ]}

  # :: Output ----
  .output = purrr::map(.beta, ~{
    row_filter = filters[[1]]

    col_filter = filters[[2]]

    .out = eval(rlang::parse_expr(.x), envir = source_env)

    .out %<>% {
    	.[eval(row_filter), eval(col_filter)][, mget(discard(ls(), \(x) grepl("strptime|^i[.]", x, ignore.case = TRUE)))] |>
        unique() |>
        setnames("full_dt", "event_date", skip_absent = TRUE)
    }

    if (rlang::is_empty(dt_key)){ .out } else { data.table::setkeyv(.out, dt_key) |> data.table::setcolorder() }
  }) %>%
  	purrr::set_names(paste0("J_data_", stringi::stri_pad_left(seq_along(.), width = stringi::stri_length(as.character(length(.))))))

  if (clean){ rm(list = names(.output), envir = assign_env) }

  assign(out_name, if (rlang::has_length(.output, 1)){ .output[[1]] } else { output }, envir = assign_env)
}

