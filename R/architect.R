#' @title
#' Intelligent Data Architecture
#'
#' @description
#' \code{architect} is a \code{\link[data.table]{data.table}}-driven framework for intelligent data architecture.
#'
#' @importFrom book.of.utilities %tf% %?% %??%
#' @name architect
NULL

define <- function(.data, ...){
#' Define a Data Operation
#'
#' \code{define} allows one to operate on data using one or more formula-based definitions. Data transformation and selection can be achieved with formulas or using standard \code{\link[data.table]{data.table}} syntax in a procedural manner with a single function call.
#'
#' @details
#' If \code{x} is a \code{\link[smart.data]{smart.data}} object, the taxonomical references to field names can be accessed by using the \code{use()} syntax in the right-hand side of the formula (e.g., \code{~use(term1, term2) + otherTerm1 + ...})
#'
#' @param x The input data (see 'Details')
#' @param ... (\code{\link[rlang]{dots_list}}): Formulae for which the left-hand side (LHS) is an expression containing the operation, and the right-hand side (RHS) contains column names that form a grouping set for the operation (i.e., \code{<expression> ~ col_1 + col_2 + ...}):
#' \itemize{
#' \item{If the form \code{<LHS>~ .} is given, the LHS executes using all columns as the grouping set}
#' \item{If the form \code{<LHS>~ 1} is given, the LHS executes without grouping}
#' \item{If no LHS is given, the operation defaults to selection based on the RHS}
#' }
#'
#' @return The data modified
#' @export
  force(.data);
  .smartData <- fun_expr <- by_args <- NULL;

	if ("smart.data" %in% installed.packages()[, "Package"]){
		if (smart.data::is.smart(.data)){
			.smartData <- .data$clone(deep = TRUE)
			.data <- data.table::copy(.smartData$data)
		}
	}
	if (!data.table::is.data.table(.data)){ .data <- data.table::as.data.table(.data, keep.rownames = TRUE) }

  .ops <- rlang::enexprs(..., .homonyms = "error", .check_assign = TRUE);

  purrr::iwalk(.ops, ~{
    env <- as.environment(.data);
    nm	<- .y

    if (!rlang::is_formula(.x)){
      fun_expr <- .x;
      by_args <- character()
    } else {
	    f 	<- eval(unlist(.x, recursive = FALSE));
      fun_expr <- rlang::f_lhs(data.table::setattr(f, ".Environment", env));
      by_args <- purrr::map(attr(terms(f), "term.labels"), ~{
					c(parse(text = glue::glue(".smartData${grep('use[()]', .x, value = TRUE)} |> names()"))
						, glue::glue("{grep('use[()]', .x, value = TRUE, invert = TRUE)}")
						) |>
					purrr::compact() |>
					sapply(eval, envir = environment())
				}) |> unlist();

      if (!all(by_args %in% names(.data))){ by_args <- NULL }
    }

    .op <- if (!rlang::is_empty(fun_expr)){
	    	# Operation Branch
	    	.out_expr = if (.y != ""){
		      # Assignment branch
	          rlang::expr(.data[, `:=`(!!.y, !!fun_expr)])
	        } else {
		      # Operation-only branch
	          rlang::expr(.data[, !!fun_expr])
	        }

	      if (!(rlang::is_empty(by_args) | identical(character(), by_args))){
	      	.out_expr$by <- by_args
	      }

	    	print(.out_expr)
	    } else if (!rlang::is_empty(by_args)){
	    	# Selection Branch
	      rlang::expr(.data[, c(!!!by_args), with = FALSE])
	    } else {
	    	# Identity
	    	.data
	    }

    .data <<- eval(.op);
  });

  return(.data)
}
#
join.mapper <- function(map_name = "new_join_map", obj_names, field_names = "*", env = parent.frame(), clean = FALSE){
#' Create a Data Join Map
#'
#' \code{join.mapper} Creates a map with which datasets can be joined using \code{\link[data.table]{data.table}} methods.  Cross-environment objects are not supported, but objects in attached environment \emph{might} work.  Also, the output is given attribute \code{"env"} to store the value of \code{env}
#'
#' @param map_name The output map name to use in assignment
#' @param obj_names (string[]) The names of objects to join
#' @param field_names (string[]) One or more strings and REGEX patterns used to define the field names to use for possible joins: matching is either REGEX or identity, and support for aliasing via \code{"primary_col==alias_col"} (note the quotes) is supported.
#' @param env The environment where source objects are found; this is also the environment of the assigned output.
#' @param clean (logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output
#'
#' @return If a \code{\link[DBOE]{DBOE}} DSN environment is passed to \code{env}, a local environment is used to contain the output of special code that transforms \code{env$metamap} into environment objects that can be used for creating the map: this should be stored or piped into \code{\link{join.reduce}}; otherwise, a \code{\link[data.table]{data.table}} object used join datasets via \code{\link{join.reduce}}
#'
#' @export
  # :: Argument Handling ----
  # map_name
  map_name <- {
    x = rlang::enexpr(map_name) |> as.character()
    if (rlang::has_length(x, 1)){ x } else { x[-1] %>% .[1] }
  }

  # field_names
  field_names <- purrr::map_chr(field_names, ~{
    if (any(stri_detect_fixed(deparse(.x), "=="))){
      stringi::stri_replace_all_fixed(.x, " ", "", vectorize_all = FALSE)
    } else { rlang::inject(substitute(!!.x) %>% as.character()) }
  }) |> unlist()

  # env: Special case of DBOE dsn environment: virtual objects are created for the purpose of creating the map
  .pipe_env <- FALSE;

  if (rlang::env_has(env, "metamap")){
    .pipe_env <- TRUE;
    .db <- env$metamap$database[1];

    env <- env$metamap |>
      split(by = c("schema_name", "tbl_name")) %>%
      purrr::map(~.x[, rep.int(1, .N) |> purrr::set_names(col_name) |> as.list(), by = .(schema_name, tbl_name)]) |>
      list2env(envir = new.env() |>setattr("DBOE", TRUE))
  }

  # :: Output ----
  .queue = mget(ls(pattern = paste(obj_names, collapse = "|"), envir = env), envir = env)

  if (clean){ rm(list = purrr::keep(map_name, exists, envir = env), envir = env) }

  .output = {
    purrr::imap_dfr(.queue, ~{
      .this = .x;

      data.table::data.table(
        obj_name = .y
        , field_names = map(field_names, ~{
          .fn = .x;
          ._1 = .fn[.fn %in% names(.this)]
          ._2 = purrr::keep(names(.this), ~grepl(.fn, .x))
          ._3 = if (any(purrr::map_lgl(names(.this), ~any(stringi::stri_detect_fixed(str = .fn, pattern = "==")) & grepl(.x, .fn)))){ .fn } else { NULL }

          c(._1, ._2, ._3)
        }) |> unlist() |> unique()
      )
    }) %>% {
      .[!is.na(field_names)
      ][, c(list(field_names = field_names), book.of.features::logic_map(.SD$obj_name))
      ][, purrr::map(.SD, max), by = field_names
      ][, unique(.SD)
      ][, idx := order(.SD[, !"field_names"] |> apply(1, sum))
      ] %T>% data.table::setkey(idx)
    }
  }

  assign(map_name, data.table::setattr(.output, "env", env), envir = env)
  if (.pipe_env){ return(env) }
}
#
join.reduce <- function(jmap, x.names, i.names, filters = TRUE, dt.key, env, clean = FALSE, .debug = FALSE){
#' Join-Reduce Multiple Datasets
#'
#' \code{join.reduce} leverages \code{\link[data.table]{data.table}} fast joins and \code{\link[purrr]{reduce}} from package \code{purr}
#'
#' @param jmap The join-map (see \code{link{join.mapper}})
#' @param x.names (string[]) Names or REGEX patterns indicating the outer table: multiple values will be concatenated into a delimited string
#' @param i.names (string[]) Names or REGEX patterns indicating the inner table to be joined: multiple values will be concatenated into a delimited string
#' @param filters  (expression[[]]) \code{data.table}-friendly expression to limit the rows of the output. A length-2 list indicates \emph{row} and \emph{column} expressions respectively.
#' @param dt.key (string[], symbol[]) Names that participate in creating the \code{\link[data.table]{key}} for the output
#' @param env The environment in which the output should be assigned.  If empty, it defaults to \code{attr(jmap, "env")}
#' @param clean (logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output
#' @param .debug (logical) Debugging flag: no output is produced
#'
#' @return Combined datasets assigned to the designated environment
#' @export

  # :: Argument Handling ----
  # env
  if (missing(env)){ env <- attr(jmap, "env") }

  # x.names
  if (!rlang::has_length(x.names, 1)){ x.names <- paste(x.names, collapse = "|") }

  # i.names
  if (rlang::is_empty(i.names)){
    i.names <- paste(names(jmap) %>% discard(~.x %ilike% x.names) %>% c("field_names"), collapse = "|")
  } else { if (!rlang::has_length(i.names, 1)){ i.names <- paste(i.names, collapse = "|") } else { i.names } }

  # jmap
  if (!data.table::is.data.table(jmap)){ jmap %<>% data.table::as.data.table() }
  if (!"field_names" %in% names(jmap)){
    stop("Column 'field_names' not found: exiting ...")
  } else { data.table::setcolorder(jmap, "field_names") }
  if (!all(jmap[, !c("field_names", "idx")] %>% unlist() %>% unique() %in% c(0, 1))){
    stop("Join map contains values other than zero (0) or (1) after the first column: exiting ...")}
  jmap %<>% .[pmap_lgl(jmap[, mget(ls(pattern = x.names))], function(...){ sum(c(...)) > 0 })];

  # filters
  if (!is.list(filters)){ filters <- as.list(filters) }
  if (rlang::has_length(filters, 1)){ filters <- append(filters, list(quote(mget(ls())))) } else if (!rlang::has_length(2)){ filters <- filters[c(1:2)] }

  # dt.key
  if (!missing(dt.key)){ dt.key <- {
    .tmp_key = substitute(dt.key) %>% as.character();

    if (rlang::has_length(.tmp_key, 1)){ .tmp_key } else { .tmp_key[-1] }
  }
  } else { dt.key <- NULL}

  # :: Definitions ----
  xion.func = function(...){
    # Creates a list that is 'data.table'-join-ready
    ._j = list(...);
    ._i = ...names()[(._j == 1) & (grepl(i.names, ...names(), ignore.case = TRUE))];
    if (length(._i) == 0){ return(NULL) };

    ._x = ...names()[(._j == 1) & (grepl(x.names, ...names(), ignore.case = TRUE))];
    ._on = ._j[[1]];

    list(x = c(._x), i = c(._i), on = c(._on)) |> expand.grid()
  }

  join.func = function(cur, nxt){ sprintf(
    fmt = "%s[%s, on = c(%s), allow.cartesian = TRUE]"
    , cur
    , nxt[1]
    , paste(nxt[2] |> unlist() |> sprintf(fmt = "'%s'"), collapse = ", ")
	  )
  }

  .alpha = { purrr::pmap(jmap, xion.func) |>
      purrr::compact() |>
      data.table::rbindlist() %>%
      .[!(on %ilike% "source"), .(on = list(I(on))), by = .(x, i)]
  }

  .beta = { .alpha[
	    , list(rlang::new_box(.SD[, .(i, on)] |> apply(1, I))), by = x][
	    , purrr::imap(set_names(V1, x), ~{
			    .this = .y; .that = .x;
			    purrr::reduce(.x = .that, .f = join.func, .init = .this)
				 })
	    ]}

  if (.debug){ print(list(alpha = .alpha, beta = .beta)) }

  # :: Output ----
  .output = purrr::map(.beta, ~{
    rfilter = filters[[1]]

    cfilter = filters[[2]]

    .out = eval(str2lang(.x), envir = attr(jmap, "env"))

    .out %<>% {
    	.[eval(rfilter), eval(cfilter)][, mget(discard(ls(), ~.x %ilike% "strptime|^i[.]"))] %>%
        unique() %>%
        setnames("full_dt", "event_date", skip_absent = TRUE)
    }

    if (rlang::is_empty(dt.key)){ .out } else { data.table::setkeyv(.out, dt.key) |> data.table::setcolorder() }

  }) %>%
  	purrr::set_names(paste0("J_data_", stringi::stri_pad_left(seq_along(.), width = stringi::stri_length(as.character(length(.))))))

  if (clean){ rm(list = names(.output), envir = env) }

  list2env(.output, envir = env)
}

