join.mapper <- function(map_name = "new_join_map", obj_names, field_names = "*", env = parent.frame(), clean = FALSE){ 
	#' Create a Join Map
	#' 
	#' Cross-environment objects are not supported, but objects in attached environment \emph{might} work.  Also, the output is given attribute \code{"env"} to store the value of \code{env}
	#' 
	#' @param map_name The output map name to use in assignment
	#' @param obj_names (string[]) The names of objects to join 
	#' @param field_names (string[]) One or more strings and REGEX patterns used to define the field names to use for possible joins: matching is either REGEX or identity, and support for aliasing via \code{"primary_col==alias_col"} (note the quotes) is supported.
	#' @param env The environment where source objects are found; this is also the environment of the assigned output.   
	#' @param clean (logical) \code{TRUE} indicates that the object should be removed from \code{env} before creating the output
	#' 
	#' @return If a 'DBOE' dsn environment is passed to \code{env}, a local environment is used to contain the output of special code that transforms \code{env$metamap} into environment objects that can be used for creating the map: this should be stored or piped into \code{\link{join.reduce}}; otherwise, a \code{\link[data.table]{data.table}} object used join datasets via \code{\link{join.reduce}}
	#'  
	
	require(magrittr);
	require(foreach);
	require(iterators);
	require(book.of.utilities, include.only = c("%::%", "%?%", "%??%"));
	
	# :: Argument Handling ----
	# map_name
	map_name <- {
		x = substitute(map_name) %>% as.character()
		if (rlang::has_length(x, 1)){ x } else { x[-1] %>% .[1] }
	}
	
	# field_names
	field_names <- purrr::map_chr(field_names, ~{
		if (any(stri_detect_fixed(deparse(.x), "=="))){
			stringi::stri_replace_all_fixed(.x, " ", "", vectorize_all = FALSE)
		} else { rlang::inject(substitute(!!.x) %>% as.character()) }
	}) %>% unlist() 
	
	# env: Special case of DBOE dsn environment: virtual objects are created for the purpose of creating the map
	.pipe_env <- FALSE;
	
	if (rlang::env_has(env, "metamap")){
		.pipe_env <- TRUE;
		.db <- env$metamap$database[1];
		
		env <- env$metamap %>% 
			split(by = c("schema_name", "tbl_name")) %>% 
			map(~.x[, rep.int(1, .N) %>% enlist(col_name), by = .(schema_name, tbl_name)]) %>% 
			list2env(envir = new.env() %>% setattr("DBOE", TRUE))
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
					._2 = keep(names(.this), ~.x %ilike% .fn)
					._3 = if (any(map_lgl(names(.this), ~any(stringi::stri_detect_fixed(str = .fn, pattern = "==")) & .fn %like% .x))){ .fn } else { NULL }
					
					c(._1, ._2, ._3)
				}) %>% unlist() %>% unique()
			)
		}) %>% {
			.[!is.na(field_names)
			][, c(list(field_names = field_names), book.of.features::xform.basis_vector(.SD$obj_name))
			][, purrr::map(.SD, max), by = field_names
			][, unique(.SD)
			][, idx := order(.SD[, !"field_names"] %>% apply(1, sum))
			] %T>% data.table::setkey(idx) 
		}
	}
	
	assign(map_name, data.table::setattr(.output, "env", env), envir = env)
	if (.pipe_env){ return(env) }
}
# debug(join.mapper)
# undebug(join.mapper)
# inspect <- join.mapper("test_map", obj_names = "(fact|dim).+(risk)", field_names = "desc|sur|id", env = dbms.monitor$AMCS_DW_IC, clean = TRUE)
# inspect$test_map
#
join.reduce <- function(jmap, x.names, i.names, filters = TRUE, dt.key, env, clean = FALSE, .debug = FALSE){
	#' Reduce Multiple Datasets
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
	
	require(magrittr);
	
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
		._i = ...names()[(._j == 1) & (...names() %ilike% i.names)];
		if (length(._i) == 0){ return(NULL) };
		
		._x = ...names()[(._j == 1) & (...names() %ilike% x.names)];
		._on = ._j[[1]];
		
		list(x = c(._x), i = c(._i), on = c(._on)) %>% expand.grid()
	}
	
	join.func = function(cur, nxt){ sprintf(
		fmt = "%s[%s, on = c(%s), allow.cartesian = TRUE]"
		, cur
		, nxt[1]
		, paste(nxt[2] %>% unlist() %>% sprintf(fmt = "'%s'"), collapse = ", ")
	) 
	}
	
	.alpha = { purrr::pmap(jmap, xion.func) %>% 
			purrr::compact() %>% 
			data.table::rbindlist() %>%
			.[!(on %ilike% "source"), .(on = list(I(on))), by = .(x, i)]
	}
	
	.beta = { .alpha[
		, list(rlang::new_box(.SD[, .(i, on)] %>% apply(1, I))), by = x
	][, purrr::imap(set_names(V1, x), ~{
		.this = .y; .that = .x; 
		purrr::reduce(.x = .that, .f = join.func, .init = .this)
	}) ]; 
	}
	
	if (.debug){ print(list(alpha = .alpha, beta = .beta)); }
	
	# :: Output ----
	.output = purrr::map(.beta, ~{
		rfilter = filters[[1]]
		
		cfilter = filters[[2]]
		
		.out = eval(str2lang(.x), envir = attr(jmap, "env"))
		
		.out %<>% {  .[eval(rfilter), eval(cfilter)][, mget(discard(ls(), ~.x %ilike% "strptime|^i[.]|modifi|create|[_]sur|database|(schema|tbl)_name"))] %>% 
				unique() %>% 
				setnames("full_dt", "event_date", skip_absent = TRUE)
		}
		
		if (rlang::is_empty(dt.key)){ .out } else { data.table::setkeyv(.out, dt.key) %>% data.table::setcolorder() }
		
	}) %>% set_names(stringi::stri_replace_first_regex(names(.), "(dim|fact)[_]", ""))
	
	if (clean){ rm(list = names(.output), envir = env) }
	
	list2env(.output, envir = env)
}
# debug(join.reduce)
# undebug(join.reduce)
# inspect$test_map[, mget(discard(ls(), ~.x %ilike% "old|stage|temp"))][, !"strptime"] %>%
#   setattr("env", attr(inspect$test_map, "env")) %>%
#   join.reduce(x.names = "fact", i.names = "dim", env = { inspect$AMCS_DW_IC <- new.env(); inspect$AMCS_DW_IC }, clean = TRUE)

# inspect$AMCS_DW_IC$dbo.pat_risk_scores_ic
