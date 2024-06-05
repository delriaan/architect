#' Definition Blueprint (EXPERIMENTAL)
#'
#' Class \code{blueprint} is set as an attribute of the output from each successfully completed invocation of \code{\link{define}}().  Blueprints execute before the \code{...} arguments and are appended with each call.  The idea is to be able to recreate a series of operations by using the \code{blueprint} as a starting point with a compatible dataset (i.e., contains enough features to satisfy the blueprint).
#'
#' @docType class
#' @name blueprint
#' @slot schema A list of \code{\link{define}} operation expressions
#' @export
blueprint <- setClass(Class = "blueprint", package = "architect", slots = c(schema = "list"));

#
define <- function(data = NULL, ..., keep.rownames = TRUE, blueprint = NULL){
#' Define a Data Transformation Schematic
#'
#' \code{define()} allows one to operate on data using one or more formula-based definitions. Data transformation and selection can be achieved with formulas or using standard \code{\link[data.table]{data.table}} syntax in a procedural manner with a single function call. Each operation operates on the previous result making \code{define()} a pipeline of operations in a compact framework.
#'
#' @details
#' \itemize{
#' \item{If \code{data} is a \code{\link[smart.data]{smart.data}} object, the taxonomical references to field names can be accessed by using the \code{use()} syntax in the right-hand side of the formula (e.g., \code{~use(term1, term2) + otherTerm1 + ...}).}
#' \item{When using formulas for \code{...} on an empty \code{\link[data.table]{data.table}} object, set the argument \code{data} to \code{NULL}; otherwise, you will get an error.}
#' }
#'
#' @param data The input data (see 'Details')
#' @param ... (\code{\link[rlang]{dots_list}}): Formulas for which the left-hand side (LHS) is an expression containing the operation, and the right-hand side (RHS) contains column names that form a grouping set for the operation (i.e., \code{<expression> ~ col_1 + col_2 + ...}):
#' \itemize{
#' \item{If the form \code{<LHS>~ .} is given, the LHS executes using all columns as the grouping set}
#' \item{If the form \code{<LHS>~ 1} is given, the LHS executes without grouping}
#' \item{If no LHS is given, the operation defaults to selection based on the RHS}
#' }
#' @param keep.rownames See \code{\link[data.table]{data.table}}
#' @param blueprint (EXPERIMENTAL) See \code{\link{blueprint}}.
#'
#' @return The data modified
#'
#' @examples
#' library(smart.data)
#' smart.start();
#'
#' taxonomy_list <- list(
#' 	identifier = taxonomy(
#' 		term = "identifier"
#' 		, desc = "Identifier"
#' 		, fields = c("rn"))
#' 	, category = taxonomy(
#' 		term = "category"
#' 		, desc = "Category"
#' 		, fields = c("cyl", "gear", "carb")
#' 		)
#' );
#'
#' smart_mt <- smart.data$
#' 	new(as.data.table(mtcars[1:10, ], keep.rownames = TRUE))$
#' 	taxonomy.rule(!!!taxonomy_list)$
#' 	enforce.rules(for_usage)$
#' 	cache_mgr(action = upd);
#'
#' define(
#' 	smart_mt
#' 	, list(j = 1, mpg) ~vs + am + use(identifier, category)
#' 	, ~j + mpg
#' 	)[];
#'
#' define(smart_mt, ~vs + am + use(identifier, category))[];
#'
#' define(smart_mt, x = sum(am^2) ~ use(identifier, category))[];
#' define()[];
#' define(x = 1:10, y = x * 3)[];
#' define(x = 1:10, y = x * 3, z = x*y)[];
#' define(NULL, x = 1:10, y = x * 3, z = x*y, ~x + z)[];
#' define(data.table(), x = 1:10, y = x * 3, list(z = 10) ~ x)[];
#'
#' # Predefined operations:
#' predef_data <- define(smart_mt, x = sum(am^2) ~ use(identifier, category));
#'
#' redef_data <- define(
#'		smart_mt
#'		# Normally, listing 'x' would throw an error as it would not exist in the data;
#'		# however, since a blueprint is provided from a previous definition, the 'x' variable is
#'		# created within scope before the additional operations execute:
#'		, list(j = 1, mpg, x) ~vs + am + use(identifier, category)
#'		, blueprint = predef_data
#'		);
#'
#' redef_data;
#'
#' attr(redef_data, "blueprint");
#'
#' identical(redef_data, define(smart_mt, blueprint = redef_data));
#'
#' @export

	force(data);
  .smartData <- fun_expr <- by_args <- NULL;

  .smartData_is_installed <- installed.packages() |>
	  	rownames() |>
	  	grep(pattern = "smart.data");

	if (.smartData_is_installed){
		if (smart.data::is.smart(data)){
			.smartData <- data$clone(deep = TRUE);
			data <- data.table::copy(.smartData$data);
		} else{
			data <- data.table::as.data.table(data, keep.rownames = keep.rownames);
		}
	} else {
		data <- data.table::as.data.table(data, keep.rownames = keep.rownames);
	}

  # `.terms_check` is a helper function that checks for the use of the `use()` function in the RHS of the formula:
  .terms_check <- \(expr){
  	if (!grepl("~", rlang::as_label(expr))){
  		return(expr);
  	}
		# browser()
  	.orig_terms <- spsUtil::quiet(as.formula(expr) |> terms(data = data) |> attr("term.labels"));

		# If any term is a `use()` term, then we need to get the taxonomy
		# for each term and replace the term with the taxonomy field names:
		if (any(grepl("^use[(]", .orig_terms)) & !rlang::is_empty(.smartData)){
			# First, capture the terms that are not `use()` terms:
			.terms <- grep("^use[(]", .orig_terms, value = TRUE, invert = TRUE);

			# Then, capture the terms that are `use()` terms:
			.taxonomy <- grep("^use[(]", .orig_terms, value = TRUE) |>
					rlang::parse_expr() |>
					as.list() |>
					(`[`)(-1) |>
					as.character();

			# Then convert the `use()` terms to the taxonomy field names:
			.taxonomy <- rlang::expr(with(
					.smartData$smart.rules$for_usage
					, mget(!!.taxonomy) |>
							purrr::map(\(tax) tax@fields) |>
							purrr::compact() |>
							purrr::reduce(c)
					));

			# Update the formula with the new terms:
			expr <- as.formula(expr);
			rlang::f_rhs(expr) <- str2lang(paste(c(.terms, eval(.taxonomy)), collapse = " + "));
		}

  	return(expr);
  }

  # `.func` is the function that is called for each operation:
  .func <- \(x, y){
    if (!rlang::is_formula(x)){
      fun_expr <- x;
      by_args <- character();
    } else {
	    f <- eval(unlist(x, recursive = FALSE)) |>
	    	data.table::setattr(".Environment", as.environment(data));

      fun_expr <- rlang::f_lhs(f = f);

      by_args <- stats::terms(f, data = data) |> attr("term.labels");

      if (!all(by_args %in% names(data))){ by_args <- character() }
    }

    .op <- if (!rlang::is_empty(fun_expr)){
	    	# Operation Branch:
	    	.out_expr <- if (y != ""){
		      # Assignment branch
	          rlang::expr(data[, data.table::`:=`(!!y, !!fun_expr)])
	        } else {
		      # Operation-only branch
	          rlang::expr(data[, !!fun_expr])
	        }

	    	# data.table grouping "by":
	      if (!(rlang::is_empty(by_args) | identical(character(), by_args))){
	      	.out_expr$by <- by_args
	      }

	    	.out_expr
	    } else if (!rlang::is_empty(by_args)){
	    	# Selection Branch:
	      rlang::expr(data[, c(!!!by_args)])
	    } else {
	    	# Identity:
	    	rlang::expr(data)
	    }

    data <<- eval(.op);
  };

  # `.ops` contains the operations to use to define the data:
  if (!rlang::is_empty(blueprint)){
  	if (methods::is(attr(blueprint, "blueprint"), "blueprint")){
  		.ops <- rlang::exprs(
  			!!!attr(blueprint, "blueprint")@schema
  			, !!!purrr::map(rlang::enexprs(...), .terms_check)
  			)
  	} else {
  		warning("Not a `blueprint`: ignoring ...");
  		.ops <- rlang::enexprs(...) |> purrr::map(.terms_check);
  	}
  } else {
  	.ops <- rlang::enexprs(...) |> purrr::map(.terms_check);
  }

  # `.ops` is iterated over using `.func`.  The call to `spsUtil::quiet()` is
  # used to suppress all messages outside of errors:
  spsUtil::quiet(purrr::iwalk(.ops, .func));

  if (!rlang::is_empty(data)){
		data <- magrittr::set_attr(data, "blueprint", new("blueprint", schema = .ops))
  }

  return(invisible(data));
}
