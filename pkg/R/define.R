define <- function(data = NULL, ..., keep.rownames = TRUE, blueprint = NULL, progress = FALSE){
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
	#' @param blueprint (EXPERIMENTAL) See \code{\link{blueprint}}. Blueprints are always evaluated first when provided: this makes it easy to "branch" off of a base dataset.
	#' @param progress (logical | FALSE) Should a progress bar be shown during execution (passed to \code{\link[purrr]{map}})?
	#' @return The data modified
	#'
	#' @examples
	#' if (require(smart.data)){
	#' 	library(smart.data)
	#' 	smart.start();
	#'
	#' 	taxonomy_list <- list(
	#' 		identifier = taxonomy(
	#' 			term = "identifier"
	#' 			, desc = "Identifier"
	#' 			, fields = c("rn"))
	#' 		, category = taxonomy(
	#' 			term = "category"
	#' 			, desc = "Category"
	#' 			, fields = c("cyl", "gear", "carb")
	#' 			)
	#' 		);
	#'
	#' 	smart_mt <- smart.data$
	#' 		new(as.data.table(mtcars[1:10, ], keep.rownames = TRUE))$
	#' 		taxonomy.rule(!!!taxonomy_list)$
	#' 		enforce.rules(for_usage)$
	#' 		cache_mgr(action = upd);
	#'
	#' 	print(test_obj <- define(
	#' 		smart_mt
	#' 		, list(j = 1, mpg) ~vs + am + use(identifier, category)
	#' 		, ~j + mpg
	#' 		, keep.rownames = FALSE
	#' 		))
	#'
	#' 	print(test_obj <- define(smart_mt, ~vs + am + use(identifier, category)));
	#'
	#' 	print(define(
	#' 		test_obj
	#' 		, `:=`(x = sum(am^2), y = 10) ~ use(identifier, category)
	#' 		));
	#'
	#' 	print(test_obj)
	#'
	#' 	rm(test_obj)
	#' }
	#'
	#' define()[];
	#' define(x = 1:10, y = x * 3)[];
	#' define(x = 1:10, y = x * 3, z = x*y)[];
	#' define(NULL, x = 1:10, y = x * 3, z = x*y, ~x + z)[];
	#' define(data.table(), x = 1:10, y = x * 3, list(z = 10) ~ x)[];
	#'
	#' # Predefined operations:
	#' predef_data <- blueprint(schema = rlang::exprs(x = sum(am^2) ~ use(identifier, category)));
	#'
	#' redef_data <- define(
	#' 	smart_mt
	#' 	# Normally, listing 'x' would throw an error as it would not exist in the data;
	#' 	# however, since a blueprint is provided from a previous definition, the 'x' variable is
	#' 	# created within scope before the additional operations execute:
	#' 	, list(j = 1, mpg, x) ~vs + am + use(identifier, category)
	#' 	, blueprint = predef_data
	#' 	);
	#'
	#' redef_data;
	#'
	#' attr(redef_data, "blueprint");
	#'
	#' identical(redef_data, define(smart_mt, blueprint = attr(redef_data, "blueprint")))
	#'
	#' if ("smart.data" %in% loadedNamespaces()){
	#' 	detach("package:smart.data", unload = TRUE)
	#' }
	#'
	#' rm(redef_data, smart_mt, predef_data, taxonomy_list)
	#'
	#' @export

	# :: Function Definitions: ----
  # `.terms_check`: a helper function that checks for the use of the 'smart.data' `use()` function in the RHS of the formula:
  .terms_check <- \(expr){
  	if (!grepl("~", rlang::as_label(expr))){
  		return(expr);
  	}

  	.orig_terms <- spsUtil::quiet(as.formula(expr) |> terms(data = data) |> attr("term.labels"));

		# 1. Capture the terms that are not `use()` terms:
		.terms <- grep("^use[(]", .orig_terms, value = TRUE, invert = TRUE);

		# If any term is a `use()` term, then we need to get the taxonomy
		# for each term and replace the term with the taxonomy field names:
		if (any(grepl("^use[(]", .orig_terms)) & !rlang::is_empty(.smartData)){
			# 2. Capture the terms that are `use()` terms:
			.taxonomy <- grep("^use[(]", .orig_terms, value = TRUE) |>
					rlang::parse_expr() |>
					as.list() |>
					(`[`)(-1) |>
					as.character();

			# 3. Convert the `use()` terms to the taxonomy field names:
			.taxonomy <- rlang::expr(with(
					.smartData$smart.rules$for_usage
					, mget(!!.taxonomy) |>
							purrr::map(\(tax) tax@fields) |>
							purrr::compact() |>
							purrr::reduce(c)
					)) |> eval();

			# 4. Update the formula with the combined terms:
			expr <- as.formula(expr);
			rlang::f_rhs(expr) <- to_rhs(!!!.terms, !!!.taxonomy)
		} else {
			# 4. Update the formula with the provided terms:
			expr <- as.formula(expr);
			rlang::f_rhs(expr) <- to_rhs(!!!.terms)
		}

  	return(expr);
  }

  # `.build`: the function that is called for each operation:
  .build <- \(x, y){
  	# x:
  	# y: Potentially a symbol under an assignment operation

    if (!rlang::is_formula(x)){
      fun_expr <- x;
      by_args <- character();
    } else {
	    f <- eval(unlist(x, recursive = FALSE)) |>
	    	data.table::setattr(".Environment", as.environment(data));

	    # j:
      fun_expr <- rlang::f_lhs(f = f);

      # by:
      by_args <- stats::terms(f, data = data) |> attr("term.labels");

      if (!all(by_args %in% names(data))){
      	by_args <- character()
      }
    }

  	# `operations`: The list of expressions to be evaluated in order of appearance:
    operations <- if (!rlang::is_empty(fun_expr)){
	    	# Operation Branch:
	    	.out_expr <- if (y != ""){
		      # Assignment branch
	    			if (":=" %in% sapply(rlang::exprs(!!!as.list(fun_expr)), as.character)){
	          	rlang::expr(data[, `:=`(!!y, !!fun_expr)])
	    			} else {
	    				# browser()
	    				rlang::expr(data[, !!rlang::sym(y) := !!fun_expr]);
	    			}
	        } else {
		      # Operation-only branch
	          rlang::expr(data[, !!fun_expr])
	        }

	    	# 'data.table' grouping "by":
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
		# Update `data` (note the `<<-` operator)
    data <<- spsUtil::quiet(eval(operations));
  };

	force(data);
  .smartData <- fun_expr <- by_args <- NULL;

  .smartData_is_installed <- installed.packages() |>
  	rownames() |>
  	grepl(pattern = "smart[.]data") |>
  	any();

  # :: Check for the presence of a 'smart.data' classed object: ----
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

  # :: `operations` contains the operations to use to define the data: ----
  # Blueprints are processed before user-supplied expressions.
  if (!rlang::is_empty(blueprint)){
  	if (methods::is(blueprint, "blueprint")){
  		operations <- rlang::exprs(
  			!!!blueprint@schema
  			, !!!purrr::map(rlang::enexprs(...), .terms_check)
  			)
  	} else {
  		warning("Not a `blueprint`: ignoring ...");
  		operations <- rlang::enexprs(...) |> purrr::map(.terms_check);
  	}
  } else {
  	operations <- rlang::enexprs(...) |> purrr::map(.terms_check);
  }

  # `operations` is iterated over using `.build`.  The call to `spsUtil::quiet()` is
  # used to suppress all messages outside of errors:
  # spsUtil::quiet(
		purrr::iwalk(operations, .build, .progress = progress)
	# );

  # :: Finalize and return: ----
  if (!rlang::is_empty(data)){
		data <- magrittr::set_attr(data, "blueprint", new("blueprint", schema = operations))
	  return(invisible(data));
  } else {
  	cli::cli_alert_danger("Object `data` is empty!")
  	return(data.table::data.table())
  }
}

to_rhs <- function(...){
	#' Convert to RHS Formula Terms
	#'
	#' \code{to_rhs} takes its input and creates an language object that can be included in the right-hand side of a formula object.
	#'
	#' @param ... \code{\link{rlang}{dots_list}} Strings or symbols to use
	#'
	#' @return A partial right-hand-side of a formula
	#'
	#' @examples
	#' res <- define(
	#' 	NULL
	#' 	, x = 1:10
	#' 	, y = x * 3
	#' 	, z = sum(y) ~ x
	#' 	, omega := 100
	#' 	)
	#' print(res)
	#' print(res <- define(res, ~!!to_rhs(z, omega)))
	#' attributes(res)$blueprint
	#' @export

	.terms <- rlang::enexprs(...) |> as.character()
	str2lang(paste(.terms, collapse = " + ") |>
		stringi::stri_replace_all_fixed("+ -", "-", vectorize_all = FALSE))
}

