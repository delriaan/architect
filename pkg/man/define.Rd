% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/define.R
\name{define}
\alias{define}
\title{Define a Data Transformation Schematic}
\usage{
define(
  data = NULL,
  ...,
  keep.rownames = TRUE,
  blueprint = NULL,
  progress = FALSE
)
}
\arguments{
\item{data}{The input data (see 'Details')}

\item{...}{(\code{\link[rlang]{dots_list}}): Formulas for which the left-hand side (LHS) is an expression containing the operation, and the right-hand side (RHS) contains column names that form a grouping set for the operation (i.e., \code{<expression> ~ col_1 + col_2 + ...}):
\itemize{
\item{If the form \code{<LHS>~ .} is given, the LHS executes using all columns as the grouping set}
\item{If the form \code{<LHS>~ 1} is given, the LHS executes without grouping}
\item{If no LHS is given, the operation defaults to selection based on the RHS}
}}

\item{keep.rownames}{See \code{\link[data.table]{data.table}}}

\item{blueprint}{(EXPERIMENTAL) See \code{\link{blueprint}}. Blueprints are always evaluated first when provided: this makes it easy to "branch" off of a base dataset.}

\item{progress}{(logical | FALSE) Should a progress bar be shown during execution (passed to \code{\link[purrr]{map}})?}
}
\value{
The data modified
}
\description{
\code{define()} allows one to operate on data using one or more formula-based definitions. Data transformation and selection can be achieved with formulas or using standard \code{\link[data.table]{data.table}} syntax in a procedural manner with a single function call. Each operation operates on the previous result making \code{define()} a pipeline of operations in a compact framework.
}
\details{
\itemize{
\item{If \code{data} is a \code{\link[smart.data]{smart.data}} object, the taxonomical references to field names can be accessed by using the \code{use()} syntax in the right-hand side of the formula (e.g., \code{~use(term1, term2) + otherTerm1 + ...}).}
\item{When using formulas for \code{...} on an empty \code{\link[data.table]{data.table}} object, set the argument \code{data} to \code{NULL}; otherwise, you will get an error.}
}
}
\examples{
if (require(smart.data)){
	library(smart.data)
	smart.start();

	taxonomy_list <- list(
		identifier = taxonomy(
			term = "identifier"
			, desc = "Identifier"
			, fields = c("rn"))
		, category = taxonomy(
			term = "category"
			, desc = "Category"
			, fields = c("cyl", "gear", "carb")
			)
		);

	smart_mt <- smart.data$
		new(as.data.table(mtcars[1:10, ], keep.rownames = TRUE))$
		taxonomy.rule(!!!taxonomy_list)$
		enforce.rules(for_usage)$
		cache_mgr(action = upd);

	print(test_obj <- define(
		smart_mt
		, list(j = 1, mpg) ~vs + am + use(identifier, category)
		, ~j + mpg
		, keep.rownames = FALSE
		))

	print(test_obj <- define(smart_mt, ~vs + am + use(identifier, category)));

	print(define(
		test_obj
		, `:=`(x = sum(am^2), y = 10) ~ use(identifier, category)
		));

	print(test_obj)

	rm(test_obj)
}

define()[];
define(x = 1:10, y = x * 3)[];
define(x = 1:10, y = x * 3, z = x*y)[];
define(NULL, x = 1:10, y = x * 3, z = x*y, ~x + z)[];
define(data.table(), x = 1:10, y = x * 3, list(z = 10) ~ x)[];

# Predefined operations:
predef_data <- blueprint(schema = rlang::exprs(x = sum(am^2) ~ use(identifier, category)));

redef_data <- define(
	smart_mt
	# Normally, listing 'x' would throw an error as it would not exist in the data;
	# however, since a blueprint is provided from a previous definition, the 'x' variable is
	# created within scope before the additional operations execute:
	, list(j = 1, mpg, x) ~vs + am + use(identifier, category)
	, blueprint = predef_data
	);

redef_data;

attr(redef_data, "blueprint");

identical(redef_data, define(smart_mt, blueprint = attr(redef_data, "blueprint")))

if ("smart.data" \%in\% loadedNamespaces()){
	detach("package:smart.data", unload = TRUE)
}

rm(redef_data, smart_mt, predef_data, taxonomy_list)

}
