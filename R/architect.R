#' Architect
#'
#' Intelligent Data Architecture
#'
#' @description
#' \code{architect} is an \code{\link{R6Class}} that provides a framework for intelligent data architecture.  It features a \emph{taxonomical-formulaic} definition framework and a \emph{predicate-logical} transformation framework
#'
#' @export
architect <- { R6::R6Class(
	classname = "architect"
	# _____ PUBLIC METHODS _____
	, public = { list(
		# _____ PUBLIC CLASS MEMBERS _____
		# _____ PUBLIC CLASS METHODS _____
		# NEW() ====
		#' @description
		#' Initialize the architectural framework
		initialize = function(){
			# ::  CLASS MEMBERS INITIALIZATION
			
			invisible(self);
			}
		)}
	# _____ PUBLIC ACTIVE BINDINGS _____
	, active = { list(
		# @field config This is an active binding that returns the configuration used to instantiate the class.
		# config = function(...){ copy(private$.params$config) %>% setkey(contexts) %T>% setattr("events.ascending", private$.params$events.ascending)}
		)}
	# _____ PRIVATE _____
	, private = { list(
		# _____ PRIVATE CLASS MEMBERS _____
		.params = { list(config = NULL, events.ascending	= NULL)}
		# _____ PRIVATE METHODS _____
		)}
)}
