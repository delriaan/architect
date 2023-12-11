library(smart.data)
library(purrr)
library(data.table)
library(stringi)
library(book.of.utilities)
library(book.of.workflow)

# Basic Usage: mtcars with row names pre-pended >>>
define(mtcars)
# <<<

# smart.data input >>>
smart.data::smart.start()
smart_mt <- smart.data::smart.data$new(as.data.table(mtcars, keep.rownames = TRUE))$taxonomy.rule();

define(smart_mt, list(j = 1, mpg) ~vs + am + use(identifier, category), ~j + mpg)
define(smart_mt, ~vs + am + use(identifier, category))[]
define(smart_mt, x = sum(am^2) ~ use(identifier, category))[]
# <<<

# No terms variant: output should be equivalent >>>
# debug(define)
define(smart_mt, mean(disp))
define(smart_mt, mean(disp) ~ 1)
# undebug(define)
# <<<

# Define using 'smart.data' input, sending output to "smart cache" and then capturing output >>>
if (hasName(.GlobalEnv, "inspect")){ rm(inspect)}

inspect <- define(
	smart_mt
	, x = sum(am^2) ~ use(identifier, category)
	, smart_key = cyl
	, smart.data::smart.data$new(.SD, "defined")$taxonomy.rule()$cache_mgr(action = upd)
	)

get.smart("defined")$use(category, identifier, retain = drat)
# <<<

# pkgdown::build_site(pkg = "pkg", override = list(destination = "../docs"))
