# Read the source directory and name the paths of source files as titles
# The titles will show up in package documentation
pkg_desc <- desc::desc(file = "pkg/DESCRIPTION")
pkg_name <- pkg_desc$get_field("Package")
pkg_title <- pkg_desc$get_field("Title")
pkg_file <- "pkg/R/book.of.utilities.R"
toc_file <- "pkg/README.md"

library(magrittr)

#
# Generation ----
pkg_doc_prep <- dir("pkg/R", pattern = "R$", recursive = TRUE, full.names = TRUE) |>
	(\(x){
		i <- which(grepl("miscell|other", x));
		if (rlang::is_empty(i)){ x } else { c(x[-i], x[i]) }
	})() |>
	purrr::discard(\(x) grepl(pkg_name, x)) |>
	purrr::map(\(x){
		list(path = x, title = { stringi::stri_replace_all_regex(x, c("pkg/R/", "[.]R$", "[_]"), c("", "", " "), vectorise_all = FALSE) |>
				stringi::stri_trans_totitle() })
	})

#
pkg_template <- c("#' \\itemize{", "%s", "#' }") |> paste(collapse = "\n")
pkg_doc_content <- c()
toc_content <- c()

# Populate content
pkg_doc_prep |> purrr::iwalk(\(x, y){
	env <- new.env();

	source(x$path, local = env)

	pkg_doc_content <<- pkg_doc_content |>
		c(paste(
			glue::glue("#' @section Chapter {y} - {x$title}: ")
			, sprintf("#' \\item{\\code{\\link{?%s}}}", ls(env)) |>
					stringi::stri_replace_all_fixed("%", "\\%", vectorise_all = TRUE) |>
					paste(collapse = "\n") |>
					sprintf(fmt = pkg_template)
			, sep = "\n"
			)) |>
		unique()

		toc_content <<- toc_content |>
			c(paste(
				glue::glue("## Chapter {y} - {x$title}\n\n")
				, sprintf("- %s", ls(env)) |> paste(collapse = "\n")
				, sep = "\n"
				)) |>
			unique()
	})

#
pkg_doc_header <- { paste(
	glue::glue("#' @title {pkg_title}")
	, "#"
	, "#' @description"
	, "#' `book.of.utilities` seeks to facilitate execution of those repetitive, ad-hoc tasks often encountered during data processing."
	, "#'"
	, "#'"
	, glue::glue("#' The following functional chapters are covered in \\code{{{pkg_name}}}:\\cr")
	, sep = "\n"
	)}

#
toc_header <- { paste(
	glue::glue("# ![book](../image_resources/) {pkg_title}\n\n")
	, glue::glue("**{pkg_name}** seeks to facilitate execution of those repetitive, ad-hoc tasks often encountered during data processing.")
	, glue::glue("The following functional families are covered in `{pkg_name}`:")
	, sep = "\n"
	)}

#
pkg_doc_footer <- { paste(
	"#'"
	, "#' @importFrom magrittr %>% %T>% or %<>%"
	, "#' @importFrom purrr map map_lgl map_chr reduce modify modify_if modify_at"
	, "#' @importFrom rlang is_empty %||%"
	, "#' @importFrom data.table %ilike% %like% like := .N .SD"
	, "#' @importFrom foreach %do% %dopar%"
	, "#' @importFrom methods new"
	, "#' @importFrom stats sd"
	, "#' @importFrom utils object.size"
	, glue::glue("#' @name {pkg_name}")
	, "NULL"
	, sep = "\n"
	)}

#
# Output ----
unlink(pkg_file)
unlink(toc_file)
cat(pkg_doc_header, pkg_doc_content, pkg_doc_footer, sep = "\n#'\n", file = pkg_file)
cat(toc_header, toc_content, sep = "\n\n", file = toc_file, append = FALSE)

#
# Build Site ----
# usethis::use_pkgdown()
library(magrittr)
dir("../resources/R", pattern = "list", full.names = TRUE) |> source();

toc_html <- dir("pkg/R", pattern = "^[a-z].+R$") %>%
	purrr::discard(\(x) grepl("book", x)) %>%
	rlang::set_names(
		stringi::stri_split_fixed(., "_") |>
			sapply(\(i) stringi::stri_trans_totitle(i) |> paste(collapse = " ")) |>
			stringi::stri_replace_all_fixed(c(".r", "Z "), "", vectorize_all = FALSE) |>
			sprintf(fmt = "Chapter %2$s - %1$s", seq_along(.))
	) |>
	purrr::map(\(i){
		readLines(paste0("pkg/R/", i)) |>
			stringi::stri_extract_all_regex(pattern = "(^[[:punct:]a-z_`%].+ (?=(<-)))", omit_no_match = TRUE) |>
			sapply(\(k){
				stringi::stri_split_fixed(k, "@family ", simplify = TRUE, omit_empty = TRUE) |> as.vector()
			}) |>
			purrr::compact() |>
			unlist() |>
			trimws() |>
			sort() %>%
			slider::slide(
				.after = 2
				, .step = 3
				, .f = \(k) sapply(k, \(i) htmltools::HTML(i) |> as.character())
				, .complete = length(.) > 2
			) |>
			purrr::compact() %>%
			rlang::set_names(rep.int("", length(.)))
	}) |>
	list2html(.ordered = FALSE) |>
	as.character();
	# cat(file = "pkg/toc.html", sep = "\n")

c("README", "NEWS") |>
	purrr::walk(\(doc){
		rmarkdown::render(
			input = glue::glue("pkg/{doc}.rmd")
			, clean = FALSE
			, knit_root_dir = getwd()
			, intermediates_dir = getwd()
			, envir = globalenv()
			);
		fs::file_copy(
			glue::glue("{doc}.knit.md")
			, glue::glue("pkg/{doc}.md")
			, overwrite = TRUE
			);
		if (fs::file_exists(glue::glue("pkg/{doc}.md"))){
			unlink(glue::glue("{doc}.knit.md"))
		}
	}, .progress = TRUE);


pkgdown::build_site(
	pkg = "pkg"
	, lazy = TRUE
	, override = list(destination = "../docs")
	)
