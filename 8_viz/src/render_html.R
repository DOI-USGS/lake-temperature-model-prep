render_html <- function(filename_md, out_ind) {

  out_location <- scipiper::as_data_file(out_ind)
  outfile <- rmarkdown::render(input = filename_md,
                               output_file = basename(out_location),
                               output_dir = dirname(out_location),
                               output_format = "html_document",
                               quiet=TRUE, run_pandoc = TRUE)
  gd_put(out_ind)

}
