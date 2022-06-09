ohdsi_project <- function(path) {
  # ensure path exists
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  
  # create file structure  --------------------
  #elaborate on file
  fstr <- c(
    paste0("input/", 
           c(paste0("cohorts/", c("meta", "specification")), 
             paste0("characterization/", c("meta", "specification")), 
             paste0("estimation/" , c("meta", "specification")), 
             paste0("prediction/" , c("meta", "specification"))
           )
    ),
    paste0("output/", 
           c(paste0("cohorts/", c("results")), 
             paste0("diagnostics/", c("results")), 
             paste0("characterization/", c("results")), 
             paste0("estimation/", c("results")), 
             paste0("prediction/", c("results"))
           )
    )
  )
  ff <- file.path(path, fstr)
  purrr::walk(ff, ~dir.create(path = .x, recursive = TRUE, showWarnings = FALSE))
  
  
  
  #create .gitignore -------
  git_ignores <-
    c(
      '.Rhistory',
      '.Rapp.history',
      '.RData',
      '.Ruserdata',
      '.Rproj.user/',
      '.Renviron',
      'config.yml'
    )
  writeLines(paste(git_ignores, sep = '\n'), con = file.path(path, ".gitignore"))
  
  #create config file --------------
  #init_study(path)
  
  #create targets file --------------------
  targetsPath <- system.file("targets/ohdsiScript.R", package = "ohdsitargets")
  targetsFile <- file(targetsPath)
  writeLines(readr::read_lines(targetsFile), con = file.path(path, "_targets.R"))
  
}

#' Create example ohdsitargets project
#' 
#' 
#' @param path Folder where the new project should be created
#'
#' @export
create_ohdsitargets_project <- function(path) {
  path <- path.expand(path)
  usethis::create_project(path)
  from <- list.files(system.file("project_template", package = "ohdsitargets", mustWork = TRUE),
                      full.names = TRUE, recursive = FALSE, include.dirs = TRUE)
  invisible(file.copy(from = from, to = path, recursive = TRUE, copy.mode = FALSE))
}
