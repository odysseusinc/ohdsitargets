
# Welcome to ohdsitargets!
# The current targets script is for the "Hello OHDSI" example 
# Every {ohdsitargets} pipeline needs a _targets.R file. You may
# use this to get acquainted with the package or edit to your own study

# To run the pipeline use targets::tar_make(). To review the pipeline run
# targets::tar_visnetwork. To look at results from the pipeline use
# targets::tar_read(<target>), where the bracketed item is replaced with 
# a suitable target in the ohdsitargets pipeline.

# For questions please review the ohdsitargets vignette (Under Construction) or
# navigate to the Discussion tab in the ohdsitargets github repository to view 
# start-up questions.


# Prepare Environment ----------------------

# Load packages required to define the pipeline:
library(ohdsitargets)
library(targets)
library(dplyr)

# Set target options:
tar_option_set(
  packages = c("tibble", "dplyr", "CirceR", "CohortGenerator", "here", "DatabaseConnector"), 
  format = "rds" # default storage format
  # Set other options as needed.
)



#set cohorts to track in the pipeline
cohortsToCreate <- readr::read_csv("input/cohorts/meta/CohortsToCreate.csv", show_col_types = F) 


# Run Targets -----------------------

list(
  tar_target(connectionDetails, config::get("connectionDetails")), 
  tar_database_diagnostics(connectionDetails = connectionDetails),
  tar_cohorts(cohortsToCreate = cohortsToCreate,
               connectionDetails = connectionDetails)
)
