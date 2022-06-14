
# This is an example _targets.R file. Every {targets} pipeline needs one.
# Use tar_script() to create _targets.R and tar_edit()bto open it again for editing.
# Then, run tar_make() to run the pipeline and tar_read(summary) to view the results.

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

# tar_make_clustermq() configuration (okay to leave alone):
# options(clustermq.scheduler = "multicore")

# tar_make_future() configuration (okay to leave alone):
# Install packages {{future}}, {{future.callr}}, and {{future.batchtools}} to allow use_targets() to configure tar_make_future() options.

# Load the R scripts with your custom functions:
# for (file in list.files("R", full.names = TRUE)) source(file)

# Set the database
Sys.setenv(R_CONFIG_ACTIVE = "eunomia")

cohortsToCreate <- readr::read_csv("input/cohorts/meta/CohortsToCreate.csv", show_col_types = F) 

list(
  tar_cohort_tables("blah"),
  tar_cohorts(cohortsToCreate = cohortsToCreate, cohortTableRef = cohort_table)
)

