
suppressPackageStartupMessages(library(Eunomia))
connectionDetails <- getEunomiaConnectionDetails()

test_that("create cohort tables", {
  cohortTableRef <- create_cohort_tables("cohort", connectionDetails, "main")
  suppressMessages( con <- DatabaseConnector::connect(connectionDetails) )
  on.exit(DatabaseConnector::disconnect(con))
  df <- dbGetQuery(con, "select * from main.cohort")
  expect_equal(names(df), c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"))
})

test_that("single cohort generation works", {
  cohortTableRef <- create_cohort_tables("cohort", connectionDetails, "main")
  cohortFile <- system.file(file.path("inst", "project_template", "input", "cohorts", "specification", "celecoxib.json"),
                            package = "ohdsitargets", mustWork = TRUE)
  
  generatedCohortRef <- generate_cohort(connectionDetails, 
                                        cdmDatabaseSchema = "main", 
                                        cohortTableRef = cohortTableRef, 
                                        cohortFile = cohortFile,
                                        cohortId = 1)
  
  suppressMessages( con <- DatabaseConnector::connect(connectionDetails) )
  on.exit(DatabaseConnector::disconnect(con))
  cohort_entries <- dbGetQuery(con, "select count(*) as n from main.cohort where cohort_definition_id = 1")$n
  
  expect_equal(cohort_entries, generatedCohortRef$cohort_entries)
})

