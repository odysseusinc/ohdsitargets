#function to build project Cohort Tables
projectCohortTables <- function(projectName,
                                connectionDetails,
                                cohortDatabaseSchema) {


  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))

  tabNames <- CohortGenerator::getCohortTableNames() %>%
    purrr::map(~paste(projectName, .x, sep = "_"))
  CohortGenerator::createCohortTables(connection = conn,
                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                      cohortTableNames = tabNames,
                                      incremental = TRUE)
  return(tabNames)
}


create_cohort_table <- function(name,   
                                connectionDetails = config::get("connectionDetails"),
                                cohortDatabaseSchema = config::get("resultsDatabaseSchema"),
                                cohortTableName = config::get("cohortTableName")) {
  # name <- targets::tar_deparse_language(substitute(name))
  # browser()
  cohortTableNames <- CohortGenerator::getCohortTableNames(name) 
  suppressWarnings(
    CohortGenerator::createCohortTables(connectionDetails,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortTableNames = cohortTableNames,
                                        incremental = FALSE)
  )
  list(structure(list(cohortDatabaseSchema = cohortDatabaseSchema, cohortTableNames = cohortTableNames, createdTimestamp = Sys.time()), class = "cohortTableRef"))
}

#' Create cohort tables
#'
#' Returns a target that references the cohort table in the database
#'
#' @param name Symbol, the name of the cohort table target
#' @param connectionDetails Database connectionDetails object created by createConnectionDetails()
#' @param cohortDatabaseSchema character string with the schema where the cohort tables can be created
#' @param cohortTableNames A list of cohort table names created by cohortTableNames
#'
#' @return a target - list that contains the data needed to query the cohort table and the time it was created
#' @export
tar_cohort_tables <- function(name,
                              connectionDetails = config::get("connectionDetails"),
                              cohortDatabaseSchema = config::get("resultsDatabaseSchema"),
                              cohortTableName = config::get("cohortTableName")) { 

  stopifnot(is.character(name))
  print("running tar_cohort_tables")

  # Return an R object that acts as a local reference the cohort table in the database
  # Importantly, if this function ever reruns the output will be different from the previous output since the timestamp will be different
  # name_as_string <- substitute(deparse(name))
  targets::tar_target_raw("cohort_table", substitute(ohdsitargets:::create_cohort_table(name, connectionDetails, cohortDatabaseSchema, cohortTableName)))
}

#' Generate one cohort
#'
#' @param connectionDetails
#' @param cdmDatabaseSchema
#' @param cohortTableRef A cohort table reference
#' @param cohortId
#' @param cohortName
#' @param cohortFile
#'
#' @return
#' @export
generateCohort <- function(connectionDetails,
                           cdmDatabaseSchema,
                           cohortTableRef,
                           cohortId,
                           cohortName,
                           cohortFile) {

  cohortJson <- readr::read_file(cohortFile)
  cohortSql <- CirceR::buildCohortQuery(CirceR::cohortExpressionFromJson(cohortJson),
                                        CirceR::createGenerateOptions(generateStats = TRUE))

  # print(str(cohortTableRef))
  # for now use this function to generate a single cohort
  r <- suppressWarnings(CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                           cohortDatabaseSchema = cohortTableRef$cohortDatabaseSchema,
                                                           cohortTableNames = cohortTableRef$cohortTableNames,
                                                           cohortDefinitionSet = data.frame(cohortId = cohortId,
                                                                                            cohortName = cohortName,
                                                                                            sql = cohortSql, stringsAsFactors = F)))

  cnt <- CohortGenerator::getCohortCounts(connectionDetails,
                                          cohortDatabaseSchema = cohortTableRef$cohortDatabaseSchema,
                                          cohortTable = cohortTableRef$cohortTableNames$cohortTable,
                                          cohortIds = cohortId)

  if(nrow(cnt) == 0)  {
    r$cohortEntries <- 0; r$chortSubjects <- 0
  } else {
    r <- dplyr::left_join(r, cnt, by = "cohortId")
  }
  r
}

#' Create references to generated cohorts
#'
#' Creates targets for each cohort in the project
#'
#' @param name Symbol, base name for the collection of cohort targets
#' @param cohortsToCreate A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath
#' @param cohortTable A reference to the cohort table created by `tar_cohort_tables`
#'
#' @return One target for each cohort file and for each generated cohort with names cohort_{id}
#' @export
tar_cohorts <- function(cohortsToCreate, cohortTableRef, connectionDetails = config::get("connectionDetails"), cdmDatabaseSchema = config::get("cdmDatabaseSchema")) {
  # name <- targets::tar_deparse_language(substitute(name))
  # stopifnot(is.list(cohortTableRef), is.data.frame(cohortsToCreate))

  
  list(
    # tar_target_raw("cohortTable", substitute(createCohortTables(connectionDetails, cohortTableRef$cohortDatabaseSchema, cohortTableRef$cohortTableNames))),
    tarchetypes::tar_map(values = cohortsToCreate, names = "cohortId",
            tar_target_raw("cohortFile", quote(here::here(filePath)), format = "file"),
            tar_target_raw("generatedCohort", substitute(generateCohort(
              connectionDetails,
              cdmDatabaseSchema,
              cohortTableRef = cohortTableRef,
              cohortId = cohortId,
              cohortName = cohortName,
              cohortFile = cohortFile)))
    )
  )
}
