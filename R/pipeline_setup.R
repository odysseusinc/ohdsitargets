#pipeline setup

#' Create execution settings
#'
#' Creates execution settings object that defines inputs for database connection
#'
#' @param active_database the database we want to implement the pipeline
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param tempEmulationSchema the temporary table schema used in certain databases
#' @param vocabularyDatabaseSchema (character) Schema where the vocabulary tables lives in the database
#' @param resultsDatabaseSchema (character) Schema where the cohorts and results live
#' @param cohortTableName (character) naming convention for cohort tables
#' @param databaseId (character) identify which database is being used
#' @param studyName (character) provide a name of the study
#' @return returns executionSettings object 
#' @export
create_execution_settings <- function(active_database,
                                      connectionDetails = config::get("connectionDetails"), 
                                      cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
                                      resultsDatabaseSchema = config::get("resultsDatabaseSchema"),
                                      cohortTableName = config::get("cohortTableName"),
                                      databaseId = config::get("databaseName"),
                                      studyName = config::get("studyName")) {
  
  
  Sys.setenv(R_CONFIG_ACTIVE = active_database)
  executionSettings <- structure(list(
    'connectionDetails' = connectionDetails,
    'cdmDatabaseSchema' = cdmDatabaseSchema,
    'vocabularyDatabaseSchema' = vocabularyDatabaseSchema,
    'resultsDatabaseSchema' = resultsDatabaseSchema,
    'cohortTableName' = cohortTableName,
    'databaseId' = databaseId,
    'studyName' = studyName
  ), class = "executionSettings")
  
  return(executionSettings)
  
}