#' Get meta information about database
#' 
#' Retrieve database meta data
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param vocabularyDatabaseSchema (character) Schema where the vocabulary tables lives in the database
#' @param databaseId (character) identify which database is being used
#' @return A tibble with meta information about the database
#' @export
diagnostics_database_meta <- function(connectionDetails,
                                      cdmDatabaseSchema,
                                      vocabularyDatabaseSchema,
                                      databaseId) {
  
  #connect to database 
  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  #get cdm information
  cdmSourceInformation <-
    CohortDiagnostics:::getCdmDataSourceInformation(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema
    )
  
  #get vocabulary version
  vocabularyVersion <- CohortDiagnostics:::getVocabularyVersion(connection, vocabularyDatabaseSchema)
  
  databaseName <- databaseId
  databaseDescription <- databaseId
  
  vocabularyVersion <- paste(vocabularyVersion, collapse = ";")
  vocabularyVersionCdm <- paste(cdmSourceInformation$vocabularyVersion, collapse = ";")
  database <- dplyr::tibble(
    databaseId = databaseId,
    databaseName = dplyr::coalesce(databaseName, databaseId),
    description = dplyr::coalesce(databaseDescription, databaseId),
    vocabularyVersionCdm = !!vocabularyVersionCdm,
    vocabularyVersion = !!vocabularyVersion,
    isMetaAnalysis = 0
  )
  
  return(database)
  
}

#' Get information about the observation period
#' 
#' See the length of coverage for the database in calendar and person years
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param tempEmulationSchema the temporary table schema used in certain databases
#' @return A tibble with information about the observation period
#' @export
diagnostics_database_observation_period <- function(connectionDetails,
                                                    cdmDatabaseSchema,
                                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")){
  
  
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  #run observation period query
  observationPeriodDateRange <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT MIN(observation_period_start_date) observation_period_min_date,
             MAX(observation_period_end_date) observation_period_max_date,
             COUNT(distinct person_id) persons,
             COUNT(person_id) records,
             SUM(DATEDIFF(dd, observation_period_start_date, observation_period_end_date)) person_days
             FROM @cdm_database_schema.observation_period;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  return(observationPeriodDateRange)
}


#' Create series of targets that provide information about the database
#'
#' Summarize information about the database used in the analysis
#'
#' @param connectionDetails ConnectionDetails
#'
#' @return multiple targets with information about database (summarize more later)
#' @export
tar_database_diagnostics <- function(active_database = "eunomia",
                                     connectionDetails,
                                     cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                     vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
                                     cohortDatabaseSchema = config::get("resultsDatabaseSchema"),
                                     cohortTableName = config::get("cohortTableName"),
                                     databaseId = config::get("databaseName")) {
  
  
  Sys.setenv(R_CONFIG_ACTIVE = active_database)
  connectionDetails_sym <- rlang::expr(connectionDetails)
  
  #create call functions
  
  databaseMeta_call <- substitute(diagnostics_database_meta(
    connectionDetails_sym,
    cdmDatabaseSchema,
    vocabularyDatabaseSchema,
    databaseId
  ))
  
  databaseOP_call <- substitute(diagnostics_database_observation_period(
    connectionDetails_sym,
    cdmDatabaseSchema
  ))
  
  list(
    targets::tar_target_raw("database_meta", databaseMeta_call),
    targets::tar_target_raw("database_observationPeriod", databaseOP_call)
  )
  
}

tar_cohort_diagnostics <- function(cohortsToTarget,
                                   connectionDetails,
                                   active_database = "eunomia",
                                   cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                   vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
                                   cohortDatabaseSchema = config::get("resultsDatabaseSchema"),
                                   cohortTableName = config::get("cohortTableName"),
                                   databaseId = config::get("databaseName")) {
  
}

