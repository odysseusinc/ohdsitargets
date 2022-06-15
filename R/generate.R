# Some code modified from CohortGenerator package https://github.com/OHDSI/CohortGenerator

#' Create cohort tables in a CDM database
#'
#' @param name The name of the cohort table as a string. Cannot contain spaces or special characters.
#' @param connectionDetails ConnectionDetails used to connect to the database. Created by DatabaseConnecton::createConnectionDetails()
#' @param cohortDatabaseSchema Schema where the cohort tables will be created. Write access is required.
#'
#' @return An object of class cohortTableRef that acts as a reference to the cohort tables created by the function.
#' @export
create_cohort_tables <- function(name, connectionDetails, cohortDatabaseSchema) {
  checkmate::check_character(name, len = 1)
  checkmate::check_character(cohortDatabaseSchema, len = 1)
  checkmate::check_class(connectionDetails, "connectionDetails")
  
  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  cohortTableNames <- list(cohortTable = name,
                          cohortInclusionTable = paste0(name, "_inclusion"),
                          cohortInclusionResultTable = paste0(name, "_inclusion_result"),
                          cohortInclusionStatsTable = paste0(name, "_inclusion_stats"),
                          cohortSummaryStatsTable = paste0(name, "_summary_stats"),
                          cohortCensorStatsTable = paste0(name, "_censor_stats"))
  
  sql <- readr::read_file(system.file("sql/sql_server/CreateCohortTables.sql", package = "ohdsitargets", mustWork = TRUE))
  
  sql <- SqlRender::render(sql = sql, 
                           cohort_database_schema = cohortDatabaseSchema, 
                           cohort_table = cohortTableNames$cohortTable, 
                           cohort_inclusion_table = cohortTableNames$cohortInclusionTable, 
                           cohort_inclusion_result_table = cohortTableNames$cohortInclusionResultTable, 
                           cohort_inclusion_stats_table = cohortTableNames$cohortInclusionStatsTable, 
                           cohort_summary_stats_table = cohortTableNames$cohortSummaryStatsTable, 
                           cohort_censor_stats_table = cohortTableNames$cohortCensorStatsTable, 
                           warnOnMissingParameters = TRUE)

  sql <- SqlRender::translate(sql = sql, targetDialect = connection@dbms)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  con <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))
  
  structure(list(cohortDatabaseSchema = cohortDatabaseSchema, 
                 cohortTableNames = cohortTableNames, 
                 createdTimestamp = Sys.time()), class = "cohortTableRef")
}


#' Generate one cohort
#' 
#' Generate a single cohort in the CDM database.
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param cohortTableRef A cohort table reference as returned by the `create_cohort_tables()` function
#' @param cohortId (integer) The id that should be assigned to the generated cohort
#' @param cohortName (character) The name of cohort
#' @param cohortFile (character) The path to the cohort json file relative to the project or working directory.
#'
#' @return An R object representing the generated cohort.
#' @export
generate_cohort <- function(connectionDetails,
                            cdmDatabaseSchema,
                            cohortTableRef,
                            cohortId,
                            cohortName,
                            cohortFile,
                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {

  cohortJson <- readr::read_file(cohortFile)
  cohortSql <- CirceR::buildCohortQuery(CirceR::cohortExpressionFromJson(cohortJson),
                                        CirceR::createGenerateOptions(generateStats = TRUE))
  
  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  cohortTableNames <- cohortTableRef$cohortTableNames
  cohortDatabaseSchema <- cohortTableRef$cohortDatabaseSchema
  sql <- SqlRender::render(sql = cohortSql,
                           cdm_database_schema = cdmDatabaseSchema,
                           vocabulary_database_schema = cdmDatabaseSchema,
                           target_database_schema = cohortDatabaseSchema,
                           results_database_schema = cohortDatabaseSchema,
                           target_cohort_table = cohortTableNames$cohortTable,
                           target_cohort_id = cohortId,
                           results_database_schema.cohort_inclusion = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionTable, sep="."),
                           results_database_schema.cohort_inclusion_result = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionResultTable, sep="."),
                           results_database_schema.cohort_inclusion_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortInclusionStatsTable, sep="."),
                           results_database_schema.cohort_summary_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortSummaryStatsTable, sep="."),
                           results_database_schema.cohort_censor_stats = paste(cohortDatabaseSchema, cohortTableNames$cohortCensorStatsTable, sep="."),
                           warnOnMissingParameters = FALSE)
  
  sql <- SqlRender::translate(sql = sql,
                              targetDialect = connectionDetails$dbms,
                              tempEmulationSchema = tempEmulationSchema)
  
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  cohortTableFullName <- paste(cohortTableRef$cohortDatabaseSchema, cohortTableRef$cohortTableNames$cohortTable, sep = ".")
  sql <- glue::glue("
  SELECT cohort_definition_id AS cohort_id,
    COUNT(*) AS cohort_entries,
    COUNT(DISTINCT subject_id) AS cohort_subjects
  FROM {cohortTableFullName} 
  WHERE cohort_definition_id = {cohortId}
  GROUP BY cohort_definition_id;")
  
  sql <- SqlRender::translate(sql = sql,targetDialect = connectionDetails$dbms)
  cnt <- DatabaseConnector::dbGetQuery(connection, sql)
  
  structure(list(cohort_id = cohortId, 
                 cohort_entries = max(cnt$cohort_entries, 0),
                 cohort_subjects = max(cnt$cohort_subjects, 0),
                 timestamp = Sys.time(),
                 cohortTableRef = cohortTableRef), class = "generatedCohort")
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
#' @importFrom rlang %||%
tar_cohorts <- function(cohortsToCreate, 
                        connectionDetails = config::get("connectionDetails"), 
                        cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                        cohortDatabaseSchema = config::get("resultsDatabaseSchema"),
                        cohortTableName = config::get("cohortTableName")) {
  
  # checkmate::check_class(connectionDetails, "connectionDetails")
  checkmate::check_character(cdmDatabaseSchema, len = 1, min.chars = 1)
  checkmate::check_character(cohortDatabaseSchema, len = 1, min.chars = 1)
  checkmate::check_character(cohortTableName, len = 1, min.chars = 1)
  # checkmate::check_data_frame(cohortsToCreate)
  
  cohortTableName <- cohortTableName %||% config::get("studyName") %||% "cohort"
  connectionDetails <- rlang::expr(connectionDetails)
  expr <- substitute(ohdsitargets::create_cohort_tables(cohortTableName, connectionDetails, cohortDatabaseSchema))
  list(
    targets::tar_target_raw("cohort_table", expr),
    tarchetypes::tar_map(values = cohortsToCreate, names = "cohortId",
            tar_target_raw("cohortFile", quote(here::here(filePath)), format = "file"),
            tar_target_raw("generatedCohort", substitute(generate_cohort(
              connectionDetails,
              cdmDatabaseSchema,
              cohortTableRef = cohort_table,
              cohortId = cohortId,
              cohortName = cohortName,
              cohortFile = cohortFile)))
    )
  )
}
