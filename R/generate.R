

create_cohort_tables <- function(name, connectionDetails, cohortDatabaseSchema) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
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

  structure(list(cohortDatabaseSchema = cohortDatabaseSchema, 
                 cohortTableNames = cohortTableNames, 
                 createdTimestamp = Sys.time()), class = "cohortTableRef")
}

#' Create cohort tables
#'
#' Returns a target that references the cohort table in the database
#'
#' @param name the name of the cohort table as a string
#' @param connectionDetails Database connectionDetails object created by createConnectionDetails()
#' @param cohortDatabaseSchema character string with the schema where the cohort tables can be created
#' @param cohortTableNames A list of cohort table names created by cohortTableNames
#'
#' @return a target named cohort_table - list that contains the data needed to query the cohort table and the time it was created
#' @export
tar_cohort_tables <- function(name = config::get("cohortTableName"),
                              connectionDetails = config::get("connectionDetails"),
                              cohortDatabaseSchema = config::get("resultsDatabaseSchema")) { 

  stopifnot(is.character(name))

  # Return an R object that acts as a local reference the cohort table in the database
  # Importantly, if this function ever reruns the output will be different from the previous output since the timestamp will be different
  expr <- substitute(ohdsitargets:::create_cohort_tables(name, connectionDetails, cohortDatabaseSchema))
  targets::tar_target_raw("cohort_table", expr)
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
generate_cohort <- function(connectionDetails,
                           cdmDatabaseSchema,
                           cohortTableRef,
                           cohortId,
                           cohortName,
                           cohortFile) {

  cohortJson <- readr::read_file(cohortFile)
  cohortSql <- CirceR::buildCohortQuery(CirceR::cohortExpressionFromJson(cohortJson),
                                        CirceR::createGenerateOptions(generateStats = TRUE))
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  cohortTableNames <- cohortTableRef$cohortTableNames
  sql <- SqlRender::render(sql = sql,
                           cdm_database_schema = cdmDatabaseSchema,
                           vocabulary_database_schema = cdmDatabaseSchema,
                           target_database_schema = cohortTableRef$cohortDatabaseSchema,
                           results_database_schema = cohortTableRef$cohortDatabaseSchema,
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
  
  DatabaseConnector::executeSql(connection, sql)
  
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
                 timestamp = lubridate::now(),
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
tar_cohorts <- function(cohortsToCreate, cohortTableRef, connectionDetails = config::get("connectionDetails"), cdmDatabaseSchema = config::get("cdmDatabaseSchema")) {
  # name <- targets::tar_deparse_language(substitute(name))
  # stopifnot(is.list(cohortTableRef), is.data.frame(cohortsToCreate))
  expr <- substitute(ohdsitargets:::create_cohort_tables(name, connectionDetails, cohortDatabaseSchema))
  list(
    targets::tar_target_raw("cohort_table", expr),
    tarchetypes::tar_map(values = cohortsToCreate, names = "cohortId",
            tar_target_raw("cohortFile", quote(here::here(filePath)), format = "file"),
            tar_target_raw("generatedCohort", substitute(generate_cohort(
              connectionDetails,
              cdmDatabaseSchema,
              cohortTableRef = cohortTableRef,
              cohortId = cohortId,
              cohortName = cohortName,
              cohortFile = cohortFile)))
    )
  )
}
