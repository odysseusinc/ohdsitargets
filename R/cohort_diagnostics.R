
#' Incidence analysis for cohort diagnostics
#' 
#' Incidence stratified by age, gender and index year used in cohort diagnostics
#'
#' @param connectionDetails ConnectionDetails
#' @param cdmDatabaseSchema (character) Schema where the CDM data lives in the database
#' @param tempEmulationSchema the temporary table schema used in certain databases
#' @param vocabularyDatabaseSchema (character) Schema where the vocabulary tables lives in the database
#' @param generatedCohort dependency object of generated cohort class that tracks cohort used in incidence
#' analysis
#' @param cdmVersion the version of the cdm >= 5
#' @param firstOccurrenceOnly a logic toggle for first occurrence
#' @param washoutPeriod number of days for washout
#' @return A dataframe with incidence calculations with several combinations
#' @export
cohort_diagnostics_incidence <- function(connectionDetails,
                                         cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                                         tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                         vocabularyDatabaseSchema =config::get("vocabularyDatabaseSchema"),
                                         generatedCohort,
                                         cdmVersion = 5, 
                                         firstOccurrenceOnly,
                                         washoutPeriod) {
  
  
  cohortTableNames <- generatedCohort$cohortTableRef$cohortTableNames
  cohortDatabaseSchema <- generatedCohort$cohortTableRef$cohortDatabaseSchema
  cohortTable <- generatedCohort$cohortTableRef$cohortTableNames$cohortTable
  cohortId <- generatedCohort$cohort_id
  
  incidenceRate <- CohortDiagnostics:::getIncidenceRate(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cdmVersion = cdmVersion,
    firstOccurrenceOnly = firstOccurrenceOnly,
    washoutPeriod = washoutPeriod,
    cohortId = cohortId
  )
  
  return(incidenceRate)
  
}





getStatsTable <- function(connectionDetails,
                          cohortDatabaseSchema,
                          table,
                          cohortId,
                          databaseId) {
  suppressMessages(connection <- DatabaseConnector::connect(connectionDetails = connectionDetails))
  on.exit(DatabaseConnector::disconnect(connection), add = TRUE)
  
  sql <- "SELECT {@database_id != ''}?{CAST('@database_id' as VARCHAR(255)) as database_id,} * 
  FROM @cohort_database_schema.@table 
  WHERE cohort_definition_id = @cohort_id"
  data <- DatabaseConnector::renderTranslateQuerySql(
    sql = sql,
    connection = connection,
    snakeCaseToCamelCase = FALSE,
    table = table,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_id = cohortId,
    database_id = ifelse(is.null(databaseId), yes = "", no = databaseId)
  )
  return(data)
}

#' Inclusion Rule analysis for cohort diangostics
#' 
#' Extract information on inclusion rules
#'
#' @param connectionDetails ConnectionDetails
#' @param generatedCohort dependency object of generated cohort class that tracks cohort used in incidence
#' analysis
#' @param databaseId (character) identify which database is being used
#' @return A list of dataframes retrieving inclusion rule states generated for the cohorts
#' @export
cohort_diagnostics_inclusion_rules <- function(connectionDetails,
                                               generatedCohort,
                                               databaseId = config::get("databaseName")) {
  
  
  
  
  #get names from generated Cohort obj
  cohortTableNames <- generatedCohort$cohortTableRef$cohortTableNames
  #remove cohort inclusion table
  cohortTableNames$cohortInclusionTable <- NULL
  
  cohortDatabaseSchema <- generatedCohort$cohortTableRef$cohortDatabaseSchema
  cohortId <- generatedCohort$cohort_id
  
  
  statTables <- purrr::map(cohortTableNames, ~getStatsTable(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    table = .x,
    cohortId = cohortId,
    databaseId = databaseId))
  
  return(statTables)
  
  
}

#' Create references to inclusion rules analysis per cohort
#'
#' Creates targets of inclusion rules for each cohort in the project
#'
#' @param cohortsToCreate A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath
#' @param executionSettings An object containing all information of the database connection created from config file
#' @return One target for each generated cohort with names cohortInclusionRules_{id}
#' @export
tar_cohort_inclusion_rules <- function(cohortsToCreate,
                                       executionSettings) {
  
  #extract out all execution settings
  connectionDetails <- executionSettings$connectionDetails
  databaseId <- executionSettings$databaseId

  #create tibble to track generated cohorts
  nn <- 1:nrow(cohortsToCreate)
  iter <- tibble::tibble('generatedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
                         'cohortId' = nn)
  
  #missing step that extracts the names of the inclusion rules from the cohort definitions
  #see CohortGeneratro::insertInclusionRuleNames
  #TODO add this function in different 
  list(
    tarchetypes::tar_map(values = iter, 
                         names = "cohortId",
                         tar_target_raw("cohortInclusionRules", 
                                        substitute(
                                          cohort_diagnostics_inclusion_rules(
                                            connectionDetails = connectionDetails,
                                            generatedCohort = generatedCohort,
                                            databaseId = databaseId)
                                        )
                         )
    )
  )
  
}

#' Create references to inclusion rules analysis per cohort
#'
#' Creates targets of inclusion rules for each cohort in the project
#'
#' @param cohortsToCreate A dataframe with one row per cohort and the following columns: cohortId, cohortName, cohortJsonPath
#' @param incidenceAnalysisSettings a dataframe where each row is a different combination of anlayis
#' @param executionSettings An object containing all information of the database connection created from config file
#' @return One target for each generated cohort with names cohortInclusionRules_{id}
#' @export
tar_cohort_incidence <- function(cohortsToCreate,
                                 incidenceAnalysisSettings,
                                 executionSettings) {
  
  #extract out all execution settings
  connectionDetails <- executionSettings$connectionDetails
  cdmDatabaseSchema <- executionSettings$cdmDatabaseSchema
  vocabularyDatabaseSchema <- executionSettings$vocabularyDatabaseSchema
  databaseId <- executionSettings$databaseId
  
  
  nn <- 1:nrow(cohortsToCreate)
  
  gg <- tibble::tibble('generatedCohort' = rlang::syms(paste("generatedCohort", nn, sep ="_")),
                       'cohortId' = nn)
  
  iter <- bind_cols(gg, 
                    incidenceAnalysisSettings)
  
  list(
    tarchetypes::tar_map(values = iter, 
                         names = "cohortId",
                         tar_target_raw("cohortIncidence", 
                                        substitute(
                                          cohort_diagnostics_incidence(
                                            connectionDetails = connectionDetails,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                            generatedCohort = generatedCohort,
                                            firstOccurrenceOnly = firstOccurrenceOnly,
                                            washoutPeriod = washoutPeriod)
                                        )
                         )
    )
  )
  
}


# tar_cohort_diagnostics <- function(cohortsToCreate,
#                                    incidenceAnalysisSettings,
#                                    connectionDetails,
#                                    active_database = "eunomia",
#                                    cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
#                                    vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
#                                    databaseId = config::get("databaseName")){
#   list(
#     tar_cohort_inclusion_rules(cohortsToCreate = cohortsToCreate,
#                                connectionDetails = connectionDetails,
#                                active_database = active_database,
#                                databaseId = databaseId),
#     tar_cohort_incidence(cohortsToCreate = cohortsToCreate,
#                          incidenceAnalysisSettings = incidenceAnalysisSettings,
#                          connectionDetails = connectionDetails,
#                          active_database = active_database,
#                          cdmDatabaseSchema = cdmDatabaseSchema,
#                          vocabularyDatabaseSchema = vocabularyDatabaseSchema,
#                          databaseId = databaseId)
#   )
# }
