# TODO translate Cohort Diagnostics into targets workflow

diagnosticsDatabaseMeta <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema,
                                    exportFolder,
                                    databaseId,
                                    databaseName = databaseId,
                                    databaseDescription = databaseId,
                                    minCellCount) {
  
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  #get cdm information
  cdmSourceInformation <-
    CohortDiagnostics:::getCdmDataSourceInformation(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema
    )
  
  #get vocabulary version
  vocabularyVersion <- CohortDiagnostics:::getVocabularyVersion(connection, vocabularyDatabaseSchema)
  
  CohortDiagnostics:::saveDatabaseMetaData(
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
    vocabularyVersion = vocabularyVersion
  )
  
  path <- file.path(exportFolder, "database.csv")
  return(path)
  
}

getObservationPeriod <- function(connectionDetails,
                                 tempEmulationSchema = NULL,
                                 cdmDatabaseSchema){
  
  
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
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


diagnostics_ComputeCohortCounts <- function(connectionDetails,
                                            cohortDatabaseSchema,
                                            cohortTable,
                                            cohorts, databaseId) {
  
  cohortCounts <- CohortDiagnostics:::getCohortCounts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId
  ) %>%
    mutate(databaseId = databaseId)
  return(cohortCounts)
}