#testing 
library(DatabaseConnector)
library(tidyverse)
library(CohortDiagnostics)

#set connection Details
connectionDetails <- createConnectionDetails(dbms = "postgresql",
                                             server = "testnode.arachnenetwork.com/synpuf_110k",
                                             user = "ohdsi",
                                             password = keyring::key_get("cdm_password"),
                                             port = "5441")

cdmDatabaseSchema <- "cdm_531"
vocabularyDatabaseSchema <- "cdm_531"
cohortDatabaseSchema <- "martin_lavallee_results"
databaseId <- "synpuf_110k"
databaseName <- databaseId
databaseDescription <- databaseId
tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")
#cohortTable <- "cohort_txpath"
cohortTableNames <- purrr::map(CohortGenerator::getCohortTableNames(),
                               ~paste(.x, "txpath", sep = "_"))
cohortDefinitionSet <- readr::read_rds("~/R/tstTreatmentPatterns/data/cohortDefinitionSet.rds") 
minCellCount <- 5L
exportFolder <- "scratch"
# check that cohort definition set is properly formated
cohortTableColumnNamesObserved <- colnames(cohortDefinitionSet) %>%
  sort()
cohortTableColumnNamesExpected <-
  getResultsDataModelSpecifications() %>%
  dplyr::filter(.data$tableName == "cohort") %>%
  dplyr::pull(.data$fieldName) %>%
  SqlRender::snakeCaseToCamelCase() %>%
  sort()
cohortTableColumnNamesRequired <-
  CohortDiagnostics::getResultsDataModelSpecifications() %>%
  dplyr::filter(.data$tableName == "cohort") %>%
  dplyr::filter(.data$isRequired == "Yes") %>%
  dplyr::pull(.data$fieldName) %>%
  SqlRender::snakeCaseToCamelCase() %>%
  sort()

expectedButNotObsevered <-
  setdiff(x = cohortTableColumnNamesExpected, y = cohortTableColumnNamesObserved)
if (length(expectedButNotObsevered) > 0) {
  requiredButNotObsevered <-
    setdiff(x = cohortTableColumnNamesRequired, y = cohortTableColumnNamesObserved)
}
obseveredButNotExpected <-
  setdiff(x = cohortTableColumnNamesObserved, y = cohortTableColumnNamesExpected)

#make cohort definition set exportable
cohortDefinitionSet <- CohortDiagnostics:::makeDataExportable(
  x = cohortDefinitionSet,
  tableName = "cohort",
  minCellCount = minCellCount,
  databaseId = NULL
)

#connect to database
connection <- DatabaseConnector::connect(connectionDetails)

#get cdm Source Info
cdmSourceInformation <-
  CohortDiagnostics:::getCdmDataSourceInformation(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema
  )

# get vocabulary version
vocabularyVersion <- CohortDiagnostics:::getVocabularyVersion(connection, vocabularyDatabaseSchema)

#get observation Period
observationPeriodDateRange <- renderTranslateQuerySql(
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

#saveDatabaseMeta
database <- CohortDiagnostics:::saveDatabaseMetaData(
  databaseId = databaseId,
  databaseName = databaseName,
  databaseDescription = databaseDescription,
  exportFolder = "test",
  minCellCount = minCellCount,
  vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
  vocabularyVersion = vocabularyVersion
)


# create concept table
CohortDiagnostics:::createConceptTable(connection, tempEmulationSchema)
sql <-
  SqlRender::loadRenderTranslateSql(
    "CreateConceptIdTable.sql",
    packageName = "CohortDiagnostics",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    table_name = DBI::SQL("martin_lavallee_results.concept_ids")
  )
DatabaseConnector::executeSql(
  connection = connection,
  sql = sql,
  progressBar = FALSE,
  reportOverallTime = FALSE
)

#get cohortcounts
cohortCounts <- CohortDiagnostics::getCohortCounts(connectionDetails = connectionDetails,
                                                   cohortDatabaseSchema = cohortDatabaseSchema,
                                                   cohortTable = cohortTableNames$cohortTable,
                                                   cohortIds = cohortDefinitionSet$cohortId) %>%
  CohortDiagnostics:::makeDataExportable(tableName = "cohort_count", minCellCount = minCellCount,
                                         databaseId = databaseId)
  
#get instantiated Cohorts
instantiatedCohorts <- cohortCounts %>%
  dplyr::filter(.data$cohortEntries > 0) %>%
  dplyr::pull(.data$cohortId)

# get inclusion stats
ff <- CohortDiagnostics:::getInclusionStats(connection,
                              exportFolder,
                              databaseId,
                              cohortDefinitionSet,
                              cohortDatabaseSchema,
                              cohortTableNames,
                              incremental = FALSE,
                              instantiatedCohorts,
                              minCellCount,
                              recordKeepingFile = NULL)
  

# run concept set diagnostics

conceptSetDiagnostics <- CohortDiagnostics:::runConceptSetDiagnostics(connection = connection,
                                                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                                                      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                                                      databaseId = databaseId,
                                                                      exportFolder = exportFolder, 
                                                                      cohorts = cohortDefinitionSet,
                                                                      runIncludedSourceConcepts = TRUE,
                                                                      runOrphanConcepts = TRUE,
                                                                      runBreakdownIndexEvents = TRUE,
                                                                      minCellCount = minCellCount,
                                                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                                                      cohortTable = cohortTableNames$cohortTable,
                                                                      conceptIdTable = "#concept_ids", 
                                                                      tempEmulationSchema = tempEmulationSchema)


## Error in conceptIdTable for fetch source concepts missing concept_ids need to
#create table

#run time series

subset <- CohortDiagnostics:::subsetToRequiredCohorts(
  cohorts = cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts),
  task = "runCohortTimeSeries",
  incremental = FALSE,
  recordKeepingFile = NULL
)

cohortIds <- subset$cohortId

timeSeries <- CohortDiagnostics::runCohortTimeSeriesDiagnostics(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTableNames$cohortTable,
  runCohortTimeSeries = TRUE,
  runDataSourceTimeSeries = FALSE,
  timeSeriesMinDate = as.Date("1980-01-01"),
  timeSeriesMaxDate = as.Date(Sys.Date()),
  stratifyByGender = TRUE,
  stratifyByAgeGroup = TRUE,
  cohortIds = cohortIds
)

# run Visit Context ---------------------
subset <- CohortDiagnostics:::subsetToRequiredCohorts(
  cohorts = cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts),
  task = "runVisitContext",
  incremental = FALSE,
  recordKeepingFile = NULL
)

visitContext <- CohortDiagnostics:::getVisitContext(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTableNames$cohortTable,
  cohortIds = subset$cohortId,
  conceptIdTable = "#concept_ids"
)
#error also here because of concept_ids


## Run Incidence Rate ----------------------------
subset <- CohortDiagnostics:::subsetToRequiredCohorts(
  cohorts = cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts),
  task = "runIncidenceRate",
  incremental = FALSE,
  recordKeepingFile = NULL
)


incidenceRate <- CohortDiagnostics:::getIncidenceRate(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTableNames$cohortTable,
  vocabularyDatabaseSchema = vocabularyDatabaseSchema,
  cdmVersion = 5,
  firstOccurrenceOnly = TRUE,
  washoutPeriod = 365,
  cohortId = subset$cohortId
)

## Run Cohort Relationship -----------------
temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useDemographicsAgeGroup = TRUE,
  useDemographicsRace = TRUE,
  useDemographicsEthnicity = TRUE,
  useDemographicsIndexYear = TRUE,
  useDemographicsIndexMonth = TRUE,
  useDemographicsIndexYearMonth = TRUE,
  useDemographicsPriorObservationTime = TRUE,
  useDemographicsPostObservationTime = TRUE,
  useDemographicsTimeInCohort = TRUE,
  useConditionOccurrence = TRUE,
  useProcedureOccurrence = TRUE,
  useDrugEraStart = TRUE,
  useMeasurement = TRUE,
  useConditionEraStart = TRUE,
  useConditionEraOverlap = TRUE,
  useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
  useConditionEraGroupOverlap = TRUE,
  useDrugExposure = FALSE, # leads to too many concept id
  useDrugEraOverlap = FALSE,
  useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
  useDrugEraGroupOverlap = TRUE,
  useObservation = TRUE,
  useDeviceExposure = TRUE,
  useCharlsonIndex = TRUE,
  useDcsi = TRUE,
  useChads2 = TRUE,
  useChads2Vasc = TRUE,
  useHfrs = FALSE,
  temporalStartDays = c(
    # components displayed in cohort characterization
    -9999, # anytime prior
    -365, # long term prior
    -180, # medium term prior
    -30, # short term prior
    
    # components displayed in temporal characterization
    -365, # one year prior to -31
    -30, # 30 day prior not including day 0
    0, # index date only
    1, # 1 day after to day 30
    31,
    -9999 # Any time prior to any time future
  ),
  temporalEndDays = c(
    0, # anytime prior
    0, # long term prior
    0, # medium term prior
    0, # short term prior
    
    # components displayed in temporal characterization
    -31, # one year prior to -31
    -1, # 30 day prior not including day 0
    0, # index date only
    30, # 1 day after to day 30
    365,
    9999 # Any time prior to any time future
  )
)




cohortRelationship <- CohortDiagnostics::runCohortRelationshipDiagnostics(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTableNames$cohortTable,
  targetCohortIds = subset$cohortId,
  comparatorCohortIds = cohortDefinitionSet$cohortId,
  relationshipDays = dplyr::tibble(
    startDay = temporalCovariateSettings$temporalStartDays,
    endDay = temporalCovariateSettings$temporalEndDays
  ),
  observationPeriodRelationship = TRUE
)


# temporal Cohort Characterization 

subset <- subsetToRequiredCohorts(
  cohorts = cohorts %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts),
  task = "runTemporalCohortCharacterization",
  incremental = FALSE,
  recordKeepingFile = NULL
)

characteristics <- getCohortCharacteristics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    cohortIds = subset$cohortId,
    covariateSettings = temporalCovariateSettings,
    cdmVersion = 5
  )