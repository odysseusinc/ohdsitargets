#test 

#testing 
library(DatabaseConnector)
library(tidyverse)
# library(CohortDiagnostics)

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
exportFolder <- "scratch/tst"
studyName <- "txPath"
conceptIdTable <- paste0(cohortDatabaseSchema, ".concept_ids", "_", studyName)

source("R/diagnostics.R")


#Step1
database <- diagnosticsDatabaseMeta(connectionDetails = connectionDetails,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                        databaseId = databaseId,
                        minCellCount = minCellCount)

op <- diagnosticsObservationPeriod(connectionDetails = connectionDetails,
                                   tempEmulationSchema = tempEmulationSchema,
                                   cdmDatabaseSchema = cdmDatabaseSchema)

#Step 2 
diagnosticsConceptTable(connectionDetails = connectionDetails,
                        tempEmulationSchema = tempEmulationSchema,
                        cohortDatabaseSchema = cohortDatabaseSchema,
                        studyName = studyName)

#Step 3
counts <- diagnosticsComputeCohortCounts(connectionDetails = connectionDetails,
                               cohortDatabaseSchema = cohortDatabaseSchema,
                               cohortTable = cohortTableNames$cohortTable,
                               cohorts = cohortDefinitionSet, 
                               databaseId = databaseId, 
                               minCellCount = minCellCount)
# step 4
inclusionStats <- diagnosticsInclusionStats(connectionDetails = connectionDetails,
                                            databaseId = databaseId,
                                            cohortDefinitionSet = cohortDefinitionSet,
                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                            minCellCount = minCellCount,
                                            cohortTableNames = cohortTableNames,
                                            instantiatedCohorts = counts$instantiatedCohorts)

#step 5: concept Set Diagnostics

# included source concept 
includedSourceConcept <- diagnosticsIncludedSourceConcepts(connectionDetails = connectionDetails,
                                                           cohorts = cohortDefinitionSet,
                                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                                           vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                                           tempEmulationSchema = tempEmulationSchema,
                                                           minCellCount = minCellCount,
                                                           databaseId = databaseId,
                                                           conceptIdTable = conceptIdTable)

# run breakdown index events

breakdownIndexEvents <- diagnosticsIndexEventBreakdown(connectionDetails = connectionDetails,
                                                       cohorts = cohortDefinitionSet,
                                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                                       vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                                       tempEmulationSchema = tempEmulationSchema,
                                                       cohortTable = cohortTableNames$cohortTable,
                                                       cohortDatabaseSchema = cohortDatabaseSchema,
                                                       minCellCount = minCellCount,
                                                       databaseId = databaseId,
                                                       conceptIdTable = conceptIdTable)

# run orphan concept 
orphanConcepts <- diagnosticsOrphanConcepts(connectionDetails = connectionDetails,
                                            cohorts = cohortDefinitionSet,
                                            cdmDatabaseSchema = cdmDatabaseSchema,
                                            cohortDatabaseSchema = cohortDatabaseSchema,
                                            tempEmulationSchema = tempEmulationSchema,
                                            conceptIdTable = conceptIdTable,
                                            studyName = studyName)


# step 6: time series
timeSeries <- diagnosticsTimeSeries(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortDefinitionSet = cohortDefinitionSet,
  cohortTable = cohortTableNames$cohortTable,
  instantiatedCohorts = counts$instantiatedCohorts,
  runCohortTimeSeries = TRUE,
  runDataSourceTimeSeries = FALSE,
  timeSeriesMinDate = as.Date("1980-01-01"),
  timeSeriesMaxDate = as.Date(Sys.Date()),
  stratifyByGender = TRUE,
  stratifyByAgeGroup = TRUE
)

#step 7: visit context
visitContext <- diagnosticsVisitContext(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        cohortDatabaseSchema = cohortDatabaseSchema,
                                        cohortDefinitionSet = cohortDefinitionSet,
                                        cohortTable = cohortTableNames$cohortTable,
                                        instantiatedCohorts = counts$instantiatedCohorts,
                                        conceptIdTable = conceptIdTable)

# step 8: incidence rate
incidenceRate <- diagnosticsIncidenceRate(connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                          tempEmulationSchema = tempEmulationSchema,
                                          cohortDatabaseSchema = cohortDatabaseSchema,
                                          cohortDefinitionSet = cohortDefinitionSet,
                                          cohortTable = cohortTableNames$cohortTable,
                                          instantiatedCohorts = counts$instantiatedCohorts,
                                          vocabularyDatabaseSchema = vocabularyDatabaseSchema,
                                          cdmVersion = 5, 
                                          firstOccurrenceOnly = TRUE,
                                          washoutPeriod = 365)

#step 9: cohort relationship

temporalCovariates <-  FeatureExtraction::createTemporalCovariateSettings(
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

cohortRelationship <- diagnosticsCohortRelationship(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTableNames$cohortTable,
  cohortDefinitionSet = cohortDefinitionSet,
  instantiatedCohorts = counts$instantiatedCohorts,
  temporalCovariateSettings = temporalCovariates
)


temporalCharacterization <- diagnosticsTemproalCohortCharacterization(
  connectionDetails = connectionDetails,
  cdmDatabaseSchema = cdmDatabaseSchema,
  cohortDatabaseSchema = cohortDatabaseSchema,
  cohortTable = cohortTableNames$cohortTable,
  cohortDefinitionSet = cohortDefinitionSet,
  instantiatedCohorts = counts$instantiatedCohorts,
  temporalCovariateSettings = temporalCovariates,
  tempEmulationSchema = tempEmulationSchema
)
