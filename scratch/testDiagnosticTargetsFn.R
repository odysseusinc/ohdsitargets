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
exportFolder <- "scratch"
studyName <- "txPath"


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
