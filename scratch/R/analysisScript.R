# script to run analysis 

#load libraries
library(tidyverse)
library(Capr)
library(CirceR)
library(CohortGenerator)
library(SqlRender)

#set connection Details
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = config::get("server"),
  user = config::get("user"),
  password = config::get("password"),
  port = "5441"
)

#create concept sets for analysis
source(here::here("data/raw/conceptSetLists.R"))
source(here::here("R/createCohortFn.R"))


#set information on where we will write and read information from cdm
writeDetails <- list(cdmSchema = "cdm_531",
                     resultsSchema = "martin_lavallee_results",
                     vocabularyDatabaseSchema = "cdm_531",
                     cohortTable = "COHORT")

## Cohort Generation ----------------------------------------------

# Create the first line therapy cohorts
firstLineTherapyCohorts <- purrr::map(c(1,3), ~firstLineHypertensionTherapy(connectionDetails = connectionDetails,
                                                                           vocabularyDatabaseSchema = writeDetails$vocabularyDatabaseSchema,
                                                                           followUp = .x,
                                                                           conceptSetList = firstLineConcepttList))

#create hypertension drug use cohorts
hypertensionDrugUseCohorts <- purrr::map2(drugUseConceptSetList, names(drugUseConceptSetList) ,
                                         ~hypertensionDrugUse(connectionDetails = connectionDetails,
                                                                     vocabularyDatabaseSchema = writeDetails$vocabularyDatabaseSchema,
                                                                     drugConceptSet = .x,
                                                                     cohortName = .y))

#create a list of cohorts to generate on backend db
cohortList <- append(firstLineTherapyCohorts, hypertensionDrugUseCohorts)
#turn into data frame for cohort generator
df <- createCohortDataFrame(cohortList = cohortList) %>%
  as_tibble() %>%
  mutate(cohortId = cohortId + 4,
         atlasId = cohortId)

gg <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                         cdmDatabaseSchema = writeDetails$cdmSchema,
                                         cohortDatabaseSchema = writeDetails$resultsSchema,
                                         cohortDefinitionSet = df)
cohortCounts <- CohortGenerator::getCohortCounts(
  connectionDetails = connectionDetails,
  cohortDatabaseSchema = writeDetails$resultsSchema,
  cohortTable = writeDetails$cohortTable) %>%
  inner_join(gg, by = c("cohortId")) %>%
  inner_join(df, by = c("cohortId"))

as_tibble(cohortCounts)


## Cohort Diagnostics ---------------------------------------------


#skip for now to reliant on ROhdsiWebApi
library(CohortDiagnostics)

dir.create("diagnostics")

CohortDiagnostics::executeDiagnostics(cohortDefinitionSet = df,
                                        connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = writeDetails$cdmSchema,
                                        cohortDatabaseSchema = writeDetails$resultsSchema,
                                        vocabularyDatabaseSchema = writeDetails$vocabularyDatabaseSchema,
                                        cohortTable = writeDetails$cohortTable,
                                        cohortIds = c(5,7:15),
                                        exportFolder = "diagnostics",
                                        databaseId = "synpuf",
                                        minCellCount = 5L)


## Run PheValuator ------------------------

library(PheValuator)

## Need to figure out what phenotype we are trying to evaluate? DOes pheValuator work for 
# drugs or just conditions?



## Pathways ------------------------------------------------------

