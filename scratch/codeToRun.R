################################################################
# Code to Run
# Written by: Martin Lavallee Odysseus Data Services Inc
# Questions contact martin.lavallee@odysseusinc.com
################################################################


library(config)
library(keyring)
library(Capr)
library(DatabaseConnector)
library(SqlRender)
library(CohortDiagnostics)
library(CohortGenerator)
library(tidyverse, quietly = TRUE)
library(usethis)

#source the analysisConfiguration funcitons
source("scratch/R/analysisConfiguration.R")

#source the R Capr template function
source("scratch/R/createCohortFn.R")
##############################
# Running a single Database
# To run multiple databases skip tp line: 92
###############################

# Step 1: Initialize the Study Session -----------------------------------------

# The first step is to initialize the study session. This entails providing
# all necessary details used to connect and interact with your CDM dbms. This
# function creates a config.yml file in your R project session


initializeStudySession(
  studyName = "testPipelines",
  database = c("synpuf_110k"), #accepts multiple inputs
  dbms = c("postgresql"), #accepts multiple inputs
  server = c("testnode.arachnenetwork.com/synpuf_110k"), #accepts multiple inputs
  user = c("ohdsi"), #accepts multiple inputs
  port = c("5441"), #accepts multiple inputs
  cdmDatabaseSchema = c("cdm_531"), #accepts multiple inputs
  vocabularyDatabaseSchema = c("cdm_531"), #accepts multiple inputs
  resultsDatabaseSchema = c("martin_lavallee_results") #accepts multiple inputs
  )

# For each database you plan to run the study, you must supply the password
# to connect to the server. This function uses {keyring} to maintain passwords
# on your local R studio session. For each database you will be prompted to
# input the password.

# Step 2: Orient database ----------------------------------
pointToDatabase("synpuf_110k")


## Cohort Generation ----------------------------------------------

# Create the first line therapy cohorts
firstLineTherapyCohorts <- purrr::map(c(1,3), ~firstLineHypertensionTherapy(connectionDetails = config::get("connectionDetails"),
                                                                            vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
                                                                            followUp = .x,
                                                                            conceptSetList = firstLineConcepttList))

#create hypertension drug use cohorts
hypertensionDrugUseCohorts <- purrr::map2(drugUseConceptSetList, names(drugUseConceptSetList) ,
                                          ~hypertensionDrugUse(connectionDetails = config::get("connectionDetails"),
                                                               vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
                                                               drugConceptSet = .x,
                                                               cohortName = .y))

#create a list of cohorts to generate on backend db
cohortList <- append(firstLineTherapyCohorts, hypertensionDrugUseCohorts)
#turn into data frame for cohort generator
df <- Capr::createCohortDataframe(cohortList = cohortList) %>%
  as_tibble() %>%
  mutate(atlasId = cohortId)



