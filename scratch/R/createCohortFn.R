#load libraries
# library(tidyverse)
# library(Capr)
# library(CirceR)
# library(CohortGenerator)


#set connection Details
# connectionDetails <- DatabaseConnector::createConnectionDetails(
#   dbms = "postgresql",
#   server = config::get("server"),
#   user = config::get("user"),
#   password = config::get("password"),
#   port = "5441"
# )

#create concept sets for analysis
#source(here::here("data/raw/conceptSetLists.R"))

#functions for creating cohort of first line therapy
firstLineHypertensionTherapy <- function(connectionDetails,
                                         vocabularyDatabaseSchema,
                                         followUp = c(1, 3),
                                         conceptSetList) {
  
  #set up database connection
  connectionDetails <- eval(rlang::parse_expr(connectionDetails))
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))
  
  #create concept set expressions------------------
  conceptSetList2 <- purrr::map(conceptSetList, ~Capr::getConceptIdDetails(conceptIds = .,
                                                                           connectionDetails = connectionDetails,
                                                                           vocabularyDatabaseSchema = vocabularyDatabaseSchema)) %>%
    purrr::map2(names(conceptSetList), ~Capr::createConceptSetExpression(conceptSet = .x, Name = .y))
  
  # create primary criteria --------------
  #create drug exposure query for first line hypertension drugs
  firstLineDrug <- createDrugExposure(conceptSetExpression = conceptSetList2$firstLineHypertensionDrugs,
                                      attributeList = list(createFirstAttribute()))
  #observation window
  if (followUp == 1) {
    ow <- createObservationWindow(PriorDays = 365L, PostDays = 365L)
  } else {
    ow <- createObservationWindow(PriorDays = 365L, PostDays = as.integer(365*3))
  }
  
  #build pc
  pc <- createPrimaryCriteria(Name = "primary criteria",
                              ComponentList = list(firstLineDrug),
                              ObservationWindow = ow,
                              Limit = "First")
  
  
  #create inclussion rules-----------------------
  
  #start with hypertension drug exposure
  hypertensionDrugExposure <- createDrugExposure(conceptSetExpression = conceptSetList2$hypertensiveDrugs)
  #set up window for count1
  win1 <- createWindow(StartDays = "All", StartCoeff = "Before",
                       EndDays = 1, EndCoeff = "Before")
  #create first count of no other hypertension drugs
  count1 <- createCount(Query = hypertensionDrugExposure,
                        Logic = "exactly",
                        Count = 0,
                        Timeline = createTimeline(StartWindow = win1))
  #create second count of at least 1 occurrence of hypertension
  hypertensionCondition <- createConditionOccurrence(conceptSetExpression = conceptSetList2$hypertensiveDisorder)
  #set up window for count2
  win2 <- createWindow(StartDays = 365, StartCoeff = "Before",
                       EndDays = 0, EndCoeff = "After")
  count2 <- createCount(Query = hypertensionCondition,
                        Logic = "at_least",
                        Count = 1,
                        Timeline = createTimeline(StartWindow = win2))
  ir1 <- createGroup(Name = "hypertension diag with no other hypertension drugs",
                     type = "ALL",
                     criteriaList = list(count1, count2))
  irs <- createInclusionRules(Name = "Inclusion rules for first line hypertension cohort",
                              Contents = list(ir1),
                              Limit = "First")
  #create cohorts
  cohortName <- paste("First-Line Therapy for Hypertension with", followUp, "year followUp")
  cd <- createCohortDefinition(Name = cohortName,
                               PrimaryCriteria = pc,
                               InclusionRules = irs)
  
  return(cd)
}

#function to create cohorts for hypertension drug use by category
hypertensionDrugUse <- function(connectionDetails,
                                vocabularyDatabaseSchema,
                                drugConceptSet,
                                cohortName) {
  
  #set up database connection
  connectionDetails <- eval(rlang::parse_expr(connectionDetails))
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))
  
  
  cse <- Capr::getConceptIdDetails(conceptIds = drugConceptSet,
                                   connectionDetails = connectionDetails,
                                   vocabularyDatabaseSchema = vocabularyDatabaseSchema) %>%
    createConceptSetExpression(Name = cohortName)
  
  # create primary criteria --------------
  #create drug exposure query for first line hypertension drugs
  drug <- createDrugExposure(conceptSetExpression = cse)
  #build pc
  pc <- createPrimaryCriteria(Name = "primary criteria",
                              ComponentList = list(drug),
                              ObservationWindow = createObservationWindow(),
                              Limit = "All")
  
  es <- createCustomEraEndStrategy(ConceptSetExpression = cse, gapDays = 30, offset = 0)
  cer <- createCohortEra(EraPadDays = 30L)
  
  cd <- createCohortDefinition(Name = paste(cohortName, "Use Cohort"),
                               PrimaryCriteria = pc,
                               EndStrategy = es,
                               CohortEra = cer)
  return(cd)

  
}


# #function to create all the cohorts
# createCohortDataFrame <- function(cohortList) {
#   
#   check <- purrr::map_chr(cohortList, ~methods::is(.x))
#   
#   if(!all(check == "CohortDefinition")) {
#     stop("all cohorts need to be a Capr CohortDefinition class")
#   }
#   
#   # get cohort names
#   cohortName <-purrr::map_chr(cohortList, ~slot(slot(.x, "CohortDetails"), "Name"))
#   
#   #set basic genops for CirceR
#   genOp <- CirceR::createGenerateOptions(generateStats = TRUE)
#   
#   #get the info from compiler
#   cohortInfo <- purrr::map(cohortList, ~Capr::compileCohortDefinition(.x, 
#                                                                       generateOptions = genOp))
#   
#   #get each element from compile
#   cohortSql <- purrr::map_chr(cohortInfo, ~getElement(.x, "ohdiSQL"))
#   cohortJson <- purrr::map_chr(cohortInfo, ~getElement(.x, "circeJson"))
#   cohortRead <- purrr::map_chr(cohortInfo, ~getElement(.x, "cohortRead"))
#   
#   
#   #create Data frame
#   df <- data.frame('cohortId' = seq_along(cohortList), 
#                    'cohortName'= cohortName, 
#                    'sql'= cohortSql, 
#                    'json' = cohortJson,
#                    'read' = cohortRead)
#   return(df)
# }