#function to build project Cohort Tables
projectCohortTables <- function(projectName,
                                connectionDetails,
                                cohortDatabaseSchema) {
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))
  
  tabNames <- paste(projectName, CohortGenerator::getCohortTableNames(), sep = "_")
  CohortGenerator::createCohortTables(connection = conn,
                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                      cohortTableNames = cohortTableNames,
                                      incremental = TRUE)
  check <- DatabaseConnector::getTableNames(conn, databaseSchema = cohortDatabaseSchema) %>%
    as_tibble() %>%
    filter(grepl(toupper(projectName), value))
  return(check)
}

#trivial set functions

setProjectName <- function(projectName) {
  projectName
}

setCdmDatabaseSchema <- function(cdmDatabaseSchema){
  cdmDatabaseSchema
}

setCohortDatabaseSchema <- function(cohortDatabaseSchema){
  cohortDatabaseSchema
}

setVocabularyDatabaseSchema <- function(vocabularyDatabaseSchema){
  vocabularyDatabaseSchema
}

