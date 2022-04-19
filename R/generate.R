#function to build project Cohort Tables
projectCohortTables <- function(projectName,
                                connectionDetails,
                                cohortDatabaseSchema) {
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))
  
  tabNames <- CohortGenerator::getCohortTableNames() %>%
    purrr::map(~paste(projectName, .x, sep = "_"))
  CohortGenerator::createCohortTables(connection = conn,
                                      cohortDatabaseSchema = cohortDatabaseSchema,
                                      cohortTableNames = tabNames,
                                      incremental = TRUE)
  return(tabNames)
}