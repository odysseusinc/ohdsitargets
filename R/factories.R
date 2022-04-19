#factory to set omop connections
omopConnection_factory <- function(projectName,
                                   cdmDatabaseSchema,
                                   cohortDatabaseSchema,
                                   vocabularyDatabaseSchema) {
  
  command_createConnectionDetails <- substitute(
    DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                               server = config::get("server"),
                                               user = config::get("user"),
                                               password = config::get("password"),
                                               port = "5441")
    )
  
  command_createCohortTables <- substitute(
    projectCohortTables(projectName = projectName,
                        connectionDetails = connectionDetails,
                        cohortDatabaseSchema = cohortDatabaseSchema),
    env = list(projectName = sym_projectName,
               connectionDetails = sym_connectionDetails,
               cohortDatabaseSchema = sym_cohortDatabaseSchema)
    )
  
  
  list(
    tar_target_raw("projectName", command_setProjectName, deployment = "main"),
    tar_target_raw("cdmDatabaseSchema", command_setCdmDatabaseSchema, deployment = "main"),
    tar_target_raw("cohortDatabaseSchema", command_setCohortDatabaseSchema, deployment = "main"),
    tar_target_raw("vocabularyDatabaseSchema", command_setCocabularyDatabaseSchema, deployment = "main"),
    tar_target_raw("connectionDetails", command_createConnectionDetails, deployment = "main"),
    tar_target_raw("cohortTables", command_createCohortTables, deployment = "main")
  )
}




# TODO create buildCohort_factory

buildCohortCapr_factory <- function(connectionDetails,
                                    conceptsFile,
                                    caprFunctions,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchmea) {
  
}

# TODO create loadWebApi_factory
loadWebApi_factory <- function(baseurl,
                               cohortIds) {
  
  #create commands -----
  #set the baseurl
  command_setBaseUrl <- substitute(
    setUrl(baseurl = baseurl),
    env = list(baseurl = baseurl)
  )
  
  #set the cohortIds to extract
  command_setCohortIds <- substitute(
    setCohortIds(cohortIds = cohortIds),
    env = list(cohortIds = cohortIds)
  )
  
  #grab the cohorts from webApi
  command_grabCohorts <- substitute(
    ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl,
                                            cohortIds = cohortIds,
                                            generateStats = TRUE),
    env = list(baseUrl = baseUrl,
                cohortIds = cohortIds)
  )
  
  
  list(
    tar_target_raw("baseUrl", command_setBaseUrl, format = "url", deployment = "main"),
    tar_target_raw("cohortIds", command_setCohortIds, deployment = "main"),
    tar_target_raw("webApiCohorts", command_grabCohorts, deployment = "main")
  )
}


#TODO factory to generate cohorts
generateCohort_factory <- function(connectionDetails,
                                   cohortList,
                                   cdmDatabaseSchema,
                                   cohortDatabaseSchema,
                                   projectName) {
  
  #list project table names
  tabNames <- paste(projectName, CohortGenerator::getCohortTableNames(), sep = "_")
  
  
  #create target names
  # projName <- deparse(substitute(projectName))
  # name_tabNames <- paste0(projName, "_tableNames")
  # name_dataframe <- paste0(projName, "_cohortDf")
  # name_generate <- paste0(projName, "_generateDf")
  
  #create symbols
  sym_projectName <- rlang::expr(projectName)
  sym_tabNames <- rlang::expr(tabNames)
  sym_connectionDetails <- rlang::expr(connectionDetails)
  sym_cdmDatabaseSchema <- rlang::expr(cdmDatabaseSchema)
  sym_cohortDatabaseSchema <- rlang::expr(cohortDatabaseSchema)
  sym_cohortList <- rlang::expr(cohortList)
  sym_cohortDataframe <- as.symbol('cohortDataframe')
  
  #create calls  
  command_createCohortTables <- substitute(projectCohortTables(projectName = projectName,
                                                               connectionDetails = connectionDetails,
                                                               cohortDatabaseSchema = cohortDatabaseSchema),
                                           env = list(projectName = sym_projectName,
                                                      connectionDetails = sym_connectionDetails,
                                                      cohortDatabaseSchema = sym_cohortDatabaseSchema))
  command_createCohortDataframe <- substitute(Capr::createCohortDataframe(cohortList = cohortList, 
                                                                          generateStats = TRUE),
                                              env = list(cohortList = sym_cohortList))
  command_generateCohortSet <- substitute(CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                                             cdmDatabaseSchema = cdmDatabaseSchema,
                                                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                                                             cohortTableNames = cohortTableNames,
                                                                             cohortDefinitionSet = cohortDefinitionSet,
                                                                             incremental = TRUE),
                                          env = list(connectionDetails = sym_connectionDetails,
                                                     cdmDatabaseSchema = sym_cdmDatabaseSchema,
                                                     cohortDatabaseSchema = sym_cohortDatabaseSchema,
                                                     cohortTableNames = sym_tabNames,
                                                     cohortDefinitionSet = sym_cohortDataframe))
  
  list(
    # tar_target_raw("connectionDetails", command_createConnectionDetails, deployment = "main"),
    # tar_target_raw("cohortTables", command_createCohortTables, deployment = "main"),
    tar_target_raw("cohortDataframe", command_createCohortDataframe, deployment = "main"),
    tar_target_raw("cohortGenerate", command_generateCohortSet, deployment = "main")
  )
}


# TODO create cohortDiagnostics_factory