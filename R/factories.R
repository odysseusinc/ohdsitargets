#factory to set omop connections
omopConnection_factory <- function(cdmDatabaseSchema,
                                   cohortDatabaseSchema,
                                   vocabularyDatabaseSchema) {
  
  # sym_cdmDatabaseSchema <- rlang::expr(cdmDatabaseSchema)
  # sym_cohortSchema <- rlang::expr(cohortSchema)
  # sym_vocabularyDatabaseSchema <- rlang::expr(vocabularyDatabaseSchema)
  
  #set commands
  command_createConnectionDetails <- substitute(
    DatabaseConnector::createConnectionDetails(dbms = "postgresql",
                                               server = config::get("server"),
                                               user = config::get("user"),
                                               password = config::get("password"),
                                               port = "5441")
  )
  
  command_setCdmDatabaseSchema <- substitute(
    setCdmDatabaseSchema(cdmDatabaseSchema = cdmDatabaseSchema),
    env = list(cdmDatabaseSchema = cdmDatabaseSchema)
  )
  
  command_setCohortDatabaseSchema <- substitute(
    setCohortDatabaseSchema(cohortDatabaseSchema = cohortDatabaseSchema),
    env = list(cohortDatabaseSchema = cohortDatabaseSchema)
  )
  
  command_setVocabularyDatabaseSchema <- substitute(
    setVocabularyDatabaseSchema(vocabularyDatabaseSchema = vocabularyDatabaseSchema),
    env = list(vocabularyDatabaseSchema = vocabularyDatabaseSchema)
  )
  

  #create target factory
  list(
    tar_target_raw("connectionDetails", command_createConnectionDetails, deployment = "main"),
    tar_target_raw("cdmDatabaseSchema", command_setCdmDatabaseSchema, deployment = "main"),
    tar_target_raw("cohortDatabaseSchema", command_setCohortDatabaseSchema, deployment = "main"),
    tar_target_raw("vocabularyDatabaseSchema", command_setVocabularyDatabaseSchema, deployment = "main")
  )
}




# TODO create buildCohort_factory

buildCohortCapr_factory <- function(connectionDetails,
                                    conceptsFile,
                                    caprFunctions,
                                    vocabularyDatabaseSchmea) {
  
  command_createCohortDataframe <- substitute(Capr::createCohortDataframe(cohortList = cohortList, 
                                                                          generateStats = TRUE),
                                              env = list(cohortList = sym_cohortList))
  
  
  list(
    tar_target_raw("conceptSets", command_loadConceptSets, deployment = "main"),
    tar_target_raw("cohortList", command_runCaprCohorts, deployment = "main"),
    tar_target_raw("cohortDataframe", command_createCohortDataframe, deployment = "main")
  )
  
}

# TODO create loadWebApi_factory
loadWebApi_factory <- function(baseUrl,
                               cohortIds) {
  
  #create commands -----
  #set the baseurl
  command_setBaseUrl <- substitute(
    setUrl(webApiUrl = baseUrl),
    env = list(baseUrl = baseUrl)
  )
  
  #set the cohortIds to extract
  command_setCohortIds <- substitute(
    setCohortIds(cohortIds = cohortIds),
    env = list(cohortIds = cohortIds)
  )
  
  sym_baseUrl <- rlang::expr(baseUrl)
  sym_cohortIds <- rlang::expr(cohortIds)
  
  #grab the cohorts from webApi
  command_grabCohorts <- substitute(
    ROhdsiWebApi::exportCohortDefinitionSet(baseUrl = baseUrl,
                                            cohortIds = cohortIds,
                                            generateStats = TRUE),
    env = list(baseUrl = sym_baseUrl,
                cohortIds = sym_cohortIds)
  )
  
  #set factory
  list(
    tar_target_raw("baseUrl", command_setBaseUrl, deployment = "main"),
    tar_target_raw("cohortIds", command_setCohortIds, deployment = "main"),
    tar_target_raw("cohortDataframe", command_grabCohorts, deployment = "main")
  )
}


#TODO factory to generate cohorts
generateCohortSet_factory <- function(connectionDetails,
                                      cohortDataframe,
                                      cdmDatabaseSchema,
                                      cohortDatabaseSchema,
                                      projectName) {

  
  
  #create symbols
  sym_tabNames <- rlang::expr(cohortTables)
  sym_connectionDetails <- rlang::expr(connectionDetails)
  sym_cdmDatabaseSchema <- rlang::expr(cdmDatabaseSchema)
  sym_cohortDatabaseSchema <- rlang::expr(cohortDatabaseSchema)
  sym_cohortDataframe <-  rlang::expr(cohortDataframe)
  
  #create calls  
  command_createCohortTables <- substitute(projectCohortTables(projectName = projectName,
                                                               connectionDetails = connectionDetails,
                                                               cohortDatabaseSchema = cohortDatabaseSchema),
                                           env = list(projectName = projectName,
                                                      connectionDetails = sym_connectionDetails,
                                                      cohortDatabaseSchema = sym_cohortDatabaseSchema))

  command_generateCohortSet <- substitute(CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                                             cdmDatabaseSchema = cdmDatabaseSchema,
                                                                             cohortDatabaseSchema = cohortDatabaseSchema,
                                                                             cohortTableNames = cohortTableNames,
                                                                             cohortDefinitionSet = cohortDefinitionSet),
                                          env = list(connectionDetails = sym_connectionDetails,
                                                     cdmDatabaseSchema = sym_cdmDatabaseSchema,
                                                     cohortDatabaseSchema = sym_cohortDatabaseSchema,
                                                     cohortTableNames = sym_tabNames,
                                                     cohortDefinitionSet = sym_cohortDataframe))
  
  list(
    tar_target_raw("cohortTables", command_createCohortTables, deployment = "main"),
    tar_target_raw("cohortGenerate", command_generateCohortSet, deployment = "main")
  )
}


# TODO create cohortDiagnostics_factory