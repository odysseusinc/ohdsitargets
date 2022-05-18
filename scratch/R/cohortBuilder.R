#' TREADS Drug Exposure cohort builder
#'
#' This function builds all TREADS drug cohorts in a datbase
#'
#' @param connectionDetails a list of connectionDetails for DatabaseConnector
#' @param vocabularyDatbaseSchema the schema that hosts the vocabulary tables, defaults to value
#' in config.yml of the active configuration
#' @param cdmDatabaseSchema the schema that hosts the cdm tables, defaults to value
#' in config.yml of the active configuration
#' @param resultsDatabaseSchema the schema that hosts the users writeable results tables (or scratch), defaults to value
#' in config.yml of the active configuration
#' @param drugConceptsExcelPath the path pointing to the excel file with drug concepts,
#' defaults to an inst file in the package
#' @param irsJsonPath the path pointing to the json file with inclusion rules component,
#' defaults to an inst file in the package
#' @param jsonOutputPath the path the to the json output folder, defaults to value
#' in config.yml of the active configuration
#' @param cohortOutputPath the path the to the cohort output folder, defaults to value
#' in config.yml of the active configuration
#' @param database the actice database where the analysis is being done
#' @return a tibble containing, the cohortId, the atlasId, the counts generated, the circe json, ohdisql and read
#' @import usethis magrittr
#' @export
buildTreadCohorts <- function(connectionDetails = config::get("connectionDetails"),
                              vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
                              cdmDatabaseSchema = config::get("cdmDatabaseSchema"),
                              resultsDatabaseSchema = config::get("resultsDatabaseSchema"),
                              drugConceptsExcelPath = system.file("data/drugConceptSetList.xlsx", package = "treadsStudy"),
                              irsJsonPath = system.file("json/CKD+T2D_IRS.json", package = "treadsStudy"),
                              jsonOutputPath = config::get("jsonFolder"),
                              cohortOutputPath = config::get("cohortOutputFolder"),
                              database = config::get("database")) {


  #set up database connection
  connectionDetails <- eval(rlang::parse_expr(connectionDetails))
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))

  #get drug concept sets
  sheetNms1 <- readxl::excel_sheets(drugConceptsExcelPath)
  drugConceptSetList <- purrr::map(sheetNms1,
                                   ~readxl::read_xlsx(path = drugConceptsExcelPath,
                                                      sheet = .x)) %>%
    purrr::set_names(gsub("_.*", "", sheetNms1))

  #get inclusion rules component
  irs <- Capr::loadComponent(irsJsonPath)

  #build drug cohorts
  drugCohorts <- purrr::map2(drugConceptSetList, names(drugConceptSetList),
                             ~treadsDrugCohort(drugConcepts = .x,
                                               drugSetName = .y,
                                               irs = irs)) %>%
    Capr::createCohortDataframe()

  #write cohort dataframe to disk
  if(!dir.exists(file.path(cohortOutputPath, database))) {
  dir.create(file.path(cohortOutputPath, database), recursive = TRUE)
  }
  readr::write_rds(drugCohorts, file = file.path(cohortOutputPath, database,"cohortDataframe.rds"))
  usethis::ui_info(
    "data.frame of drug cohorts saved to: {ui_path(file.path(cohortOutputPath, database, \"cohortDataframe.rds\"))}"
    )

  #write cohort jsons to disk
  nn <- rownames(drugCohorts)
  json <- drugCohorts$json
  if(!dir.exists(file.path(jsonOutputPath, database))) {
    dir.create(file.path(jsonOutputPath, database), recursive = TRUE)
  }
  purrr::walk2(json, nn, ~readr::write_file(.x, file = file.path(jsonOutputPath, database,paste0(.y, ".json"))))
  purrr::walk(nn,
              ~usethis::ui_info(
                "{ui_field(.x)} cohort saved to: {ui_path(file.path(jsonOutputPath, database, paste0(.x, \".json\")))}"
                )
              )

  # generate cohorts

  #create tabNames same as setDatabaseConnections.R function
  # tabNames <- purrr::map(CohortGenerator::getCohortTableNames(),
  #                        ~paste(.x, config::get("studyName"), sep = "_"))

  #generate the cohorts save meta fie
  CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                     cdmDatabaseSchema = cdmDatabaseSchema,
                                     cohortDatabaseSchema = resultsDatabaseSchema,
                                     cohortTableNames = config::get("cohortTableNames"),
                                     cohortDefinitionSet = drugCohorts,
                                     incremental = TRUE,
                                     incrementalFolder = file.path(cohortOutputPath, database))

  cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,
                                                   cohortDatabaseSchema = resultsDatabaseSchema,
                                                   cohortTable = config::get("cohortTableNames")$cohortTable,
                                                   cohortDefinitionSet = drugCohorts) %>%
    dplyr::right_join(drugCohorts) %>%
    tibble::as_tibble() %>%
    tidyr::replace_na(list(cohortEntries = 0, cohortSubjects = 0)) %>%
    dplyr::arrange(cohortId) %>%
    dplyr::mutate(database = database)

  return(cohortCounts)
}


#decprecated - moved to a source call for user

# treadsDrugCohort <- function(connectionDetails = config::get("connectionDetails"),
#                             vocabularyDatabaseSchema = config::get("vocabularyDatabaseSchema"),
#                             drugConcepts,
#                             drugSetName,
#                             irs) {
#
#   #set up database connection
#   connectionDetails <- eval(rlang::parse_expr(connectionDetails))
#   conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
#   on.exit(DatabaseConnector::disconnect(conn))
#
#
#
#   # drug CSE ------------
#   drugs <- Capr::getConceptIdDetails(conceptIds = drugConcepts$conceptId,
#                                connection = conn,
#                                vocabularyDatabaseSchema = vocabularyDatabaseSchema,
#                                mapToStandard = TRUE)
#   conceptMapping <- Capr::createConceptMapping(n = nrow(drugConcepts),
#                                          isExcluded = drugConcepts$isExcluded,
#                                          includeDescendants = drugConcepts$includeDescendants,
#                                          includeMapped = drugConcepts$isMapped)
#   drugCse <- Capr::createConceptSetExpressionCustom(conceptSet = drugs,
#                                                     Name = drugSetName,
#                                                     conceptMapping = conceptMapping)
#
#   #create primary criteria of drug exposure --------------------
#   drugQuery <- Capr::createDrugExposure(conceptSetExpression = drugCse,
#                                   attributeList = list(
#                                     #age attribute
#                                     Capr::createAgeAttribute(Op = "gte", Value = 18),
#                                     #first time attribute
#                                     Capr::createFirstAttribute(),
#                                     #occurrence start Attribute
#                                     Capr::createOccurrenceStartDateAttribute(Op = "gt",
#                                                                        Value = "2012-01-01")
#                                   ))
#   ow <- Capr::createObservationWindow(PriorDays = 365, PostDays = 0)
#   pc <- Capr::createPrimaryCriteria(Name = paste(drugSetName, "Drug Exposure"),
#                               ComponentList = list(drugQuery),
#                               ObservationWindow = ow,
#                               Limit = "All")
#
#   # Create exit strategy --------------------
#   es <- Capr::createCustomEraEndStrategy(ConceptSetExpression = drugCse,
#                                    gapDays = 60L,
#                                    offset = 14L)
#
#   # Censoring events ---------------------------
#   cen <- Capr::createCensoringCriteria(Name = "Death Censoring Criteria",
#                                  ComponentList = list(Capr::createDeath()))
#
#   # Create cohort definition --------------------
#   cd <- Capr::createCohortDefinition(Name = paste(drugSetName, "Cohort"),
#                                PrimaryCriteria = pc,
#                                InclusionRules = irs,
#                                EndStrategy = es,
#                                CensoringCriteria = cen)
#
#   return(cd)
#
# }
