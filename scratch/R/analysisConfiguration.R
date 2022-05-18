setPasswords <- function(databases) {
  purrr::map(databases, ~toupper(paste(.x, "password", sep = "_"))) %>%
    purrr::walk(~keyring::key_set(.x, prompt = paste(.x, "Password:"))) %>%
    purrr::walk(~usethis::ui_done("keyring set for {ui_field(.x)}"))
}

#' Function that initializes the study project for analysis
#'
#' This function sets the config.yml file used to facilitate aspects of the
#' database connection used for the analysis. Contact your database administrator
#' to ensure you have all the information needed to run an OHDSI analysis. You must
#' have read access to the cdm and vocabulary schemas. You must have read and write
#' access to a user specific results schema (or a scratch schema). The results schema
#' should be seperate from the schema used by webAPI. This function does not have an input for
#' passwords, which is done by design. When this function is run, the user will be prompted to input
#' the password corresponding to the database user, using the {keyring} package in R. This input
#' does not leave a trace in the .Rhistory.
#'
#' @param database the database names hosting the OMOP data (ex. synpuf_110k). Can take multiple inputs
#' @param studyName a name for this study; default is TREADS
#' @param jarFolder a folder to download the jdbc drivers for DatabaseConnector; default is jarFolder
#' @param diagnosticsFolder a folder to write CohortDiagnostics results; default is diagnostics
#' @param jsonFolder a folder to write cohort definition jsons; default is json
#' @param cohortOutputFolder a folder to write rds files for cohort definitions; default is cohort
#' @param dbms the dbms used for the OMOP database (ex. postgresql, redshift, sql server). This
#' argument takes all dbms types that are compatible with DatabaseConnector. Can take multiple inputs based on the number of databases entered
#' @param server the server name for the OMOP database. Contact your database admin for a full server string.
#' Can take multiple inputs based on the number of databases entered
#' @param user the user for the OMOP database. Can take multiple inputs based on the number of databases entered
#' @param port the port used for the OMOP databse. Can take multiple inputs based on the number of databases entered
#' @param cdmDatabaseSchema the schema that holds the cdm tables. Make sure your user has read access.
#' Can take multiple inputs based on the number of databases entered.
#' @param vocabularyDatabaseSchmea the schema that holds the vocabulary tables. Many times this is the same as the cdm schema.
#' Make sure your user has read access. Can take multiple inputs based on the number of databases entered.
#' @param resultsDatabaseSchema the schema that hosts the result tables for this study. This schema should be separate from the
#' the results schema used by webAPI. Make sure your user has read and write access. Can take multiple inputs based on the number
#' of databases entered.
#' @import usethis magrittr
#' @export

initializeStudySession <- function(database,
                             studyName,
                             jarFolder = "scratch/jarFolder",
                             diagnosticsFolder = "scratch/diagnostics",
                             jsonFolder = "scratch/json",
                             cohortOutputFolder = "scratch/cohort",
                             dbms,
                             server,
                             user,
                             port,
                             cdmDatabaseSchema,
                             vocabularyDatabaseSchema,
                             resultsDatabaseSchema) {
  #set environment variable
  Sys.setenv(R_CONFIG_ACTIVE = database[1])
  usethis::ui_info("R_CONFIG_ACTIVE variable set to {ui_value(database[1])}")

  Sys.setenv(DATABASECONNECTOR_JAR_FOLDER = jarFolder)
  usethis::ui_info("DATABASECONNECTOR_JAR_FOLDER variable set to {ui_path(jarFolder)}")

  purrr::walk(dbms, ~DatabaseConnector::downloadJdbcDrivers(dbms = .x))
  usethis::ui_done("Downloaded Jdbc Drivers to {ui_value(jarFolder)}")

  setPasswords(database)

  #set keyring function for password
  pw <- toupper(paste(database, "password", sep = "_")) %>%
    purrr::map(~rlang::call2(
                  expr(keyring::key_get),
                  .x)) %>%
    purrr::map(~deparse1(.x))

  # setup connection details
  conCalls <- purrr::map(c("dbms", "user", "password", "server", "port"),
                         ~rlang::call2(
                           expr(config::get),
                           .x)) %>%
    purrr::set_names(c("dbms", "user", "password", "server", "port"))
  conCalls$password <- expr(eval(rlang::parse_expr(!!conCalls$password)))
   conDet <- rlang::call2(
      expr(DatabaseConnector::createConnectionDetails),
      !!!conCalls) %>%
     rlang::call_standardise() %>%
    deparse1()


  #table names for analysis
  tabNames <- purrr::map(CohortGenerator::getCohortTableNames(),
                          ~paste(.x, studyName, sep = "_"))

  #get all inputs for configFile
  default <- list('default' = list(studyName = studyName,
                                   diagnosticsFolder = diagnosticsFolder,
                                   jsonFolder = jsonFolder,
                                   cohortOutputFolder = cohortOutputFolder,
                                   connectionDetails = conDet,
                                   cohortTableNames = tabNames))
  ll <- list(database = database,
             dbms = dbms,
             server = server,
             user = user,
             password = pw,
             port = port,
             cdmDatabaseSchema = cdmDatabaseSchema,
             vocabularyDatabaseSchema = vocabularyDatabaseSchema,
             resultsDatabaseSchema = resultsDatabaseSchema) %>%
    purrr::transpose() %>%
    purrr::set_names(database)



  yaml::write_yaml(append(default, ll), file = "config.yml")
  usethis::ui_done("Initialized config.yml file")

  #invisible return
  invisible()
}

#' Function that orients the active database in the analysis
#'
#' A study site can have multiple OMOP databases. Many times they use the same dbms, however,
#' the databases a separate. One must connect to each database in order to execute an analysis.
#' This functions helps the user to point to a specific database to conduct the analysis.
#' The database specified will become the active configuration for the config.yml file.
#' With the configuration specified, the correct environment variables corresponding
#' to the database will be retrieved to use for the analysis of that particular database.
#' This function will also create the study specific tables in the users results schema
#' to save cohort counts and use for cohort diagnostics. There is no return in this function
#' however, notes will be printed to the console informing the user of what environment
#' variables are being used.
#'
#' @param database the database for the analysis (ex. synpuf_110k)
#' @import usethis
#' @export
pointToDatabase <- function(database) {
  Sys.setenv(R_CONFIG_ACTIVE = database)
  usethis::ui_info("R_CONFIG_ACTIVE variable set to {ui_value(database)}")

  #check the connection information

  dbms <- config::get("dbms")
  usethis::ui_info("dbms for {ui_field(database)} is {ui_value(dbms)}")

  server <- config::get("server")
  usethis::ui_info("server for {ui_field(database)} is {ui_value(server)}")

  user <- config::get("user")
  usethis::ui_info("user for {ui_field(database)} is {ui_value(user)}")

  vocabularyDatabaseSchema <- config::get("vocabularyDatabaseSchema")
  usethis::ui_info("vocabularyDatabaseSchema for {ui_field(database)} is {ui_value(vocabularyDatabaseSchema)}")

  resultsDatabaseSchema <- config::get("resultsDatabaseSchema")
  usethis::ui_info("resultsDatabaseSchema for {ui_field(database)} is {ui_value(resultsDatabaseSchema)}")

  cdmDatabaseSchema <- config::get("cdmDatabaseSchema")
  usethis::ui_info("cdmDatabaseSchema for {ui_field(database)} is {ui_value(cdmDatabaseSchema)}")


  #create cohort tables
  # tabNames <- purrr::map(CohortGenerator::getCohortTableNames(),
  #                        ~paste(.x, config::get("studyName"), sep = "_"))
  connectionDetails <- eval(rlang::parse_expr(config::get("connectionDetails")))

  CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                      cohortDatabaseSchema = resultsDatabaseSchema,
                                      cohortTableNames = config::get("cohortTableNames"),
                                      incremental = TRUE)
  # usethis::ui_done("cohort tables created in {ui_value(resultsDatabaseSchema)}")
  # usethis::ui_info("Cohort Table Name: {ui_field(tabNames$cohortTable)}")
}




