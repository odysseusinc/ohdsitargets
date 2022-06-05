set_credential <- function(database, credential) {
  cred <- toupper(paste(database, credential, sep = "_"))
  keyring::key_set(cred, prompt = paste0(cred, ":"))
  usethis::ui_done("keyring set for {ui_field(cred)}")
}


init_study <- function(path) {
  default <- list('default' = list(
    cohortOutput = "output/cohorts/results",
    diagnosticsOutput = "output/diagnostics/results",
    characterizationOutput = "output/characterization/results"
  )
  )
  
  eunomia <- list('eunomia' = list(
    database = "Eunomia",
    dbms = "sqlite",
    cdmDatabaseSchema = "main",
    vocabularyDatabaseSchema = "main",
    resultsDatabaseSchema = "main")
    )
  config <- append(default, eunomia)
  yaml::write_yaml(config, file = file.path(path,"config.yml"))
  invisible()
}

#' Function to add new database connection in config file
#' 
#' This function will add a new database connection into the config.yml file
#' which specifies the configurations of the ohdsi study. The function will
#' prompt a keyring input for the user, host and password. 
add_databaseConnection <- function(database,
                                   dbms,
                                   port = NULL,
                                   cdmDatabaseSchema,
                                   vocabularyDatabaseSchema,
                                   resultsDatabaseSchema,
                                   configFile = Sys.getenv("R_CONFIG_FILE", "config.yml")) {
  
  #load old yaml file
  oldYml <- yaml::yaml.load_file(configFile)
  
  purrr::walk(c("host", "user", "password"), 
              ~set_credential(database = database, credential = .x))
  
  ll <- list(
    list(
      database = database,
      dbms = dbms,
      user = keyring::key_get(toupper(paste(database, "user", sep = "_"))),
      password = keyring::key_get(toupper(paste(database, "user", sep = "_"))),
      server = paste(keyring::key_get(toupper(paste(database, "user", sep = "_"))), database, sep = "/"),
      port = port,
      cdmDatabaseSchema = cdmDatabaseSchema,
      vocabularyDatabaseSchema = vocabularyDatabaseSchema,
      resultsDatabaseSchema = resultsDatabaseSchema
    )
  )
  names(ll) <- database
  yaml::write_yaml(append(oldYml, ll), file = "config.yml")
  usethis::ui_done("Added new database connection: {ui_value(database)}")
  
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




