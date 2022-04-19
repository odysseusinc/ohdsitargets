

#trivial set functions

setProjectName <- function(projectName) {
  projectName
}

setCdmDatabaseSchema <- function(cdmDatabaseSchema) {
  cdmDatabaseSchema
}

setCohortDatabaseSchema <- function(cohortDatabaseSchema) {
  cohortDatabaseSchema
}

setVocabularyDatabaseSchema <- function(vocabularyDatabaseSchema) {
  vocabularyDatabaseSchema
}

setDuckDb <- function(path, name) {
  con_duck <- dbConnect(duckdb::duckdb(), file.path(path, paste0(name,".duckdb")))
  dbDisconnect(con_duck, shutdown = TRUE) #always remember to shutdown when disconnecting from duckdb
}