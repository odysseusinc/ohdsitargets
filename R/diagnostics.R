# TODO translate Cohort Diagnostics into targets workflow

createDatabaseMeta <- function(connectionDetails,
                               cdmDatabaseSchema,
                               vocabularyDatabaseSchema,
                               exportFolder,
                               databaseId,
                               databaseName = databaseId,
                               databaseDescription = databaseId,
                               minCellCount) {
  
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  #get cdm information
  cdmSourceInformation <-
    CohortDiagnostics:::getCdmDataSourceInformation(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema
    )
  
  #get vocabulary version
  vocabularyVersion <- CohortDiagnostics:::getVocabularyVersion(connection, vocabularyDatabaseSchema)
  
  CohortDiagnostics:::saveDatabaseMetaData(
    databaseId = databaseId,
    databaseName = databaseName,
    databaseDescription = databaseDescription,
    exportFolder = exportFolder,
    minCellCount = minCellCount,
    vocabularyVersionCdm = cdmSourceInformation$vocabularyVersion,
    vocabularyVersion = vocabularyVersion
  )
  
  path <- file.path(exportFolder, "database.csv")
  return(path)
  
}
