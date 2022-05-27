# TODO translate Cohort Diagnostics into targets workflow


# Step 1: Database Target --------------------------------
diagnosticsDatabaseMeta <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema,
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
  
  vocabularyVersion <- paste(vocabularyVersion, collapse = ";")
  vocabularyVersionCdm <- paste(cdmSourceInformation$vocabularyVersion, collapse = ";")
  database <- dplyr::tibble(
    databaseId = databaseId,
    databaseName = dplyr::coalesce(databaseName, databaseId),
    description = dplyr::coalesce(databaseDescription, databaseId),
    vocabularyVersionCdm = !!vocabularyVersionCdm,
    vocabularyVersion = !!vocabularyVersion,
    isMetaAnalysis = 0
  )
  database <- CohortDiagnostics:::makeDataExportable(
    x = database,
    tableName = "database",
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  return(database)
  
}

diagnosticsObservationPeriod <- function(connectionDetails,
                                         tempEmulationSchema = NULL,
                                         cdmDatabaseSchema){
  
  
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  #run observation period query
  observationPeriodDateRange <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT MIN(observation_period_start_date) observation_period_min_date,
             MAX(observation_period_end_date) observation_period_max_date,
             COUNT(distinct person_id) persons,
             COUNT(person_id) records,
             SUM(DATEDIFF(dd, observation_period_start_date, observation_period_end_date)) person_days
             FROM @cdm_database_schema.observation_period;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  )
  return(observationPeriodDateRange)
}

# Step 2: Initiate Concept Table Target ----------------------------
diagnosticsConceptTable <- function(connectionDetails,
                                    tempEmulationSchema,
                                    cohortDatabaseSchema, 
                                    studyName) {
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  sql <-
    SqlRender::loadRenderTranslateSql(
      "CreateConceptIdTable.sql",
      packageName = "CohortDiagnostics",
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      table_name = DBI::SQL(cohortDatabaseSchema, paste0("concept_ids", "_", studyName))
    )
  DatabaseConnector::executeSql(
    connection = connection,
    sql = sql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}



# Step 3: Cohort Counts Targets --------------------------------------

diagnosticsComputeCohortCounts <- function(connectionDetails,
                                           cohortDatabaseSchema,
                                           cohortTable,
                                           cohorts, 
                                           databaseId, 
                                           minCellCount) {
  
  cohortCounts <- CohortDiagnostics:::getCohortCounts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId
  )
  
  cohortCounts <- CohortDiagnostics:::makeDataExportable(
    x = cohortCounts,
    tableName = "cohort_count",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  
  instantiatedCohorts <- cohortCounts %>%
    dplyr::filter(.data$cohortEntries > 0) %>%
    dplyr::pull(.data$cohortId)
  
  
  ll <- list(cohortCounts = cohortCounts,
             instantiatedCohorts = instantiatedCohorts)
  
  return(ll)
}


# diagnosticsInstantiatedCohorts <- function(cohortCounts) {
#   instantiatedCohorts <- cohortCounts %>%
#     dplyr::filter(.data$cohortEntries > 0) %>%
#     dplyr::pull(.data$cohortId)
#   return(instantiatedCohorts)
# }
# Step 4: Inclusion Rule Target --------------------------------------


diagnosticsInclusionStats <- function(connectionDetails,
                                      databaseId,
                                      cohortDefinitionSet,
                                      cohortDatabaseSchema,
                                      cohortTableNames,
                                      minCellCount,
                                      instantiatedCohorts) {
  
  
  #connect to database
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  #From CohortDiagnostics InclusionRules.R L178:201
  cohorts <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)

  CohortGenerator::insertInclusionRuleNames(
    connection = connection,
    cohortDefinitionSet = subset,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortInclusionTable = cohortTableNames$cohortInclusionTable
  )
  
  inclusionStatisticsFolder <-
    tempfile("CdCohortStatisticsFolder")
  on.exit(unlink(inclusionStatisticsFolder), add = TRUE)
  CohortGenerator::exportCohortStatsTables(
    connection = connection,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = inclusionStatisticsFolder,
    incremental = FALSE
  ) # Note use of FALSE to always generate stats here
  stats <-
    CohortDiagnostics:::getInclusionStatisticsFromFiles(
      cohortIds = subset$cohortId,
      folder = inclusionStatisticsFolder
   )
  #get inclusion rule info
  inclusionRuleStats <- CohortDiagnostics:::makeDataExportable(
    x = stats$inclusionRuleStats,
    tableName = "inclusion_rule_stats",
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  cohortInclusion <- CohortDiagnostics:::makeDataExportable(
    x = stats$cohortInclusion,
    tableName = "cohort_inclusion",
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  cohortIncStats <- CohortDiagnostics:::makeDataExportable(
    x = stats$cohortIncStats,
    tableName = "cohort_inc_stats",
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  cohortIncResult <- CohortDiagnostics:::makeDataExportable(
    x = stats$cohortIncResult,
    tableName = "cohort_inc_result",
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  cohortSummaryStats <- CohortDiagnostics:::makeDataExportable(
    x = stats$cohortSummaryStats,
    tableName = "cohort_summary_stats",
    databaseId = databaseId,
    minCellCount = minCellCount
  )
  
  ll <- list(inclusionRuleStats = inclusionRuleStats,
             cohortInclusion = cohortInclusion,
             cohortIncStats = cohortIncStats,
             cohortIncResult = cohortIncResult,
             cohortSummaryStats = cohortSummaryStats)
  
  return(ll)
}

# Step 5: Concept Set Diagnostics Targets -------------------------------------

diagnosticsIncludedSourceConcepts <- function(connectionDetails,
                                              cohorts,
                                              cdmDatabaseSchema,
                                              vocabularyDatabaseSchema,
                                              tempEmulationSchema,
                                              minCellCount,
                                              databaseId,
                                              conceptIdTable) {
  ##From Cohort Diagnostics ConceptSets.R Line 521:581
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  #get concept sets
  conceptSets <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-.data$cohortId, -.data$conceptSetId)
  
  
  CohortDiagnostics:::instantiateUniqueConceptSets(
    uniqueConceptSets = uniqueConceptSets,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptSetsTable = "#inst_concept_sets"
  )
  
  sql <- SqlRender::loadRenderTranslateSql(
    "CohortSourceCodes.sql",
    packageName = "CohortDiagnostics",
    dbms = connection@dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    instantiated_concept_sets = "#inst_concept_sets",
    include_source_concept_table = "#inc_src_concepts",
    by_month = FALSE
  )
  DatabaseConnector::executeSql(connection = connection, sql = sql)
  counts <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM @include_source_concept_table;",
      include_source_concept_table = "#inc_src_concepts",
      tempEmulationSchema = tempEmulationSchema,
      snakeCaseToCamelCase = TRUE
    ) %>%
    tidyr::tibble()
  
  counts <- counts %>%
    dplyr::rename(uniqueConceptSetId = .data$conceptSetId) %>%
    dplyr::inner_join(
      conceptSets %>% dplyr::select(
        .data$uniqueConceptSetId,
        .data$cohortId,
        .data$conceptSetId
      ),
      by = "uniqueConceptSetId"
    ) %>%
    dplyr::select(-.data$uniqueConceptSetId) %>%
    dplyr::mutate(databaseId = !!databaseId) %>%
    dplyr::relocate(
      .data$databaseId,
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId
    ) %>%
    dplyr::distinct()
  
  counts <- counts %>%
    dplyr::group_by(
      .data$databaseId,
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId,
      .data$sourceConceptId
    ) %>%
    dplyr::summarise(
      conceptCount = max(.data$conceptCount),
      conceptSubjects = max(.data$conceptSubjects)
    ) %>%
    dplyr::ungroup()
  
  included_source_concept <- CohortDiagnostics:::makeDataExportable(
    x = counts,
    tableName = "included_source_concept",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  
  sql <-
    "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    tempEmulationSchema = tempEmulationSchema,
    include_source_concept_table = "#inc_src_concepts",
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
  
  if (!is.null(conceptIdTable)) {
    sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @include_source_concept_table;

                  INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id
                  FROM @include_source_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      concept_id_table = conceptIdTable,
      include_source_concept_table = "#inc_src_concepts",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  
  return(included_source_concept)
}



diagnosticsIndexEventBreakdown <- function(connectionDetails,
                                           cohorts,
                                           cdmDatabaseSchema,
                                           vocabularyDatabaseSchema,
                                           tempEmulationSchema,
                                           cohortTable,
                                           cohortDatabaseSchema,
                                           minCellCount,
                                           databaseId,
                                           conceptIdTable) {
  
  connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  #get concept sets
  conceptSets <- CohortDiagnostics:::combineConceptSetsFromCohorts(cohorts)
  uniqueConceptSets <-
    conceptSets[!duplicated(conceptSets$uniqueConceptSetId), ] %>%
    dplyr::select(-.data$cohortId, -.data$conceptSetId)
  
  
  CohortDiagnostics:::instantiateUniqueConceptSets(
    uniqueConceptSets = uniqueConceptSets,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    conceptSetsTable = "#inst_concept_sets"
  )
  
  domains <-
    readr::read_csv(
      system.file("csv", "domains.csv", package = "CohortDiagnostics"),
      col_types = readr::cols(),
      guess_max = min(1e7)
    )
  
  runBreakdownIndexEvents <- function(cohort) {
    ParallelLogger::logInfo(
      "- Breaking down index events for cohort '",
      cohort$cohortName,
      "'"
    )
    
    cohortDefinition <-
      RJSONIO::fromJSON(cohort$json, digits = 23)
    primaryCodesetIds <-
      lapply(
        cohortDefinition$PrimaryCriteria$CriteriaList,
        CohortDiagnostics:::getCodeSetIds
      ) %>%
      dplyr::bind_rows()
    if (nrow(primaryCodesetIds) == 0) {
      warning(
        "No primary event criteria concept sets found for cohort id: ",
        cohort$cohortId
      )
      return(tidyr::tibble())
    }
    primaryCodesetIds <- primaryCodesetIds %>% dplyr::filter(.data$domain %in%
                                                               c(domains$domain %>% unique()))
    if (nrow(primaryCodesetIds) == 0) {
      warning(
        "Primary event criteria concept sets found for cohort id: ",
        cohort$cohortId, " but,", "\nnone of the concept sets belong to the supported domains.",
        "\nThe supported domains are:\n", paste(domains$domain,
                                                collapse = ", "
        )
      )
      return(tidyr::tibble())
    }
    primaryCodesetIds <- conceptSets %>%
      dplyr::filter(.data$cohortId %in% cohort$cohortId) %>%
      dplyr::select(codeSetIds = .data$conceptSetId, .data$uniqueConceptSetId) %>%
      dplyr::inner_join(primaryCodesetIds, by = "codeSetIds")
    
    pasteIds <- function(row) {
      return(dplyr::tibble(
        domain = row$domain[1],
        uniqueConceptSetId = paste(row$uniqueConceptSetId, collapse = ", ")
      ))
    }
    primaryCodesetIds <-
      lapply(
        split(primaryCodesetIds, primaryCodesetIds$domain),
        pasteIds
      )
    primaryCodesetIds <- dplyr::bind_rows(primaryCodesetIds)
    
    getCounts <- function(row) {
      domain <- domains[domains$domain == row$domain, ]
      sql <-
        SqlRender::loadRenderTranslateSql(
          "CohortEntryBreakdown.sql",
          packageName = "CohortDiagnostics",
          dbms = connection@dbms,
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = cdmDatabaseSchema,
          vocabulary_database_schema = vocabularyDatabaseSchema,
          cohort_database_schema = cohortDatabaseSchema,
          cohort_table = cohortTable,
          cohort_id = cohort$cohortId,
          domain_table = domain$domainTable,
          domain_start_date = domain$domainStartDate,
          domain_concept_id = domain$domainConceptId,
          domain_source_concept_id = domain$domainSourceConceptId,
          use_source_concept_id = !(is.na(domain$domainSourceConceptId) | is.null(domain$domainSourceConceptId)),
          primary_codeset_ids = row$uniqueConceptSetId,
          concept_set_table = "#inst_concept_sets",
          store = TRUE,
          store_table = "#breakdown"
        )
      
      DatabaseConnector::executeSql(
        connection = connection,
        sql = sql,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      sql <- "SELECT * FROM @store_table;"
      counts <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          store_table = "#breakdown",
          snakeCaseToCamelCase = TRUE
        ) %>%
        tidyr::tibble()
      
      if (!is.null(conceptIdTable)) {
        sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @store_table;"
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          concept_id_table = conceptIdTable,
          store_table = "#breakdown",
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      }
      
      sql <-
        "TRUNCATE TABLE @store_table;\nDROP TABLE @store_table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        store_table = "#breakdown",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      return(counts)
    }
    counts <-
      lapply(split(primaryCodesetIds, 1:nrow(primaryCodesetIds)), getCounts) %>%
      dplyr::bind_rows() %>%
      dplyr::arrange(.data$conceptCount)
    
    if (nrow(counts) > 0) {
      counts$cohortId <- cohort$cohortId
    } else {
      ParallelLogger::logInfo(
        "Index event breakdown results were not returned for: ",
        cohort$cohortId
      )
      return(dplyr::tibble())
    }
    return(counts)
  }
  data <-
    lapply(
      split(cohorts, cohorts$cohortId),
      runBreakdownIndexEvents
    )
  data <- dplyr::bind_rows(data)
  if (nrow(data) > 0) {
    data <- data %>%
      dplyr::mutate(databaseId = !!databaseId)
    data <-
      enforceMinCellValue(data, "conceptCount", minCellCount)
    if ("subjectCount" %in% colnames(data)) {
      data <-
        CohortDiagnostics:::enforceMinCellValue(data, "subjectCount", minCellCount)
    }
  }
  
  data_index_event <- makeDataExportable(
    x = data,
    tableName = "index_event_breakdown",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  return(data_index_event)
}

diagnosticsOrphanConcepts <- function(connectionDetails,
                                      cdmDatabaseSchema,
                                      tempEmulationSchema,
                                      conceptIdTable) {
  # [OPTIMIZATION idea] can we modify the sql to do this for all uniqueConceptSetId in one query using group by?
  data <- list()
  for (i in (1:nrow(uniqueConceptSets))) {
    conceptSet <- uniqueConceptSets[i, ]
    ParallelLogger::logInfo(
      "- Finding orphan concepts for concept set '",
      conceptSet$conceptSetName,
      "'"
    )
    data[[i]] <- .findOrphanConcepts(
      connection = connection,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      useCodesetTable = TRUE,
      codesetId = conceptSet$uniqueConceptSetId,
      conceptCountsDatabaseSchema = conceptCountsDatabaseSchema,
      conceptCountsTable = conceptCountsTable,
      conceptCountsTableIsTemp = conceptCountsTableIsTemp,
      instantiatedCodeSets = "#inst_concept_sets",
      orphanConceptTable = "#orphan_concepts"
    )
    
    if (!is.null(conceptIdTable)) {
      sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @orphan_concept_table;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        concept_id_table = conceptIdTable,
        orphan_concept_table = "#orphan_concepts",
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
    sql <-
      "TRUNCATE TABLE @orphan_concept_table;\nDROP TABLE @orphan_concept_table;"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      orphan_concept_table = "#orphan_concepts",
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  data <- dplyr::bind_rows(data) %>%
    dplyr::distinct() %>%
    dplyr::rename(uniqueConceptSetId = .data$codesetId) %>%
    dplyr::inner_join(
      conceptSets %>%
        dplyr::select(
          .data$uniqueConceptSetId,
          .data$cohortId,
          .data$conceptSetId
        ),
      by = "uniqueConceptSetId"
    ) %>%
    dplyr::select(-.data$uniqueConceptSetId) %>%
    dplyr::select(
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId,
      .data$conceptCount,
      .data$conceptSubjects
    ) %>%
    dplyr::group_by(
      .data$cohortId,
      .data$conceptSetId,
      .data$conceptId
    ) %>%
    dplyr::summarise(
      conceptCount = max(.data$conceptCount),
      conceptSubjects = max(.data$conceptSubjects)
    ) %>%
    dplyr::ungroup()
  data_orphan <- CohortDiagnostics:::makeDataExportable(
    x = data,
    tableName = "orphan_concept",
    minCellCount = minCellCount,
    databaseId = databaseId
  )
  return(data_orphan)
}

# Step 6: Run Time Series ----------------------------------

diagnosticsTimeSeries <- function(connectionDetails,
                                  cdmDatabaseSchema,
                                  cohortDatabaseSchema,
                                  cohortDefinitionSet,
                                  cohortTable,
                                  instantiatedCohorts,
                                  runCohortTimeSeries = TRUE,
                                  runDataSourceTimeSeries = FALSE,
                                  timeSeriesMinDate = as.Date("1980-01-01"),
                                  timeSeriesMaxDate = as.Date(Sys.Date()),
                                  stratifyByGender = TRUE,
                                  stratifyByAgeGroup = TRUE) {
  
 
  cohorts <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)
  
  timeSeries <- CohortDiagnostics::runCohortTimeSeriesDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    runCohortTimeSeries = TRUE,
    runDataSourceTimeSeries = FALSE,
    timeSeriesMinDate = as.Date("1980-01-01"),
    timeSeriesMaxDate = as.Date(Sys.Date()),
    stratifyByGender = TRUE,
    stratifyByAgeGroup = TRUE,
    cohortIds = cohorts$cohortId
  )
  
  return(timeSeries)
  
}


# Step 7: Run Visit Context ------------------------------

diagnosticsVisitContext <- function(connectionDetails,
                                    cdmDatabaseSchema,
                                    cohortDatabaseSchema,
                                    cohortDefinitionSet,
                                    cohortTable,
                                    instantiatedCohorts,
                                    conceptIdTable) {
  
  cohorts <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)

  
  visitContext <- CohortDiagnostics:::getVisitContext(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId,
    conceptIdTable = conceptIdTable
  )
  return(visitContext)
}

# Step 8: Run Incidence Rate --------------------------------
diagnosticsIncidenceRate <- function(connectionDetails,
                                     cdmDatabaseSchema,
                                     cohortDatabaseSchema,
                                     cohortDefinitionSet,
                                     cohortTable,
                                     instantiatedCohorts,
                                     vocabularyDatabaseSchema,
                                     cdmVersion = 5, 
                                     firstOccurrenceOnly = TRUE,
                                     washoutPeriod = 365) {
  
  cohorts <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)
  
  incidenceRate <- CohortDiagnostics:::getIncidenceRate(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    vocabularyDatabaseSchema = vocabularyDatabaseSchema,
    cdmVersion = 5,
    firstOccurrenceOnly = TRUE,
    washoutPeriod = 365,
    cohortId = cohorts$cohortId
  )
  return(incidenceRate)
}



# Step 9/10a create temporal Covariates
temporalCovariates <-  FeatureExtraction::createTemporalCovariateSettings(
  useDemographicsGender = TRUE,
  useDemographicsAge = TRUE,
  useDemographicsAgeGroup = TRUE,
  useDemographicsRace = TRUE,
  useDemographicsEthnicity = TRUE,
  useDemographicsIndexYear = TRUE,
  useDemographicsIndexMonth = TRUE,
  useDemographicsIndexYearMonth = TRUE,
  useDemographicsPriorObservationTime = TRUE,
  useDemographicsPostObservationTime = TRUE,
  useDemographicsTimeInCohort = TRUE,
  useConditionOccurrence = TRUE,
  useProcedureOccurrence = TRUE,
  useDrugEraStart = TRUE,
  useMeasurement = TRUE,
  useConditionEraStart = TRUE,
  useConditionEraOverlap = TRUE,
  useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
  useConditionEraGroupOverlap = TRUE,
  useDrugExposure = FALSE, # leads to too many concept id
  useDrugEraOverlap = FALSE,
  useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
  useDrugEraGroupOverlap = TRUE,
  useObservation = TRUE,
  useDeviceExposure = TRUE,
  useCharlsonIndex = TRUE,
  useDcsi = TRUE,
  useChads2 = TRUE,
  useChads2Vasc = TRUE,
  useHfrs = FALSE,
  temporalStartDays = c(
    # components displayed in cohort characterization
    -9999, # anytime prior
    -365, # long term prior
    -180, # medium term prior
    -30, # short term prior
    
    # components displayed in temporal characterization
    -365, # one year prior to -31
    -30, # 30 day prior not including day 0
    0, # index date only
    1, # 1 day after to day 30
    31,
    -9999 # Any time prior to any time future
  ),
  temporalEndDays = c(
    0, # anytime prior
    0, # long term prior
    0, # medium term prior
    0, # short term prior
    
    # components displayed in temporal characterization
    -31, # one year prior to -31
    -1, # 30 day prior not including day 0
    0, # index date only
    30, # 1 day after to day 30
    365,
    9999 # Any time prior to any time future
  )
)

# Step 9: Run CohortRelationship -------------------------------

diagnosticsCohortRelationship <- function(connectionDetails,
                                          cdmDatabaseSchema,
                                          cohortDatabaseSchema,
                                          cohortTable,
                                          cohortDefinitionSet,
                                          instantiatedCohorts,
                                          temporalCovariateSettings) {
  
  cohorts <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)
  
  cohortRelationship <- CohortDiagnostics::runCohortRelationshipDiagnostics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    targetCohortIds = cohorts$cohortId,
    comparatorCohortIds = cohortDefinitionSet$cohortId,
    relationshipDays = dplyr::tibble(
      startDay = temporalCovariateSettings$temporalStartDays,
      endDay = temporalCovariateSettings$temporalEndDays
    ),
    observationPeriodRelationship = TRUE
  )
  return(cohortRelationship)
}


#Step 10: Run Temporal Cohort Characterization
diagnosticsTemproalCohortCharacterization <- function(connectionDetails,
                                                      cdmDatabaseSchema,
                                                      cohortDatabaseSchema,
                                                      cohortTable,
                                                      cohortDefinitionSet,
                                                      instantiatedCohorts,
                                                      temporalCovariateSettings,
                                                      tempEmulationSchema) {
  
  
  cohorts <- cohortDefinitionSet %>%
    dplyr::filter(.data$cohortId %in% instantiatedCohorts)
  
  characteristics <- getCohortCharacteristics(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTable,
    cohortIds = cohorts$cohortId,
    covariateSettings = temporalCovariateSettings,
    cdmVersion = 5
  )
  return(characteristics)
}