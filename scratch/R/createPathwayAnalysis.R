#pathway analysis---------------------


createPathwayTables <- function(connectionDetails, ddlPath, resultsSchema) {
  conn <- DatabaseConnector::connect(connectionDetails = connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))
  
  #run ddl 
  list.files(ddlPath, full.names = TRUE) %>%
    purrr::map(~readr::read_file(.x)) %>%
    purrr::map(~SqlRender::render(sql =.x, results_schema = resultsSchema)) %>%
    purrr::map(~DatabaseConnector::executeSql(connection = conn, sql = .x))
  
  usethis::ui_done()
} 