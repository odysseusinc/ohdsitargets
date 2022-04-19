con <- dbConnect(duckdb())
dbGetQuery(con, "SELECT 'Hello, world!'")
dbDisconnect(con, shutdown = TRUE)

con <- dbConnect(duckdb())

data <- data.frame(a = 1:3, b = letters[1:3])

duckdb_register(con, "data", data)
dbReadTable(con, "data")
duckdb_unregister(con, "data")
try(dbReadTable(con, "data"))

dbDisconnect(con)

drv <- duckdb::duckdb()
con_duck <- dbConnect(duckdb::duckdb(), "~/R/test.duckdb")
data <- data.frame(a = 1:3, b = letters[1:3])

duckdb_register(con_duck, "data", data)
dbReadTable(con_duck, "data")
dbDisconnect(con_duck, shutdown = TRUE) #always remember to shutdown when disconnecting from duckdb


#test duckdb load

path <- "~/R/bitbucket/txpathwaysstudytest/diagnostics" #path of previously written diagnostics
ff <- list.files(path, full.names = TRUE, pattern = ".csv")
nn <- tools::file_path_sans_ext(basename(ff))
con_duck <- dbConnect(duckdb::duckdb(), file.path(path, "test.duckdb"))
purrr::map2(nn, ff, ~duckdb::duckdb_read_csv(conn = con_duck, name = .x, files = .y))
dbDisconnect(con_duck, shutdown = TRUE) #always remember to shutdown when disconnecting from duckdb


con_duck <- dbConnect(duckdb::duckdb(), file.path(path, "test.duckdb"))
jj <- dbReadTable(con_duck, "cohort")
dbDisconnect(con_duck, shutdown = TRUE) #always remember to shutdown when disconnecting from duckdb
