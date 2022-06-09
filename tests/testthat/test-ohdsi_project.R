test_that("create_ohdsitargets_project works", {
  path <- file.path(tempdir(), "example")
  create_ohdsitargets_project(path)
  
  expect_true(file.exists(file.path(path, "example.Rproj")))
})
