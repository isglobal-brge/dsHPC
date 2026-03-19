test_that("ds_encode produces B64 prefix for lists", {
  encoded <- dsJobsClient:::.ds_encode(list(a = 1, b = "hello"))
  expect_true(startsWith(encoded, "B64:"))
})

test_that("ds_encode is identity for scalars", {
  expect_equal(dsJobsClient:::.ds_encode("hello"), "hello")
  expect_equal(dsJobsClient:::.ds_encode(42), 42)
})

test_that("ds_encode handles vectors", {
  encoded <- dsJobsClient:::.ds_encode(c("a", "b", "c"))
  expect_true(startsWith(encoded, "B64:"))
})

test_that("generate_symbol has correct format", {
  sym <- dsJobsClient:::.generate_symbol("dsJ")
  expect_true(startsWith(sym, "dsJ."))
  expect_equal(nchar(sym), 10L)
})

test_that("generate_symbol is unique", {
  syms <- replicate(100, dsJobsClient:::.generate_symbol())
  expect_equal(length(unique(syms)), 100L)
})
