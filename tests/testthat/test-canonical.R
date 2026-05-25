spec_hash <- function(x) {
  digest::digest(jsonlite::toJSON(dsHPC:::.canonicalise_spec(x),
    auto_unbox = TRUE), algo = "sha256", serialize = FALSE)
}

test_that("canonical spec hashing sorts top-level named lists", {
  expect_equal(spec_hash(list(a = 1, b = 2)),
    spec_hash(list(b = 2, a = 1)))
})

test_that("canonical spec hashing preserves positional step order", {
  step_a <- list(type = "run", plane = "artifact", config = list(z = 1, a = 2))
  step_b <- list(plane = "session", type = "emit", config = list(b = 3, a = 4))

  one <- list(steps = list(step_a, step_b))
  two <- list(steps = list(
    list(config = list(a = 2, z = 1), plane = "artifact", type = "run"),
    list(config = list(a = 4, b = 3), type = "emit", plane = "session")))
  reversed <- list(steps = list(step_b, step_a))

  expect_equal(spec_hash(one), spec_hash(two))
  expect_false(identical(spec_hash(one), spec_hash(reversed)))
})

test_that("canonical spec hashing sorts deeply nested named lists", {
  one <- list(outer = list(z = list(c = 3, a = 1), a = list(y = 2, x = 1)))
  two <- list(outer = list(a = list(x = 1, y = 2), z = list(a = 1, c = 3)))

  expect_equal(spec_hash(one), spec_hash(two))
})

test_that("canonical spec hashing sorts named subset before unnamed entries", {
  one <- list(9, b = list(y = 2, x = 1), a = 1)
  two <- list(a = 1, b = list(x = 1, y = 2), 9)
  names(one)[1] <- ""
  names(two)[3] <- ""

  expect_equal(spec_hash(one), spec_hash(two))
  expect_equal(names(dsHPC:::.canonicalise_spec(one)), c("a", "b", ""))
})
