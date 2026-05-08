# Module: Resource-aware scheduler
#
# dsJobs accepts work into a durable queue, then starts jobs only when their
# declared runner resources fit the local node budget. This is intentionally
# small and embedded: it follows the same "requests / consumable resources"
# model used by production schedulers without making Slurm/Kubernetes mandatory
# for ordinary DataSHIELD installations.

#' @keywords internal
.scheduler_node_budget <- function(settings = .dsjobs_settings()) {
  total_mem <- .scheduler_resolve_memory_mb(settings$node_memory_mb)
  reserve <- as.integer(settings$memory_reserve_mb %||% 0L)
  usable_mem <- if (is.finite(total_mem)) max(256L, total_mem - reserve) else Inf

  cpu <- .scheduler_resolve_cpu_slots(settings$cpu_slots)
  gpu <- .scheduler_gpu_inventory(settings)
  list(
    memory_mb = usable_mem,
    total_memory_mb = total_mem,
    memory_reserve_mb = reserve,
    cpu_slots = cpu,
    gpus = gpu$count,
    gpu_memory_mb = gpu$total_memory_mb,
    gpu_devices = gpu$devices,
    gpu_backend = gpu$backend
  )
}

#' @keywords internal
.scheduler_cell_id <- function(settings = .dsjobs_settings()) {
  value <- settings$cell_id %||% "auto"
  if (is.character(value) && length(value) == 1 &&
      nzchar(value) && !identical(value, "auto")) {
    return(value)
  }
  env <- Sys.getenv("DSJOBS_CELL_ID", unset = "")
  if (nzchar(env)) return(env)
  home <- .dsjobs_home(must_exist = FALSE)
  home_key <- if (!is.null(home) && nzchar(home)) {
    tryCatch(normalizePath(home, mustWork = FALSE), error = function(e) home)
  } else "default"
  paste0("cell_", substr(digest::digest(home_key, algo = "xxhash64"), 1, 12))
}

#' @keywords internal
.scheduler_node_id <- function(settings = .dsjobs_settings()) {
  value <- settings$node_id %||% "auto"
  if (is.character(value) && length(value) == 1 &&
      nzchar(value) && !identical(value, "auto")) {
    return(value)
  }
  env <- Sys.getenv("DSJOBS_NODE_ID", unset = "")
  if (nzchar(env)) return(env)
  host <- Sys.info()[["nodename"]] %||% Sys.getenv("HOSTNAME", unset = "node")
  paste0(gsub("[^A-Za-z0-9_.-]", "_", host), "@", .scheduler_cell_id(settings))
}

#' @keywords internal
.scheduler_worker_id <- function(settings = .dsjobs_settings()) {
  existing <- .dsjobs_env$.worker_id %||% Sys.getenv("DSJOBS_WORKER_ID", unset = "")
  if (nzchar(existing)) return(existing)
  paste(.scheduler_node_id(settings), Sys.getpid(), sep = ":")
}

#' @keywords internal
.scheduler_resolve_memory_mb <- function(value) {
  if (is.numeric(value) && length(value) == 1 && is.finite(value))
    return(as.integer(value))
  if (is.character(value) && length(value) == 1 && !identical(value, "auto")) {
    num <- suppressWarnings(as.numeric(value))
    if (is.finite(num)) return(as.integer(num))
  }

  cgroup_max <- .scheduler_read_cgroup_memory_max()
  if (is.finite(cgroup_max)) return(as.integer(cgroup_max / 1024^2))

  meminfo <- "/proc/meminfo"
  if (file.exists(meminfo)) {
    line <- grep("^MemTotal:", readLines(meminfo, warn = FALSE), value = TRUE)
    if (length(line) == 1) {
      kb <- suppressWarnings(as.numeric(gsub("[^0-9]", "", line)))
      if (is.finite(kb)) return(as.integer(kb / 1024))
    }
  }
  sysctl <- Sys.which("sysctl")
  if (nzchar(sysctl)) {
    out <- tryCatch(system2(sysctl, c("-n", "hw.memsize"),
      stdout = TRUE, stderr = FALSE), error = function(e) character(0))
    bytes <- suppressWarnings(as.numeric(out[1]))
    if (is.finite(bytes)) return(as.integer(bytes / 1024^2))
  }
  as.integer(.dsj_option("fallback_node_memory_mb", 8192L))
}

#' @keywords internal
.scheduler_read_cgroup_memory_max <- function() {
  path <- "/sys/fs/cgroup/memory.max"
  if (!file.exists(path)) return(Inf)
  raw <- tryCatch(readLines(path, n = 1, warn = FALSE), error = function(e) "max")
  if (length(raw) == 0 || identical(raw, "max")) return(Inf)
  val <- suppressWarnings(as.numeric(raw))
  if (!is.finite(val) || val <= 0) Inf else val
}

#' @keywords internal
.scheduler_resolve_cpu_slots <- function(value) {
  if (is.numeric(value) && length(value) == 1 && is.finite(value))
    return(max(1L, as.integer(value)))
  if (is.character(value) && length(value) == 1 && !identical(value, "auto")) {
    num <- suppressWarnings(as.numeric(value))
    if (is.finite(num)) return(max(1L, as.integer(num)))
  }
  cores <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) NA_integer_)
  if (!is.finite(cores) || is.na(cores)) {
    cores <- tryCatch(parallel::detectCores(logical = TRUE), error = function(e) 1L)
  }
  max(1L, as.integer(cores %||% 1L))
}

#' @keywords internal
.scheduler_gpu_inventory <- function(settings = .dsjobs_settings()) {
  opt <- settings$gpu_count
  if (is.numeric(opt) && length(opt) == 1 && is.finite(opt)) {
    count <- max(0L, as.integer(opt))
    return(list(count = count, total_memory_mb = Inf,
                devices = as.character(seq_len(count) - 1L),
                backend = "option"))
  }
  if (is.character(opt) && length(opt) == 1 && !identical(opt, "auto")) {
    num <- suppressWarnings(as.numeric(opt))
    if (is.finite(num)) {
      count <- max(0L, as.integer(num))
      return(list(count = count, total_memory_mb = Inf,
                  devices = as.character(seq_len(count) - 1L),
                  backend = "option"))
    }
  }

  nvidia <- Sys.which("nvidia-smi")
  if (nzchar(nvidia)) {
    out <- tryCatch(system2(nvidia,
      c("--query-gpu=index,memory.total,name",
        "--format=csv,noheader,nounits"),
      stdout = TRUE, stderr = FALSE), error = function(e) character(0))
    if (length(out) > 0) {
      parts <- strsplit(out, ",", fixed = TRUE)
      ids <- vapply(parts, function(x) trimws(x[[1]]), character(1))
      mem <- suppressWarnings(vapply(parts, function(x) as.numeric(trimws(x[[2]])),
                                     numeric(1)))
      return(list(count = length(ids),
                  total_memory_mb = sum(mem[is.finite(mem)]),
                  devices = ids,
                  backend = "nvidia-smi"))
    }
  }

  visible <- Sys.getenv("NVIDIA_VISIBLE_DEVICES", unset = "")
  if (nzchar(visible) && !visible %in% c("void", "none")) {
    devices <- strsplit(visible, ",", fixed = TRUE)[[1]]
    if (identical(visible, "all")) {
      # We know GPU passthrough was requested, but cannot count devices without
      # nvidia-smi. Treat as one schedulable accelerator to stay conservative.
      devices <- "0"
    }
    return(list(count = length(devices), total_memory_mb = Inf,
                devices = devices, backend = "nvidia-visible-devices"))
  }

  cuda <- Sys.getenv("CUDA_VISIBLE_DEVICES", unset = "")
  if (nzchar(cuda) && !cuda %in% c("-1", "none")) {
    devices <- strsplit(cuda, ",", fixed = TRUE)[[1]]
    return(list(count = length(devices), total_memory_mb = Inf,
                devices = devices, backend = "cuda-visible-devices"))
  }

  list(count = 0L, total_memory_mb = 0L, devices = character(0), backend = "none")
}

#' @keywords internal
.scheduler_runner_profile <- function(runner_name, settings = .dsjobs_settings()) {
  if (is.null(runner_name) || is.na(runner_name) || !nzchar(runner_name)) {
    return(list(runner = NA_character_, memory_mb = 0L, cpu_slots = 0L,
                max_concurrent = Inf, concurrency_group = NA_character_))
  }
  cfg <- .load_runner_config(runner_name)
  resources <- cfg$resources %||% list()
  resource_class <- cfg$resource_class %||% "default"

  class_defaults <- switch(resource_class,
    cpu_heavy = list(memory_mb = 4096L, cpu_slots = 2L, max_concurrent = Inf),
    gpu_optional = list(memory_mb = 4096L, cpu_slots = 2L, max_concurrent = Inf,
                        gpus = 0L, optional_gpus = 1L, gpu_memory_mb = 0L,
                        accelerator = "optional"),
    gpu = list(memory_mb = 4096L, cpu_slots = 2L, max_concurrent = Inf,
               gpus = 1L, optional_gpus = 0L, gpu_memory_mb = 0L,
               accelerator = "required"),
    cpu = list(memory_mb = 1024L, cpu_slots = 1L, max_concurrent = Inf),
    list(memory_mb = settings$default_runner_memory_mb,
         cpu_slots = settings$default_runner_cpu_slots,
         max_concurrent = Inf)
  )

  override <- settings$runner_overrides[[runner_name]] %||% list()
  memory_mb <- .scheduler_first_number(
    override$memory_mb, resources$memory_mb, cfg$memory_mb,
    class_defaults$memory_mb)
  cpu_slots <- .scheduler_first_number(
    override$cpu_slots, resources$cpu_slots, cfg$cpu_slots,
    class_defaults$cpu_slots)
  max_concurrent <- .scheduler_first_number(
    override$max_concurrent, resources$max_concurrent, cfg$max_concurrent,
    class_defaults$max_concurrent)
  gpus <- .scheduler_first_number(
    override$gpus, resources$gpus, cfg$gpus, class_defaults$gpus %||% 0L)
  optional_gpus <- .scheduler_first_number(
    override$optional_gpus, resources$optional_gpus, cfg$optional_gpus,
    class_defaults$optional_gpus %||% 0L)
  gpu_memory_mb <- .scheduler_first_number(
    override$gpu_memory_mb, resources$gpu_memory_mb, cfg$gpu_memory_mb,
    class_defaults$gpu_memory_mb %||% 0L)
  accelerator <- override$accelerator %||% resources$accelerator %||%
    cfg$accelerator %||% class_defaults$accelerator %||% "none"
  concurrency_group <- override$concurrency_group %||%
    resources$concurrency_group %||% cfg$concurrency_group %||% runner_name
  cooldown_secs <- .scheduler_first_number(
    override$oom_cooldown_secs, resources$oom_cooldown_secs,
    cfg$oom_cooldown_secs, 900L)

  list(
    runner = runner_name,
    resource_class = resource_class,
    memory_mb = as.integer(memory_mb),
    cpu_slots = as.integer(cpu_slots),
    gpus = as.integer(gpus),
    optional_gpus = as.integer(optional_gpus),
    gpu_memory_mb = as.integer(gpu_memory_mb),
    accelerator = as.character(accelerator),
    max_concurrent = max_concurrent,
    concurrency_group = as.character(concurrency_group),
    oom_cooldown_secs = as.integer(cooldown_secs)
  )
}

#' @keywords internal
.scheduler_first_number <- function(...) {
  vals <- list(...)
  for (v in vals) {
    if (is.null(v)) next
    if (is.character(v) && identical(v, "auto")) next
    num <- suppressWarnings(as.numeric(v))
    if (length(num) > 0 && is.finite(num[1])) return(num[1])
    if (length(num) > 0 && is.infinite(num[1])) return(Inf)
  }
  Inf
}

#' @keywords internal
.scheduler_job_plan <- function(spec, settings = .dsjobs_settings()) {
  steps <- spec$steps %||% list()
  profiles <- lapply(steps, function(step) {
    if (!identical(step$plane %||% .infer_step_plane(step$type), "artifact"))
      return(NULL)
    .scheduler_runner_profile(step$runner, settings)
  })
  profiles <- Filter(Negate(is.null), profiles)
  if (length(profiles) == 0) {
    return(list(memory_mb = 0L, cpu_slots = 0L, runners = character(0),
                gpus = 0L, optional_gpus = 0L, gpu_memory_mb = 0L, groups = character(0),
                profiles = list()))
  }
  list(
    memory_mb = max(vapply(profiles, `[[`, numeric(1), "memory_mb")),
    cpu_slots = max(vapply(profiles, `[[`, numeric(1), "cpu_slots")),
    gpus = max(vapply(profiles, `[[`, numeric(1), "gpus")),
    optional_gpus = max(vapply(profiles, `[[`, numeric(1), "optional_gpus")),
    gpu_memory_mb = max(vapply(profiles, `[[`, numeric(1), "gpu_memory_mb")),
    runners = unique(vapply(profiles, `[[`, character(1), "runner")),
    groups = unique(vapply(profiles, `[[`, character(1), "concurrency_group")),
    profiles = profiles
  )
}

#' @keywords internal
.scheduler_running_usage <- function(db, settings = .dsjobs_settings()) {
  running <- DBI::dbGetQuery(db,
    "SELECT job_id, spec_json FROM jobs WHERE state = 'RUNNING'")
  memory_mb <- 0
  cpu_slots <- 0
  gpus <- 0
  gpu_memory_mb <- 0
  runner_counts <- integer(0)
  group_counts <- integer(0)
  if (nrow(running) > 0) {
    for (i in seq_len(nrow(running))) {
      spec <- tryCatch(jsonlite::fromJSON(running$spec_json[i], simplifyVector = FALSE),
                       error = function(e) NULL)
      if (is.null(spec)) next
      plan <- .scheduler_job_plan(spec, settings)
      memory_mb <- memory_mb + plan$memory_mb
      cpu_slots <- cpu_slots + plan$cpu_slots
      lease <- .scheduler_job_gpu_devices(db, running$job_id[i])
      gpus <- gpus + length(lease)
      gpu_memory_mb <- gpu_memory_mb + plan$gpu_memory_mb
      for (runner in plan$runners) {
        runner_counts[[runner]] <- .scheduler_named_count(runner_counts, runner) + 1L
      }
      for (group in plan$groups) {
        group_counts[[group]] <- .scheduler_named_count(group_counts, group) + 1L
      }
    }
  }
  list(memory_mb = memory_mb, cpu_slots = cpu_slots,
       gpus = gpus, gpu_memory_mb = gpu_memory_mb,
       runners = runner_counts, groups = group_counts,
       running_jobs = nrow(running))
}

#' @keywords internal
.scheduler_can_start_job <- function(db, job_id, spec, settings = .dsjobs_settings()) {
  plan <- .scheduler_job_plan(spec, settings)
  usage <- .scheduler_running_usage(db, settings)
  budget <- .scheduler_node_budget(settings)
  delegated <- .executor_delegates_resources(settings)
  enforce_runner_concurrency <- .executor_enforces_runner_concurrency(settings)

  if (!identical(settings$scheduler, "adaptive")) {
    running_n <- DBI::dbGetQuery(db,
      "SELECT COUNT(*) AS n FROM jobs WHERE state = 'RUNNING'")$n
    return(list(ok = running_n < settings$max_jobs_global,
                reason = if (running_n < settings$max_jobs_global) "ok" else "max_jobs_global",
                plan = plan, usage = usage, budget = budget))
  }

  if (usage$running_jobs >= settings$max_jobs_global)
    return(list(ok = FALSE, reason = "max_jobs_global", plan = plan,
                usage = usage, budget = budget))
  if (!delegated && usage$memory_mb + plan$memory_mb > budget$memory_mb)
    return(list(ok = FALSE, reason = "memory_budget", plan = plan,
                usage = usage, budget = budget))
  if (!delegated && usage$cpu_slots + plan$cpu_slots > budget$cpu_slots)
    return(list(ok = FALSE, reason = "cpu_budget", plan = plan,
                usage = usage, budget = budget))
  if (!delegated && usage$gpus + plan$gpus > budget$gpus)
    return(list(ok = FALSE, reason = "gpu_budget", plan = plan,
                usage = usage, budget = budget))
  if (!delegated && is.finite(budget$gpu_memory_mb) &&
      usage$gpu_memory_mb + plan$gpu_memory_mb >
        max(0L, budget$gpu_memory_mb - settings$gpu_memory_reserve_mb))
    return(list(ok = FALSE, reason = "gpu_memory_budget", plan = plan,
                usage = usage, budget = budget))

  assigned_gpus <- character(0)
  wanted_gpus <- plan$gpus
  optional_gpus <- plan$optional_gpus %||% 0L
  if (!delegated && (wanted_gpus > 0L || optional_gpus > 0L)) {
    available <- .scheduler_available_gpu_devices(db, budget$gpu_devices)
    if (length(available) < wanted_gpus) {
      return(list(ok = FALSE, reason = "gpu_devices", plan = plan,
                  usage = usage, budget = budget))
    }
    take <- wanted_gpus + min(optional_gpus, max(0L, length(available) - wanted_gpus))
    if (take > 0L) assigned_gpus <- available[seq_len(take)]
  }

  cooldown <- .scheduler_active_cooldown(db, plan$runners)
  if (!is.null(cooldown))
    return(list(ok = FALSE, reason = paste0("cooldown:", cooldown$runner),
                plan = plan, usage = usage, budget = budget, cooldown = cooldown))

  if (enforce_runner_concurrency) {
    for (profile in plan$profiles) {
      runner_count <- .scheduler_named_count(usage$runners, profile$runner)
      if (is.finite(profile$max_concurrent) &&
          runner_count >= profile$max_concurrent) {
        return(list(ok = FALSE, reason = paste0("runner_concurrency:", profile$runner),
                    plan = plan, usage = usage, budget = budget))
      }
      group_count <- .scheduler_named_count(usage$groups, profile$concurrency_group)
      if (is.finite(profile$max_concurrent) &&
          group_count >= profile$max_concurrent) {
        return(list(ok = FALSE, reason = paste0("group_concurrency:", profile$concurrency_group),
                    plan = plan, usage = usage, budget = budget))
      }
    }
  }

  list(ok = TRUE, reason = "ok", plan = plan, usage = usage, budget = budget,
       gpu_devices = assigned_gpus)
}

#' @keywords internal
.scheduler_named_count <- function(x, name) {
  if (is.null(x) || length(x) == 0 || is.null(name) || is.na(name) || !nzchar(name))
    return(0L)
  if (!name %in% names(x)) return(0L)
  as.integer(x[[name]])
}

#' @keywords internal
.scheduler_available_gpu_devices <- function(db, devices) {
  if (length(devices) == 0) return(character(0))
  leased <- DBI::dbGetQuery(db,
    "SELECT details_json FROM resource_leases WHERE resource = 'gpu_devices'")
  used <- character(0)
  if (nrow(leased) > 0) {
    for (i in seq_len(nrow(leased))) {
      vals <- tryCatch(jsonlite::fromJSON(leased$details_json[i], simplifyVector = FALSE)$devices,
                       error = function(e) character(0))
      used <- c(used, vals)
    }
  }
  setdiff(as.character(devices), unique(used))
}

#' @keywords internal
.scheduler_acquire_leases <- function(db, job_id, decision) {
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  plan <- decision$plan
  leases <- list(
    memory_mb = list(amount = plan$memory_mb %||% 0L),
    cpu_slots = list(amount = plan$cpu_slots %||% 0L)
  )
  if (length(decision$gpu_devices %||% character(0)) > 0) {
    leases$gpu_devices <- list(amount = length(decision$gpu_devices),
      details = list(devices = decision$gpu_devices))
  }
  for (resource in names(leases)) {
    details <- leases[[resource]]$details %||% list()
    DBI::dbExecute(db,
      "INSERT OR REPLACE INTO resource_leases
        (job_id, resource, amount, details_json, acquired_at)
       VALUES (?, ?, ?, ?, ?)",
      params = list(job_id, resource, leases[[resource]]$amount,
        as.character(jsonlite::toJSON(details, auto_unbox = TRUE)),
        now))
  }
  invisible(TRUE)
}

#' @keywords internal
.scheduler_release_leases <- function(db, job_id) {
  DBI::dbExecute(db, "DELETE FROM resource_leases WHERE job_id = ?",
    params = list(job_id))
  invisible(TRUE)
}

#' @keywords internal
.scheduler_job_gpu_devices <- function(db, job_id) {
  row <- DBI::dbGetQuery(db,
    "SELECT details_json FROM resource_leases
     WHERE job_id = ? AND resource = 'gpu_devices'",
    params = list(job_id))
  if (nrow(row) == 0) return(character(0))
  tryCatch(jsonlite::fromJSON(row$details_json[1], simplifyVector = FALSE)$devices,
           error = function(e) character(0))
}

#' @keywords internal
.scheduler_active_cooldown <- function(db, runners) {
  if (length(runners) == 0) return(NULL)
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  ph <- paste(rep("?", length(runners)), collapse = ", ")
  rows <- DBI::dbGetQuery(db, paste0(
    "SELECT runner, concurrency_group, reason, until, last_exit_code, failure_count
     FROM runner_cooldowns WHERE runner IN (", ph, ") AND until > ?
     ORDER BY until DESC LIMIT 1"),
    params = c(as.list(runners), list(now)))
  if (nrow(rows) == 0) return(NULL)
  as.list(rows[1, ])
}

#' @keywords internal
.scheduler_record_runner_failure <- function(db, runner_name, exit_code, reason = NULL) {
  if (is.null(runner_name) || is.na(runner_name) || !nzchar(runner_name))
    return(invisible(FALSE))
  settings <- .dsjobs_settings()
  profile <- .scheduler_runner_profile(runner_name, settings)
  oom <- .scheduler_is_oom_exit(exit_code)
  if (!oom) return(invisible(FALSE))

  until <- format(Sys.time() + profile$oom_cooldown_secs,
                  "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  old <- DBI::dbGetQuery(db,
    "SELECT failure_count FROM runner_cooldowns WHERE runner = ?",
    params = list(runner_name))
  failures <- if (nrow(old) == 0) 1L else as.integer(old$failure_count[1]) + 1L
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO runner_cooldowns
      (runner, concurrency_group, reason, until, last_exit_code, failure_count, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)",
    params = list(runner_name, profile$concurrency_group,
      reason %||% "oom", until, as.integer(exit_code), failures, now))
  invisible(TRUE)
}

#' @keywords internal
.scheduler_is_oom_exit <- function(exit_code) {
  identical(as.integer(exit_code), -9L) || identical(as.integer(exit_code), 137L)
}

#' @keywords internal
.scheduler_status <- function(db = NULL) {
  close_db <- FALSE
  if (is.null(db)) {
    db <- .db_connect()
    close_db <- TRUE
  }
  if (close_db) on.exit(.db_close(db))
  settings <- .dsjobs_settings()
  list(
    mode = settings$scheduler,
    executor = .executor_backend_status(settings),
    cell = .scheduler_cell_status(db, settings),
    node = .scheduler_node_budget(settings),
    usage = .scheduler_running_usage(db, settings),
    cooldowns = DBI::dbGetQuery(db,
      "SELECT runner, concurrency_group, reason, until, last_exit_code, failure_count
       FROM runner_cooldowns
       WHERE until > ?
       ORDER BY until",
      params = list(format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")))
  )
}

#' @keywords internal
.scheduler_cell_status <- function(db, settings = .dsjobs_settings()) {
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  ttl <- .scheduler_worker_ttl_secs(settings)
  stale_cutoff <- format(Sys.time() - ttl * 2, "%Y-%m-%dT%H:%M:%OS3Z",
                         tz = "UTC")
  lock <- DBI::dbGetQuery(db,
    "SELECT name, holder, acquired_at, heartbeat_at, expires_at
     FROM scheduler_locks WHERE name = 'worker_leader'")
  workers <- DBI::dbGetQuery(db,
    "SELECT worker_id, cell_id, node_id, hostname, pid, state,
            started_at, last_heartbeat, resources_json
     FROM worker_nodes
     WHERE last_heartbeat > ?
     ORDER BY last_heartbeat DESC",
    params = list(stale_cutoff))
  leader <- if (nrow(lock) > 0 && lock$expires_at[1] > now) {
    as.list(lock[1, ])
  } else NULL
  list(
    cell_id = .scheduler_cell_id(settings),
    node_id = .scheduler_node_id(settings),
    leader = leader,
    workers = workers
  )
}

#' @keywords internal
.scheduler_worker_ttl_secs <- function(settings = .dsjobs_settings()) {
  ttl <- suppressWarnings(as.numeric(settings$worker_leader_ttl_secs %||% 30))
  poll <- suppressWarnings(as.numeric(settings$worker_poll_secs %||% 2))
  max(10, ttl, poll * 5)
}

#' @keywords internal
.scheduler_worker_leader <- function(db) {
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  row <- DBI::dbGetQuery(db,
    "SELECT name, holder, acquired_at, heartbeat_at, expires_at
     FROM scheduler_locks
     WHERE name = 'worker_leader' AND expires_at > ?",
    params = list(now))
  if (nrow(row) == 0) return(NULL)
  as.list(row[1, ])
}

#' @keywords internal
.scheduler_has_active_worker <- function(db = NULL) {
  close_db <- FALSE
  if (is.null(db)) {
    db <- .db_connect()
    close_db <- TRUE
  }
  if (close_db) on.exit(.db_close(db))
  !is.null(.scheduler_worker_leader(db))
}

#' @keywords internal
.scheduler_register_worker <- function(db, worker_id = .scheduler_worker_id(),
                                       resources = .scheduler_node_budget()) {
  settings <- .dsjobs_settings()
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  details <- list(
    r_version = paste(R.version$major, R.version$minor, sep = "."),
    pid = Sys.getpid()
  )
  DBI::dbExecute(db,
    "INSERT OR REPLACE INTO worker_nodes
      (worker_id, cell_id, node_id, hostname, pid, state, started_at,
       last_heartbeat, resources_json, details_json)
     VALUES (
       ?, ?, ?, ?, ?, COALESCE((SELECT state FROM worker_nodes WHERE worker_id = ?), 'running'),
       COALESCE((SELECT started_at FROM worker_nodes WHERE worker_id = ?), ?),
       ?, ?, ?
     )",
    params = list(worker_id, .scheduler_cell_id(settings),
      .scheduler_node_id(settings), Sys.info()[["nodename"]] %||% NA_character_,
      as.integer(Sys.getpid()), worker_id, worker_id, now, now,
      as.character(jsonlite::toJSON(resources, auto_unbox = TRUE, null = "null")),
      as.character(jsonlite::toJSON(details, auto_unbox = TRUE, null = "null"))))
  invisible(TRUE)
}

#' @keywords internal
.scheduler_renew_worker_leader <- function(db, worker_id = .scheduler_worker_id(),
                                           resources = .scheduler_node_budget()) {
  settings <- .dsjobs_settings()
  now_time <- Sys.time()
  now <- format(now_time, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  expires <- format(now_time + .scheduler_worker_ttl_secs(settings),
                    "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  .scheduler_register_worker(db, worker_id, resources)

  DBI::dbExecute(db, "BEGIN IMMEDIATE")
  tryCatch({
    current <- DBI::dbGetQuery(db,
      "SELECT holder, expires_at FROM scheduler_locks WHERE name = 'worker_leader'")
    can_hold <- nrow(current) == 0 ||
      identical(current$holder[1], worker_id) ||
      current$expires_at[1] <= now
    if (can_hold) {
      DBI::dbExecute(db,
        "INSERT OR REPLACE INTO scheduler_locks
          (name, holder, acquired_at, heartbeat_at, expires_at)
         VALUES (
          'worker_leader', ?, COALESCE(
            (SELECT acquired_at FROM scheduler_locks
             WHERE name = 'worker_leader' AND holder = ?), ?),
          ?, ?)",
        params = list(worker_id, worker_id, now, now, expires))
    }
    DBI::dbExecute(db, "COMMIT")
  }, error = function(e) {
    tryCatch(DBI::dbExecute(db, "ROLLBACK"), error = function(e2) NULL)
    stop(e)
  })

  leader <- .scheduler_worker_leader(db)
  !is.null(leader) && identical(leader$holder, worker_id)
}

#' @keywords internal
.scheduler_release_worker_leader <- function(db, worker_id = .scheduler_worker_id()) {
  now <- format(Sys.time(), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
  DBI::dbExecute(db,
    "UPDATE worker_nodes SET state = 'stopped', last_heartbeat = ?
     WHERE worker_id = ?",
    params = list(now, worker_id))
  DBI::dbExecute(db,
    "DELETE FROM scheduler_locks WHERE name = 'worker_leader' AND holder = ?",
    params = list(worker_id))
  invisible(TRUE)
}
