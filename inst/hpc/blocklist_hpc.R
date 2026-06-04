## -- timing/debugging helpers for running blocklist on NCI/Pawsey.
## source(system.file("hpc/blocklist_hpc.R", package = "blocklist")) ## or
## source("https://raw.githubusercontent.com/hypertidy/blocklist/main/inst/hpc/blocklist_hpc.R")
## Each helper times a single layer so a slow run points at a stage, not a wall.
##
## Layers (slowest-suspect first):
##   1. raw H5Dchunk_iter on ONE array  -> .time_iter()   (the 0.11s vs 101s test)
##   2. codec probe on ONE file         -> .time_probe()
##   3. full scan of ONE file           -> run_one()
##   4. scan of N files                 -> run_n()
##   5. whole pipeline (mosaic+write)   -> run_all()

suppressMessages({
  library(blocklist, lib.loc = Sys.getenv("BLOCKLIST_LIB", "~/lib"))
})

## ---- source table ----------------------------------------------------------
## Default = BRAN ocean_temp on NCI /g/data; override dir/var/url for elsewhere.
bran_sources <- function(
    dir = "/g/data/gb6/BRAN/BRAN2023/daily",
    var = "ocean_temp",
    url = "https://thredds.nci.org.au/thredds/fileServer/gb6/BRAN/BRAN2023/daily/") {
  access <- sort(fs::dir_ls(dir, regexp = sprintf("%s.*nc$", var)))
  tibble::tibble(access = as.character(access),
                 public = gsub(paste0(dir, "/"), url, access, fixed = TRUE))
}

## ---- 0. is this file local or remote, and is it cold? ----------------------
## Confirms you're in the FAST regime: access must be a real local path, and a
## first stat() should be quick. Prints the path kind so you never silently scan
## the https URL by accident.
check_access <- function(path) {
  kind <- if (grepl("^[a-z][a-z0-9+.-]*://", path)) "REMOTE-URL (slow regime!)"
          else if (file.exists(path)) "local file" else "MISSING"
  t <- system.time(invisible(file.size(path)))["elapsed"]
  message(sprintf("[access] %s  | stat %.3fs | %s", kind, t, path))
  invisible(kind)
}

## ---- 1. raw chunk-index walk on one array (the latency test) ---------------
## This is the 0.11s (local) vs 101s (remote) measurement, isolated from blocklist.
.time_iter <- function(path, array = "/temp") {
  check_access(path)
  fid <- rhdf5::H5Fopen(path, flags = "H5F_ACC_RDONLY")
  on.exit(rhdf5::H5Fclose(fid), add = TRUE)
  did <- rhdf5::H5Dopen(fid, array)
  on.exit(rhdf5::H5Dclose(did), add = TRUE)
  tt <- system.time(hf <- rhdf5:::H5Dchunk_iter(did))
  n  <- if (is.null(dim(hf$offset))) 1L else nrow(hf$offset)
  message(sprintf("[iter] %s %s | %d chunks | %.2fs elapsed (%.4fs cpu)",
                  basename(path), array, n, tt["elapsed"], tt["user.self"]))
  message(sprintf("[iter] => %.1f ms/chunk  (local ~ <0.01, remote ~ 1+)",
                  1000 * tt["elapsed"] / n))
  invisible(tt)
}

## ---- 2. codec probe on one file (one open, header only) --------------------
.time_probe <- function(path, array = "/temp") {
  tt <- system.time(zc <- blocklist:::.probe_codec(path, array, itemsize = 2L))
  message(sprintf("[probe] %s | %.2fs | contiguous=%s chunks=%s",
                  basename(path), tt["elapsed"], zc$contiguous,
                  paste(zc$chunks, collapse = ",")))
  invisible(zc)
}

## ---- helper: build the mosaic VRT from a source table ----------------------
## Writes one access path per line and passes it via gdal's @filelist
## indirection, so the command line never hits an argument-length limit.
build_vrt <- function(src, vrt = tempfile(fileext = ".vrt"),
                      filelist = tempfile(fileext = ".txt")) {
  writeLines(src$access, filelist)                 # one access path per line
  tt <- system.time(
    st <- system2("gdal", c("mdim", "mosaic", paste0("@", filelist), vrt),
                  stdout = TRUE, stderr = TRUE))
  status <- attr(st, "status")
  if (!is.null(status) && status != 0)
    stop("gdal mdim mosaic failed (status ", status, "):\n", paste(st, collapse = "\n"))
  message(sprintf("[mosaic] %d sources -> %s | %.2fs (filelist %s)",
                  nrow(src), vrt, tt["elapsed"], filelist))
  vrt
}

## ---- 3. one file end to end ------------------------------------------------
run_one <- function(src = bran_sources(), i = 1L,
                    out = file.path(tempdir(), "one.zarr"), array = "/temp") {
  s1 <- src[i, , drop = FALSE]
  check_access(s1$access)
  .time_iter(s1$access, array)
  vrt <- build_vrt(s1)
  tt <- system.time(virtualize_mosaic(vrt, out, sources = s1))
  message(sprintf("[run_one] %s -> %s | %.2fs total",
                  basename(s1$access), out, tt["elapsed"]))
  invisible(out)
}

## ---- 4. n files ------------------------------------------------------------
run_n <- function(src = bran_sources(), n = 10L,
                  out = file.path(tempdir(), "n.zarr")) {
  sn <- src[seq_len(min(n, nrow(src))), , drop = FALSE]
  vrt <- build_vrt(sn)
  tt <- system.time(virtualize_mosaic(vrt, out, sources = sn))
  message(sprintf("[run_n] %d files -> %s | %.2fs total (%.2fs/file)",
                  nrow(sn), out, tt["elapsed"], tt["elapsed"] / nrow(sn)))
  invisible(out)
}

## ---- 5. whole thing (explicit paths, for qsub) -----------------------------
run_all <- function(out, src = bran_sources(), vrt =tempfile(fileext = ".vrt")) {
  message(sprintf("[run_all] %d sources -> %s", nrow(src), out))
  vrt <- build_vrt(src, vrt)
  tt <- system.time(virtualize_mosaic(vrt, out, sources = src))
  message(sprintf("[run_all] DONE %s | %.1fs total (%.2fs/file)",
                  out, tt["elapsed"], tt["elapsed"] / nrow(src)))
  invisible(out)
}
