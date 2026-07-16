## cosima_cj50_harvest.R
##
## Goal: probe the THREDDS catalog for cj50/access-om2/cf-compliant to:
##   1. measure depth to leaf .nc files  → tells you the right `level`
##   2. extract the full URL list        → same as bb_handler_thredds dry-run
##   3. parse filenames → manifest tibble → generate scoped bb_source() calls
##
## Key design: the sweep and bowerbird are complementary, not competing.
##   - sweep  → discover structure, level, variable taxonomy
##   - bowerbird → enumerate + download (it already works, 28k URLs found)
##
## Packages: xml2, tibble, dplyr, stringr (all bowerbird deps already)

library(xml2)
library(tibble)
library(dplyr)
library(stringr)
library(bowerbird)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

THREDDS   <- "https://thredds.nci.org.au/thredds"
CJ50_BASE <- paste0(THREDDS, "/catalog/cj50/access-om2/cf-compliant/access-om2")

THREDDS_NS <- c(
  d1    = "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0",
  xlink = "http://www.w3.org/1999/xlink"
)

# ---------------------------------------------------------------------------
# Low-level XML helpers (thin, no reimplementation of bowerbird internals)
# ---------------------------------------------------------------------------

fetch_xml <- function(url) {
  tryCatch(read_xml(url), error = function(e) {
    message("  [skip] ", basename(url), " — ", conditionMessage(e))
    NULL
  })
}

# Resolve a potentially-relative catalog href against its parent URL
resolve_href <- function(href, base_url) {
  if (grepl("^https?://", href)) href
  else paste0(dirname(base_url), "/", href)
}

catalog_refs <- function(x, base_url) {
  nodes <- xml_find_all(x, ".//d1:catalogRef", THREDDS_NS)
  tibble(
    name = xml_attr(nodes, "xlink:title"),
    url  = vapply(xml_attr(nodes, "xlink:href"),
                  resolve_href, character(1), base_url = base_url)
  )
}

# Leaf datasets with a urlPath (these become fileServer URLs)
catalog_leaves <- function(x) {
  nodes <- xml_find_all(x, ".//d1:dataset[@urlPath]", THREDDS_NS)
  tibble(
    name    = xml_attr(nodes, "name"),
    urlPath = xml_attr(nodes, "urlPath")
  )
}

# ---------------------------------------------------------------------------
# Depth probe: walk one branch to measure catalog depth to .nc leaves
# ---------------------------------------------------------------------------

#' Walk one path from a catalog URL down to the first .nc leaf,
#' returning the depth and the leaf catalog URL.
#'
#' This is the key diagnostic: run it once against the known-working
#' source_url to confirm what `level` actually means for this collection.
probe_depth <- function(url, depth = 0, max_depth = 8, verbose = TRUE) {
  if (verbose) message(strrep("  ", depth), "[", depth, "] ", basename(url))
  x <- fetch_xml(url)
  if (is.null(x)) return(NULL)

  leaves <- catalog_leaves(x)
  nc_leaves <- leaves[str_detect(leaves$name, "\\.nc$"), ]

  if (nrow(nc_leaves) > 0) {
    if (verbose) message(strrep("  ", depth + 1),
                         "-> ", nrow(nc_leaves), " .nc files found here")
    return(list(depth = depth, url = url,
                sample_files = head(nc_leaves$name, 6),
                sample_paths = head(nc_leaves$urlPath, 6)))
  }

  if (depth >= max_depth) return(NULL)

  refs <- catalog_refs(x, url)
  if (nrow(refs) == 0) return(NULL)

  # Follow only the first branch (sufficient to measure depth)
  probe_depth(refs$url[1], depth + 1, max_depth, verbose)
}

# ---------------------------------------------------------------------------
# Full catalog sweep → manifest tibble
# ---------------------------------------------------------------------------

#' Recursively walk the catalog, collecting leaf file metadata.
#' Stops expanding at nodes where .nc files are found (leaf level).
#' Caps fan-out at `max_refs` to avoid enumerating huge output-step lists.
#'
#' @param url       catalog XML URL to start from
#' @param depth     current depth (internal)
#' @param max_depth hard stop on recursion
#' @param max_refs  max catalogRefs to follow at any one level
#' @return tibble with columns: depth, catalog_url, name, urlPath
sweep_catalog <- function(url, depth = 0, max_depth = 8, max_refs = 200) {
  x <- fetch_xml(url)
  if (is.null(x)) return(tibble())

  leaves <- catalog_leaves(x)
  nc_leaves <- filter(leaves, str_detect(name, "\\.nc$"))

  # If we've hit files, return them tagged with provenance
  if (nrow(nc_leaves) > 0) {
    return(mutate(nc_leaves, depth = depth, catalog_url = url))
  }

  if (depth >= max_depth) return(tibble())

  refs <- catalog_refs(x, url)
  if (nrow(refs) == 0) return(tibble())
  refs <- head(refs, max_refs)

  bind_rows(lapply(seq_len(nrow(refs)), function(i) {
    sweep_catalog(refs$url[i], depth + 1, max_depth, max_refs)
  }))
}

# ---------------------------------------------------------------------------
# Filename parser → semantic columns
# ---------------------------------------------------------------------------

# cj50 cf-compliant filenames look like:
#   ocean_month.nc  ocean_daily.nc  ice_month.nc  ocean_scalar.nc
#   ocean_month_z_3d.nc  etc.
# Simpler than raw-output; component and freq are usually in the name.

parse_cf_filename <- function(fname) {
  base <- tools::file_path_sans_ext(basename(fname))
  parts <- str_split_1(base, "_")
  list(
    component = parts[1],
    freq      = if (length(parts) >= 2) parts[2] else NA_character_,
    extra     = if (length(parts) >= 3) paste(parts[-(1:2)], collapse = "_")
                else NA_character_,
    raw       = base
  )
}

# ---------------------------------------------------------------------------
# Main: build manifest from sweep
# ---------------------------------------------------------------------------

#' Run the full sweep and return a tidy manifest tibble.
#' @param base_url  catalog.xml or catalog.html URL to start from
#' @param ...       passed to sweep_catalog (max_depth, max_refs)
build_manifest <- function(
    base_url = paste0(CJ50_BASE, "/v20171212/catalog.xml"),
    ...
) {
  message("Sweeping: ", base_url)
  raw <- sweep_catalog(base_url, ...)

  if (nrow(raw) == 0) {
    message("No .nc files found — check URL and max_depth")
    return(raw)
  }

  # Parse filenames
  parsed <- lapply(raw$name, parse_cf_filename)
  bind_cols(
    raw,
    tibble(
      component    = vapply(parsed, `[[`, character(1), "component"),
      freq         = vapply(parsed, `[[`, character(1), "freq"),
      extra        = vapply(parsed, `[[`, character(1), "extra"),
      fileserver   = paste0(THREDDS, "/fileServer/", raw$urlPath),
      opendap      = paste0(THREDDS, "/dodsC/",      raw$urlPath)
    )
  )
}

# ---------------------------------------------------------------------------
# Manifest → scoped bb_source definitions
# ---------------------------------------------------------------------------

#' Summarise the manifest: one row per (version, component, freq) group.
summarise_manifest <- function(manifest) {
  manifest |>
    mutate(version = str_extract(catalog_url, "v\\d+")) |>
    group_by(version, component, freq) |>
    summarise(
      n_files     = n(),
      extra_types = list(sort(unique(extra[!is.na(extra)]))),
      example_url = first(fileserver),
      .groups     = "drop"
    )
}

#' Build one bb_source for a manifest summary row.
make_bb_source <- function(row, base_url = CJ50_BASE) {
  ver    <- row$version %||% "v20171212"
  comp   <- row$component
  freq   <- row$freq %||% ".*"

  # Tight accept_download regex: component_freq*.nc
  file_re <- sprintf("%s_%s.*\\.nc$", comp, freq)

  src_url <- sprintf("%s/%s/catalog.html", base_url, ver)

  bb_source(
    name        = sprintf("COSIMA cj50 cf-compliant %s %s %s",
                          ver, comp, freq),
    id          = sprintf("cosima-cj50-cf-%s-%s-%s", ver, comp, freq),
    description = sprintf(
      "COSIMA ACCESS-OM2 cf-compliant output, %s component, %s frequency (%d files)",
      comp, freq, row$n_files
    ),
    doc_url     = "https://cosima.org.au",
    source_url  = src_url,
    method      = list(
      "bb_handler_thredds",
      level           = row$depth_to_files %||% 4L,
      accept_download = file_re
    ),
    citation    = paste(
      "Kiss et al. (2020) doi:10.5194/gmd-13-401-2020;",
      "COSIMA doi:10.25914/5f48874f65b82"
    ),
    license     = "CC BY-NC-ND 4.0",
    data_group  = "ocean-model"
  )
}

#' Generate all bb_source definitions from a manifest.
manifest_to_bb_sources <- function(manifest) {
  # Attach the median leaf depth to each group (needed for `level`)
  with_depth <- manifest |>
    mutate(version = str_extract(catalog_url, "v\\d+")) |>
    group_by(version, component, freq) |>
    mutate(depth_to_files = median(depth, na.rm = TRUE)) |>
    ungroup()

  summary <- summarise_manifest(manifest) |>
    left_join(
      distinct(with_depth, version, component, freq, depth_to_files),
      by = c("version", "component", "freq")
    )

  sources <- lapply(seq_len(nrow(summary)), function(i) {
    tryCatch(make_bb_source(summary[i, ]),
             error = function(e) { message("skip row ", i, ": ", e$message); NULL })
  })

  nms <- with(summary, sprintf("%s/%s/%s", version %||% "?", component, freq))
  Filter(Negate(is.null), setNames(sources, nms))
}

# ---------------------------------------------------------------------------
# Diagnostic: check why file = NA (the actual problem you hit)
# ---------------------------------------------------------------------------

#' Probe a single fileServer URL to see what content-type THREDDS returns.
#' file = NA in bb_sync usually means the server returned HTML (redirect,
#' login wall, or virtual dataset) instead of the expected binary file.
check_url_response <- function(url) {
  resp <- httr::HEAD(url)
  list(
    status  = httr::status_code(resp),
    ctype   = httr::headers(resp)[["content-type"]],
    url_out = resp$url   # reveals redirects
  )
}

#' Given the manifest, spot-check a sample of fileServer URLs to classify
#' response types — distinguishes real files from virtual/redirect hits.
audit_manifest_urls <- function(manifest, n = 10) {
  sample_urls <- sample(manifest$fileserver, min(n, nrow(manifest)))
  results <- lapply(sample_urls, function(u) {
    r <- tryCatch(check_url_response(u), error = function(e)
      list(status = NA, ctype = NA, url_out = NA))
    tibble(url = u, status = r$status, content_type = r$ctype,
           final_url = r$url_out)
  })
  bind_rows(results)
}

# ---------------------------------------------------------------------------
# NULL-coalescing operator (if not loaded via rlang/purrr)
# ---------------------------------------------------------------------------
`%||%` <- function(a, b) if (!is.null(a) && length(a) > 0 && !is.na(a[1])) a else b

# ---------------------------------------------------------------------------
# Interactive workflow
# ---------------------------------------------------------------------------
if (FALSE) {

  ## ---- Step 0: confirm the catalog entry point ----
  # Your working source_url was:
  base_url <- "https://thredds.nci.org.au/thredds/catalog/cj50/access-om2/cf-compliant/access-om2/v20171212/catalog.html"
  # Change .html → .xml for xml2
  xml_url  <- sub("catalog\\.html$", "catalog.xml", base_url)

  ## ---- Step 1: probe depth (fast, follows one branch only) ----
  depth_info <- probe_depth(xml_url)
  # depth_info$depth    → tells you what `level` should be in bb_source
  # depth_info$sample_files → confirms filename patterns

  ## ---- Step 2: diagnose the file = NA problem ----
  # Build a one-off source at the level you found, dry_run to get URLs
  test_src <- bb_source(
    name = "test", id = "test",
    source_url = base_url,
    method = list("bb_handler_thredds",
                  level = depth_info$depth,
                  accept_download = "\\.nc$"),
    citation = "", license = ""
  )
  cfg  <- bb_config(local_file_root = tempdir())
  cfg  <- bb_add(cfg, test_src)
  # Note: bb_sync dry_run not a param — use verbose + check status tibble
  # Instead: grab URLs from the manifest and HEAD them directly:
  sample_paths <- depth_info$sample_paths
  sample_fs    <- paste0(THREDDS, "/fileServer/", sample_paths)
  audit        <- lapply(sample_fs, check_url_response)
  # If content_type is "text/html" → virtual/redirect issue
  # If status is 401/403 → auth required (need Earthdata / NCI login)
  # If status is 200 and content_type is application/x-netcdf → real file

  ## ---- Step 3: full sweep → manifest ----
  manifest <- build_manifest(xml_url)

  # What did we find?
  manifest |>
    count(component, freq, sort = TRUE)

  ## ---- Step 4: audit a sample of URLs ----
  url_audit <- audit_manifest_urls(manifest, n = 20)
  url_audit |> count(status, content_type)
  # If most are 200 + netcdf/HDF5 → real files, bowerbird should work
  # If 200 + text/html → virtual aggregations, need different approach

  ## ---- Step 5: generate scoped bb_source definitions ----
  sources <- manifest_to_bb_sources(manifest)
  names(sources)

  ## ---- Step 6: run a real (non-dry) sync on one small source ----
  # e.g. scalar diagnostics — typically one file per output step, tiny
  scalar_src <- sources[["v20171212/ocean/scalar"]]
  cfg <- bb_config(local_file_root = "~/data/cosima")
  cfg <- bb_add(cfg, scalar_src)
  bb_sync(cfg, verbose = TRUE)

  ## ---- Step 7: inspect a sample file with vapour ----
  library(vapour)
  # Use OPeNDAP URL to avoid download entirely:
  test_opendap <- manifest$opendap[1]
  vapour::vapour_sds_names(test_opendap)
  # or fileServer URL:
  vapour::vapour_raster_info(manifest$fileserver[1])
}
