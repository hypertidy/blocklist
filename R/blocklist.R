# GDAL-first virtualization wrapper
# -----------------------------------------------------------------------------
# Pipeline:  GDAL (mdim mosaic -> logical manifest)
#         -> R/rhdf5 (physical byte refs; stand-in for VirtualiZarr until
#                     `gdal mdim get-refs` does this natively)
#         -> kerchunk-Parquet store (portable sibling + icechunk input)
#
# The mosaic VRT already carries the logical layer: dimension order (C order),
# per-array dtype + BlockSize, CF Scale/Offset/NoDataValue, composed coordinate
# values, and the source->DestSlab mapping. rhdf5 supplies only what the VRT
# cannot: per-chunk byte offset/length and the filter pipeline.
#
# Path model: a sources data.frame(access, public). $access is BOTH the mosaic
# input and the path rhdf5 opens to scan (one physical-access role), and the join
# key back to the VRT (GDAL echoes it verbatim as SourceFilename). $public is the
# durable URL written into the refs. Offsets are copy-invariant, so $access and
# $public may differ (scan the cheap NFS/local path, reference the URL).
#
# kerchunk-Parquet contract (validated against fsspec's own writer):
#   <root>/.zmetadata          {"metadata": {<zkey>: <object>}, "record_size": N}
#   <root>/<var>/refs.<p>.parq path:string, offset:int64, size:int64, raw:binary
#     virtual -> path/offset/size set; inline -> raw set; missing -> all null;
#     padded to record_size; p = flat_chunk_index %/% record_size (C order)
# -----------------------------------------------------------------------------


# default serial; set options(blocklist.workers = N) to fan out over mirai daemons
.vmap <- function(x, f) {
  w <- as.integer(getOption("blocklist.workers", 0L))
  if (is.na(w)) w <- 1L
  if (w < 1L || !requireNamespace("mirai", quietly = TRUE)) return(lapply(x, f))
  mirai::daemons(w)                       # spin up w background R processes
  mirai::everywhere({
    .libPaths(Sys.getenv("BLOCKLIST_LIB", "~/lib"))
    requireNamespace("blocklist")
    requireNamespace("rhdf5")
  })
  print(sprintf("daemons: %i", as.integer(w)))
  on.exit(mirai::daemons(0L), add = TRUE) # tear down
  mirai::mirai_map(x, f)[]                # [] collects, preserves order
}


# ---- type + codec mapping --------------------------------------------------

# VRT <DataType> -> numpy dtype string (OISST is little-endian)
.gdal_to_numpy <- function(t) {
  c(Byte="|u1", Int8="|i1", UInt16="<u2", Int16="<i2", UInt32="<u4",
    Int32="<i4", UInt64="<u8", Int64="<i8", Float32="<f4", Float64="<f8")[[t]]
}

# H5Z filter IDs. Zarr read order = filters then compressor; HDF5 writes
# shuffle-then-deflate, so we emit filters=[shuffle], compressor=zlib.
# Note H5Pget_filter returns only (id, name) -- no cd_values -- which is fine:
# shuffle elementsize == dtype itemsize, and the deflate level is decode-irrelevant.
.H5Z <- c(deflate = 1L, shuffle = 2L, fletcher32 = 3L, szip = 4L)

# ---- Parse the mosaic VRT (the logical manifest) ---------------------------

#' Parse a GDAL multidimensional mosaic VRT
#'
#' Reads the logical manifest produced by `gdal mdim mosaic`: dimension sizes,
#' and for each data array its C-order dimension names, dtype, shape, GDAL block
#' size, CF `scale`/`offset`/`nodata`/`unit`, and the ordered list of sources
#' (each with `SourceFilename`, `SourceArray`, and `DestSlab` offset).
#' Coordinate arrays are returned with their composed values (from the VRT's
#' `InlineValues` or `RegularlySpacedValues`). No data or chunk bytes are read.
#'
#' @param vrt Path to a `.vrt` written by `gdal mdim mosaic`.
#' @return A list with `dim_size` (named integer vector of dimension extents),
#'   `arrays` (named list of data-variable descriptions, each with `dim_names`,
#'   `dtype`, `shape`, `chunks`, `scale`, `offset`, `nodata`, `unit`, `sources`),
#'   and `coords` (named list with `dim`, `dtype`, `values`).
#' @seealso [virtualize_mosaic()]
#' @export
#' @importFrom stats setNames
#' @importFrom xml2 read_xml xml_find_first xml_find_all  xml_attr xml_text
parse_mosaic_vrt <- function(vrt) {
  doc <- read_xml(vrt)
  g   <- xml_find_first(doc, ".//Group")
  dims <- xml_find_all(g, "./Dimension")
  dim_size <- setNames(as.integer(xml_attr(dims, "size")), xml_attr(dims, "name"))

  arrays <- list(); coords <- list()
  for (a in xml_find_all(g, "./Array")) {
    nm   <- xml_attr(a, "name")
    refs <- xml_attr(xml_find_all(a, "./DimensionRef"), "ref")   # C order
    dt   <- .gdal_to_numpy(xml_text(xml_find_first(a, "./DataType")))

    if (length(refs) <= 1L) {                                    # coordinate
      iv <- xml_find_first(a, "./InlineValues")
      rs <- xml_find_first(a, "./RegularlySpacedValues")
      vals <- if (!is.na(iv)) {
        as.numeric(strsplit(trimws(xml_text(iv)), "\\s+")[[1]])
      } else if (!is.na(rs)) {
        st <- as.numeric(xml_attr(rs, "start")); inc <- as.numeric(xml_attr(rs, "increment"))
        n  <- dim_size[[refs]]; st + inc * (seq_len(n) - 1L)
      } else NULL
      coords[[nm]] <- list(name = nm, dim = refs, dtype = dt, values = vals)
    } else {                                                     # data variable
      bs <- as.integer(strsplit(xml_text(xml_find_first(a, "./BlockSize")), ",")[[1]])
      sources <- {
        fns  <- xml_text(xml_find_all(a, "./Source/SourceFilename"))
        arrs <- xml_text(xml_find_all(a, "./Source/SourceArray"))
        offs <- xml_attr(xml_find_all(a, "./Source/DestSlab"), "offset")
        # positional alignment relies on exactly one of each child per Source,
        # which gdal mdim mosaic guarantees; guard anyway
        stopifnot(length(arrs) == length(fns), length(offs) == length(fns))

        # GDAL may emit SourceFilename relative to the VRT; make it absolute
        # before any URL rewrite, else bare basenames leak into the refs.
        rel <- !grepl("^(/|[a-z][a-z0-9+.-]*://)", fns)
        if (any(rel))
          fns[rel] <- normalizePath(file.path(dirname(vrt), fns[rel]), mustWork = FALSE)

        dest <- lapply(strsplit(offs, ",", fixed = TRUE), as.integer)
        mapply(function(f, ar, d) list(filename = f, array = ar, dest = d),
               fns, arrs, dest, SIMPLIFY = FALSE, USE.NAMES = FALSE)
      }

      arrays[[nm]] <- list(
        name = nm, dim_names = refs, dtype = dt,
        shape = as.integer(dim_size[refs]), chunks = bs,
        scale  = .num_or_null(xml_text(xml_find_first(a, "./Scale"))),
        offset = .num_or_null(xml_text(xml_find_first(a, "./Offset"))),
        nodata = .num_or_null(xml_text(xml_find_first(a, "./NoDataValue"))),
        unit   = .chr_or_null(xml_text(xml_find_first(a, "./Unit"))),
        sources = sources)
    }
  }
  list(dim_size = dim_size, arrays = arrays, coords = coords)
}

.num_or_null <- function(x) if (length(x) && nzchar(x)) as.numeric(x) else NULL
.chr_or_null <- function(x) if (length(x) && nzchar(x)) x else NULL

# ---- Two-column source model -----------------------------------------------
# sources: data.frame(access, public).
#   access : the path handed to `gdal mdim mosaic` AND opened by rhdf5 to scan --
#            one physical-access role. == VRT SourceFilename == JOIN KEY.
#   public : the durable URL written into the refs (nothing opens it at build).
# Offsets are copy-invariant, so access and public may differ freely: scan the
# cheap local/NFS path, reference the URL. All-remote -> set both equal.

#' Build a sources table for [virtualize_mosaic()]
#'
#' Pairs each source file's *access* path (what `gdal mdim mosaic` is given and
#' what rhdf5 opens to scan) with its *public* URL (what is written into the
#' reference store). Chunk byte offsets are identical in any byte-identical copy
#' of a file, so the two may differ freely: scan a cheap local or NFS path while
#' referencing a durable remote URL.
#'
#' @param public Character vector of durable URLs to record in the references
#'   (e.g. `https://` or `s3://`), one per source file.
#' @param access Character vector, same order as `public`, of the paths actually
#'   opened during the build (handed to `gdal mdim mosaic` and opened by rhdf5).
#'   Defaults to `public` for the all-remote case.
#' @return A `data.frame` with columns `access` and `public`, one row per source.
#' @seealso [virtualize_mosaic()]
#' @examples
#' \dontrun{
#' mosaic_sources(
#'   public = c("https://host/day01.nc", "https://host/day02.nc"),
#'   access = c("/nfs/day01.nc",        "/nfs/day02.nc"))
#' }
#' @export
mosaic_sources <- function(public, access = public) {
  data.frame(access = access, public = public, stringsAsFactors = FALSE)
}

# Join a VRT SourceFilename back to its row (exact on $access, basename fallback).
.match_source <- function(source_filename, sources) {
  i <- match(source_filename, sources$access)
  if (is.na(i)) i <- match(basename(source_filename), basename(sources$access))
  if (is.na(i)) stop("VRT SourceFilename not in sources$access: ", source_filename)
  sources[i, , drop = FALSE]
}

# ---- One-time per-array probe: codec + layout + chunk shape (NOT in VRT) ----
# Reads the creation plist of ONE representative source. The authoritative chunk
# shape comes from here (H5Pget_chunk), not the VRT BlockSize. scale/offset/
# nodata already came from the VRT. Accepts an open file id or a path.
# All calls are stock rhdf5; H5Dget_offset (contiguous branch) is the only gap.
#' @importFrom rhdf5 H5Fopen H5Dopen H5Dget_create_plist H5Pclose H5Dclose H5Fclose H5Pget_filter H5Pget_layout H5Pget_chunk H5Pget_nfilters
#' @importFrom jsonlite unbox
.probe_codec <- function(fid_or_path, source_array, itemsize) {
  fid <- fid_or_path; opened <- FALSE
  if (is.character(fid_or_path)) { fid <- rhdf5::H5Fopen(fid_or_path, flags = "H5F_ACC_RDONLY"); opened <- TRUE }
  did <- rhdf5::H5Dopen(fid, source_array)
  pid <- rhdf5::H5Dget_create_plist(did)
  on.exit({ rhdf5::H5Pclose(pid); H5Dclose(did); if (opened) H5Fclose(fid) }, add = TRUE)
  contiguous <- !identical(H5Pget_layout(pid), "H5D_CHUNKED")
  chunks <- if (contiguous) NULL else H5Pget_chunk(pid)
  filters <- list()
  for (i in seq_len(H5Pget_nfilters(pid))) {
    f <- H5Pget_filter(pid, i); id <- f[[1L]]
    if (id == .H5Z[["deflate"]]) {
      filters[[length(filters) + 1L]] <- list(id = unbox("zlib"), level = unbox(1L))
    } else if (id == .H5Z[["shuffle"]]) {
      filters[[length(filters) + 1L]] <- list(id = unbox("shuffle"),
                                              elementsize = unbox(as.integer(itemsize)))
    } else if (id == .H5Z[["fletcher32"]]) {
      filters[[length(filters) + 1L]] <- list(id = unbox("fletcher32"))
    } else stop("unsupported HDF5 filter id ", id, " (", f[[2L]], ")")
  }
  list(compressor = NULL, filters = if (length(filters)) filters else NULL,
       contiguous = contiguous, chunks = chunks)
}
# ---- Per-source chunk scan (physical byte refs) ----------------------------
# Wraps fork's H5Dchunk_iter, whose return is:
#   $offset      n_chunks x ndim matrix of chunk-grid coords (element units, C order)
#   $addr        byte address of each chunk in the file
#   $size        compressed byte length of each chunk
#   $filter_mask per-chunk skipped-filter bitmask (0 == all filters applied)
# Returns a data.frame: c1..c{d} (grid coords, 0-based, LOCAL) + offset(=addr) +
# size + path(=ref_path). Caller shifts coords to global by DestSlab. Contiguous
# sources have no B-tree -> one whole-array reference.

#' Extract chunk byte references for one array in one source file
#'
#' Opens `source_array` in `scan_path` read-only and walks its HDF5 chunk index
#' (`H5Dchunk_iter`) to recover, per chunk, the grid coordinate (C order) and
#' the byte address and length of the stored, compressed data. Coordinates are
#' local to the source; the caller shifts them to global position using the
#' mosaic `DestSlab` offset. A non-zero filter mask on any chunk aborts with an
#' error - it means the chunk does not share the array-level codec and cannot be
#' virtualised uniformly.
#'
#' @param scan_path Path opened to read the chunk index (the source's `access`).
#' @param source_array Array name within the file, e.g. `"/anom"`.
#' @param ref_path Durable URL written into the `path` column (the `public` URL).
#' @param A Array description from [parse_mosaic_vrt()] with at least `shape` and
#'   `chunks` (C order); `chunks` must be the authoritative HDF5 storage chunk.
#' @param contiguous Logical; if `TRUE` the dataset is unchunked and a single
#'   whole-array reference is emitted instead of iterating a chunk index.
#' @return A `data.frame`, one row per chunk: `ndim` integer coordinate columns
#'   (`c1`..`cN`), `offset` (byte address), `size` (byte length), `path`.
#' @export
#' @importFrom rhdf5 H5Dget_storage_size
scan_source_chunks <- function(scan_path, source_array, ref_path, A, contiguous) {
  ndim <- length(A$shape)
  if (isTRUE(contiguous)) {
    stop("contiguous storage not yet supported")
    #df <- as.data.frame(as.list(integer(ndim))); names(df) <- paste0("c", seq_len(ndim))
    ## pseudo unused code
    #df$offset <- H5Dget_offset_(scan_path, source_array)        # <- fork: byte addr
    #df$size   <- H5Dget_storage_size(H5Dopen(H5Fopen(scan_path, flags = "H5F_ACC_RDONLY"), source_array))
    #df$path   <- ref_path
    #return(df)
  }

  fid <- H5Fopen(scan_path, flags = "H5F_ACC_RDONLY"); did <- H5Dopen(fid, source_array)
  on.exit({ H5Dclose(did); H5Fclose(fid) }, add = TRUE)
  ck <- rhdf5:::H5Dchunk_iter(did)

  # $offset: element-unit grid coords -> chunk-grid indices (divide by chunk shape)
  coords <- ck$offset
  if (is.null(dim(coords))) coords <- matrix(coords, nrow = 1L)  # single chunk
  chunk_shape <- matrix(A$chunks, nrow = nrow(coords), ncol = ndim, byrow = TRUE)
  cidx <- coords %/% chunk_shape

  bad <- which(ck$filter_mask != 0L)
  if (length(bad)) stop(sprintf(
    "%s @ %s: %d chunk(s) with nonzero filter_mask - codec not uniform; ",
    source_array, scan_path, length(bad)),
    "this source cannot share the array-level codec (the GOES-Shuffle hazard).")

  df <- as.data.frame(cidx); names(df) <- paste0("c", seq_len(ndim))
  df$offset <- as.double(ck$addr)     # byte address in the source file
  df$size   <- as.double(ck$size)
  df$path   <- ref_path
  df
}

# ---- Coordinate attrs (the VRT drops units!) -------------------------------
# Coordinates are HDF5 dimension scales; they carry netCDF4 plumbing attrs
# (CLASS/NAME/REFERENCE_LIST/DIMENSION_LIST/_Netcdf4*) that are not CF metadata
# and that h5readAttributes can't decode (REFERENCE_LIST is COMPOUND -> NA). Drop
# the reserved names and any attr that came back NA; keep real CF (units, etc.).
.read_coord_attrs <- function(scan_path, name) {
  a <- suppressWarnings(rhdf5::h5readAttributes(scan_path, name))
  reserved <- c("CLASS", "NAME", "DIMENSION_LIST", "REFERENCE_LIST",
                "_Netcdf4Dimid", "_Netcdf4Coordinates", "_NCProperties", "_nc3_strict")
  a <- a[setdiff(names(a), reserved)]
  a[!vapply(a, function(x) all(is.na(x)), logical(1L))]
}

.compose_data_attrs <- function(A) {
  at <- list("_ARRAY_DIMENSIONS" = I(A$dim_names))
  if (!is.null(A$scale))  at[["scale_factor"]] <- unbox(A$scale)
  if (!is.null(A$offset)) at[["add_offset"]]   <- unbox(A$offset)
  if (!is.null(A$nodata)) at[["_FillValue"]]   <- unbox(A$nodata)
  if (!is.null(A$unit))   at[["units"]]        <- unbox(A$unit)
  at
}

.values_to_raw <- function(values, dtype) {
  sz <- as.integer(sub("^.{2}", "", dtype))
  endian <- if (startsWith(dtype, ">")) "big" else "little"
  con <- rawConnection(raw(0), "wb")
  writeBin(if (substr(dtype, 2, 2) == "f") as.double(values) else as.integer(values),
           con, size = sz, endian = endian)
  on.exit(close(con)); rawConnectionValue(con)
}

# ---- C-order flat index over a chunk grid ----------------------------------
.flat_index <- function(coords, counts) {
  stride <- rev(cumprod(rev(c(counts[-1], 1L))))
  sum(coords * stride)
}


zarr_fill_value <- function(x) {
  if (is.null(x) || length(x) == 0) return(NULL)
  x <- x[[1]]
  if (is.numeric(x)) {
    if (is.nan(x))      return(unbox("NaN"))                                # before is.na!
    if (is.infinite(x)) return(unbox(if (x > 0) "Infinity" else "-Infinity"))
    if (is.na(x))       return(NULL)                                        # true NA -> no fill
  }
  unbox(x)
}

as_zobject <- function(x) if (length(x)) x else setNames(list(), character(0))
# ---- Write the kerchunk-Parquet store --------------------------------------

#' Writes the fsspec `LazyReferenceMapper` layout (Zarr v2): a root `.zmetadata`
#' holding consolidated array/group metadata plus `record_size`, and a
#' `<var>/refs.<N>.parq` shard per array. Each row is either a chunk reference
#' (`path`/`offset`/`size`) or inline data (`raw`); rows are placed at the
#' C-order flat chunk index and padded to `record_size`. The result is readable
#' by VirtualiZarr (`KerchunkParquetParser`), the GDAL Zarr driver, and any
#' fsspec reference filesystem.
#'
#' @param root Output directory for the store (created, overwriting any existing).
#' @param vars_meta Named list of per-variable metadata; each entry has `shape`,
#'   `chunks`, `dtype`, `compressor`, `filters`, `fill_value`, `dim_names`,
#'   `attrs`.
#' @param ref_tables Named list (matching `vars_meta`) of chunk-reference
#'   `data.frame`s from [scan_source_chunks()]; omit for variables given via
#'   `inline`.
#' @param inline Named list of variables stored inline; each entry has `raw`,
#'   the bytes of a single whole-array chunk.
#' @param record_size Integer chunk references per parquet shard (default 1e5).
#' @param root_attrs Named list of group-level attributes for the root `.zattrs`.
#' @return The store `root`, invisibly.
#' @seealso [virtualize_mosaic()]
#' @export
write_kerchunk_parquet <- function(root, vars_meta, ref_tables,
                                   inline = list(), record_size = 100000L,
                                   root_attrs = list()) {
  if (dir.exists(root)) unlink(root, recursive = TRUE)
  dir.create(root, recursive = TRUE)

  metadata <- list(".zgroup" = list(zarr_format = unbox(2L)),
                   ".zattrs" = as_zobject(root_attrs))

  for (v in names(vars_meta)) {
    info <- vars_meta[[v]]; is_inline <- v %in% names(inline)
    zarray <- list(
      zarr_format = unbox(2L),
      shape  = I(as.integer(info$shape)),
      chunks = if (is_inline) I(as.integer(info$shape)) else I(as.integer(info$chunks)),
      dtype  = unbox(info$dtype),
      compressor = if (is_inline) NULL else info$compressor,
      filters    = if (is_inline) NULL else info$filters,
      fill_value = zarr_fill_value(info$fill_value),
      order = unbox("C"), dimension_separator = unbox("."))
    metadata[[paste0(v, "/.zarray")]] <- zarray
    metadata[[paste0(v, "/.zattrs")]] <- as_zobject(info$attrs)

    counts <- if (is_inline) rep(1L, length(info$shape))
    else as.integer(ceiling(info$shape / info$chunks))
    n_chunks <- prod(counts)
    path <- rep(NA_character_, n_chunks); offset <- numeric(n_chunks)  # double, not int:
    size <- numeric(n_chunks); raw <- vector("list", n_chunks)         # byte offsets > 2^31

    if (is_inline) {
      raw[[1L]] <- inline[[v]]$raw
    } else {

      df <- ref_tables[[v]]; ndim <- length(info$shape)
      coords <- as.matrix(df[, seq_len(ndim), drop = FALSE])   # nrow x ndim, C order
      stride <- rev(cumprod(rev(c(counts[-1], 1L))))           # length ndim
      fi <- as.integer(coords %*% stride) + 1L                 # all flat indices at once
      path[fi]   <- df$path
      offset[fi] <- as.double(df$offset)
      size[fi]   <- as.double(df$size)

    }

    vdir <- file.path(root, v); dir.create(vdir, recursive = TRUE)
    n_part <- as.integer(ceiling(n_chunks / record_size))
    for (p in seq_len(n_part)) {
      idx <- ((p - 1L) * record_size + 1L):min(p * record_size, n_chunks)
      pad <- record_size - length(idx)
      tbl <- arrow::arrow_table(
        path   = c(path[idx],   rep(NA_character_, pad)),
        offset = c(offset[idx], numeric(pad)),
        size   = c(size[idx],   numeric(pad)),
        raw    = arrow::Array$create(c(raw[idx], vector("list", pad)), type = arrow::binary()))
      tbl$offset <- tbl$offset$cast(arrow::int64())   # double -> int64 (exact to 2^53)
      tbl$size   <- tbl$size$cast(arrow::int64())
      arrow::write_parquet(tbl, file.path(vdir, sprintf("refs.%d.parq", p - 1L)))
    }
  }
  zmeta <- list(metadata = metadata, record_size = unbox(as.integer(record_size)))
  writeLines(jsonlite::toJSON(zmeta, auto_unbox = TRUE, null = "null", na = "null"),
             file.path(root, ".zmetadata"))
  invisible(root)
}

# ---- Top-level driver: VRT -> kerchunk-Parquet -----------------------------

#' Virtualise a GDAL mosaic into a kerchunk-Parquet reference store
#'
#' End-to-end driver. Parses a `gdal mdim mosaic` VRT for the logical layout,
#' probes each array's codec and storage chunk shape once via rhdf5, scans every
#' source file for chunk byte references, composes them across files using the
#' VRT `DestSlab` offsets, inlines coordinate variables (values from the VRT, CF
#' attributes read from a source), and writes the store. The output is both a
#' portable reference store and the input to a VirtualiZarr -> Icechunk step.
#'
#' @param vrt Path to a VRT written by `gdal mdim mosaic` over `sources$access`.
#' @param root Output directory for the store.
#' @param sources A `data.frame` with columns `access` and `public`, typically
#'   from [mosaic_sources()]. Joined to the VRT by `access` (the
#'   `SourceFilename`), with a basename fallback.
#' @param record_size Integer chunk references per parquet shard (default 1e5).
#' @return The store `root`, invisibly.
#' @seealso [mosaic_sources()], [parse_mosaic_vrt()], [write_kerchunk_parquet()]
#' @examples
#' \dontrun{
#' src <- mosaic_sources(public = urls, access = nfs_paths)
#' system(sprintf("gdal mdim mosaic %s mosaic.vrt", paste(src$access, collapse = " ")))
#' virtualize_mosaic("mosaic.vrt", "oisst.zarr", sources = src)
#' }
#' @export
virtualize_mosaic <- function(vrt, root, sources, record_size = 100000L) {
  stopifnot(all(c("access", "public") %in% names(sources)))
  m <- parse_mosaic_vrt(vrt)
  vars_meta <- list(); ref_tables <- list(); inline <- list()

  # representative access path (first source of first array) for probe + coords;
  # the rep file holds every array/coord, opened read-only.
  rep_access <- .match_source(m$arrays[[1]]$sources[[1]]$filename, sources)$access

  # data variables: byte refs placed by DestSlab; codec probed once per array
  for (nm in names(m$arrays)) {
    A <- m$arrays[[nm]]
    itemsize <- as.integer(sub("^.{2}", "", A$dtype))
    zc <- .probe_codec(rep_access, A$sources[[1]]$array, itemsize)
    # authoritative chunk shape is the HDF5 storage chunk, not the VRT BlockSize
    A$chunks <- if (zc$contiguous) A$shape else zc$chunks

    scan_one <- function(s) {
      row <- .match_source(s$filename, sources)
      ci  <- scan_source_chunks(row$access, s$array, row$public, A, zc$contiguous)
      shift <- s$dest %/% A$chunks                        # shift INSIDE the unit ->
      for (d in seq_along(A$chunks)) ci[[d]] <- ci[[d]] + shift[d]  # globally-placed rows
      ci
    }
    parts <- .vmap(A$sources, scan_one)
    #str(lapply(parts, function(p) if (inherits(p, "miraiError")) p else dim(p)))  # shapes / errors
    ref_tables[[nm]] <- do.call(rbind, parts)
    vars_meta[[nm]] <- list(shape = A$shape, chunks = A$chunks, dtype = A$dtype,
                            compressor = zc$compressor, filters = zc$filters,
                            fill_value = A$nodata, dim_names = A$dim_names,
                            attrs = .compose_data_attrs(A))
  }

  # coordinates: values from the VRT (already composed), attrs from the rep file
  for (nm in names(m$coords)) {
    C <- m$coords[[nm]]
    attrs <- c(list("_ARRAY_DIMENSIONS" = I(C$dim)), .read_coord_attrs(rep_access, nm))
    vars_meta[[nm]] <- list(shape = length(C$values), chunks = length(C$values),
                            dtype = C$dtype, compressor = NULL, filters = NULL,
                            fill_value = NULL, dim_names = C$dim, attrs = attrs)
    inline[[nm]] <- list(raw = .values_to_raw(C$values, C$dtype))
  }

  write_kerchunk_parquet(root, vars_meta, ref_tables, inline = inline,
                         record_size = record_size)
}

# -----------------------------------------------------------------------------
# src <- mosaic_sources(public = c("https://.../day01.nc", ...),
#                       access = c("/nfsmount/.../day01.nc", ...))  # access=public if remote
# system(sprintf("gdal mdim mosaic %s mosaic.vrt", paste(src$access, collapse = " ")))
# virtualize_mosaic("mosaic.vrt", "oisst_test.zarr", sources = src)
#   # VRT SourceFilename joins back to src$access; $public lands in the refs.
# Then Python:
#   from virtualizarr import open_virtual_dataset
#   from virtualizarr.parsers import KerchunkParquetParser
#   vds = open_virtual_dataset("oisst_test.zarr", parser=KerchunkParquetParser())
# -----------------------------------------------------------------------------
