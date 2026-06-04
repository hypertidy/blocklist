# blocklist 0.0.1

Renamed and transferred from `vrefs` (mdsumner/vrefs) to `blocklist`
(hypertidy/blocklist).

## Core functionality

* `mosaic_sources()` constructs a sources table with `access` and `public`
  path columns, separating the scan-time path from the durable reference URL.

* `parse_mosaic_vrt()` parses a GDAL multidimensional VRT produced by
  `gdal mdim mosaic`, extracting the logical array layout and
  `source -> DestSlab` mapping.

* `scan_source_chunks()` walks the HDF5 chunk index of a single source file
  via `rhdf5::H5Dchunk_iter`, returning block byte offsets and sizes as a
  flat table. Codec and storage chunk shape are read from the HDF5 creation
  property list.

* `write_kerchunk_parquet()` writes a Zarr v2 kerchunk-Parquet store:
  `.zmetadata` plus per-variable `refs.<N>.parq` shards in fsspec
  `LazyReferenceMapper` layout, readable by VirtualiZarr, the GDAL Zarr
  driver, and any fsspec reference filesystem.

* `virtualize_mosaic()` orchestrates the full pipeline: parses the VRT,
  scans each source for block references, and writes the reference store.
  Block references are computed per-source with no shared state, suitable
  for parallel execution via `mirai` or `future`.

## Design notes

* Block byte references are a flat table - `(path, offset, size, raw)`, 
ready for independent and general usage.

* `access`/`public` path duality: scan a local or NFS copy, write remote
  URLs into the references. Offsets are copy-invariant for byte-identical
  files.

* Codec homogeneity is a hard boundary: a changed filter mask or
  `scale`/`offset` across files means the span cannot be one Zarr array.
