# blobcache-recover

Recovery tool for BlobCache that rebuilds the index by scanning segment files and removes corrupt segments.

## When to Use

Use this tool when:
- The BlobCache index is corrupted or missing
- The cache fails to start due to index errors
- You suspect index corruption after an unclean shutdown
- You want to validate and clean up corrupt segment files

## Usage

```bash
# Build the tool
go build ./cmd/blobcache-recover

# Run recovery
./blobcache-recover --recover --path=/path/to/cache
```

## What It Does

1. **Scans all segment files** in the cache directory structure
2. **Validates each segment** by checking:
   - Footer magic number
   - Footer checksum
   - Segment record integrity
   - Blob record positions and sizes
3. **Removes corrupt segments** that fail validation
4. **Rebuilds the index** from scratch using valid segments
5. **Rebuilds the bloom filter** from the recovered index

## Recovery Process

The recovery process is safe and atomic:

1. Creates a temporary recovery index (`<path>_recovery/db`)
2. Scans all segment files and adds valid ones to the recovery index
3. Closes the recovery index to ensure data is flushed
4. Removes the old corrupted index
5. Renames the recovery index to replace the old one
6. Opens the recovered index and rebuilds the bloom filter

If recovery fails at any point, the original index and segment files remain unchanged (except for corrupt segments that were already removed).

## Example

```bash
$ ./blobcache-recover --recover --path=/data/blobcache
Starting recovery for cache at: /data/blobcache
WARNING: This will remove corrupt segment files and rebuild the index.

2025/12/22 08:46:32 INFO starting index recovery path=/data/blobcache
2025/12/22 08:46:32 WARN corrupt or incomplete segment file, removing path=/data/blobcache/segments/0000/123.seg error="invalid footer magic: 0x0"
2025/12/22 08:46:32 INFO segment scan completed valid_segments=42 corrupt_segments=1 total_blobs=1523
2025/12/22 08:46:32 INFO index recovery completed, building bloom filter
2025/12/22 08:46:32 INFO bloom filter populated keys_added=1523
2025/12/22 08:46:32 INFO recovery completed successfully

Recovery completed successfully!
You can now use the cache normally.
```

## Exit Codes

- `0`: Recovery completed successfully
- `1`: Recovery failed (error message printed to stderr)

## Notes

- **Backup**: While the recovery process is designed to be safe, consider backing up your cache directory before running recovery if possible
- **Downtime**: The cache must not be in use during recovery
- **Corrupt segments**: Any segment files that fail validation will be permanently deleted
- **Data loss**: If a segment is corrupt, all blobs in that segment will be lost
