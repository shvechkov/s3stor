# s3stor: A Deduplicating S3 Backup Utility

**s3stor** is a command-line tool for backing up files to S3-compatible storage (e.g., AWS S3, Wasabi) with block-based deduplication, point-in-time snapshots, and efficient file management. Designed for reliability and multi-writer safety, it supports syncing files, creating snapshots (with Volume Shadow Copy Service on Windows), listing/restoring files, and cleaning up unused data. Ideal for backup scenarios requiring data integrity and storage efficiency.

## TL;DR (For Impatient Users)

### Quick Setup
```bash
# Install Go (https://go.dev/doc/install)
git clone <your-repo-url>
cd s3stor
go build -o s3stor

# Configure Wasabi (or other S3-compatible storage)
export S3_PROVIDER=wasabi
export S3_BUCKET=your-bucket-name
export S3_REGION=us-east-1
export S3_ENDPOINT=https://s3.us-east-1.wasabisys.com
export AWS_ACCESS_KEY_ID=your-wasabi-access-key
export AWS_SECRET_ACCESS_KEY=your-wasabi-secret-key
```

### Basic Usage
```bash
# Sync a file to S3
./s3stor sync test_out/file1.txt
# Output: Synced file1.txt (123 bytes)

# Create a snapshot
./s3stor snapshot test_out sn001 file1.txt
# Output: Snapshot sandow-sn001 created with 1 files

# List files in snapshot
./s3stor ls sandow-sn001
# Output: Files in snapshot sandow-sn001 (created 2025-07-27T22:50:00Z by sandow):
#         - file1.txt (123 bytes)

# Restore a file from snapshot
./s3stor get sandow-sn001 file1.txt ./restore
# Output: File reconstructed to: ./restore/file1.txt

# Delete a file from global catalog
./s3stor delete file1.txt
# Output: Deleted file: file1.txt
#         Block cleanup completed: 0 blocks deleted
```

Jump to [Usage](#usage) for more examples or [Architecture](#architecture) for how it works.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Examples](#examples)
- [S3 Bucket Structure](#s3-bucket-structure)
- [Locking Mechanism](#locking-mechanism)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Features
- **Block-Based Deduplication**: Splits files into blocks, stores unique blocks by SHA-256 hash, and reuses them across files and snapshots to save storage.
- **Point-in-Time Snapshots**: Creates consistent backups using Volume Shadow Copy Service (VSS) on Windows, with independent file maps for each snapshot.
- **Multi-Writer Safety**: Uses S3-based locking to prevent conflicts when multiple instances (e.g., on different machines) access the same bucket.
- **File Management**:
  - `sync`: Upload files to S3 with deduplication.
  - `ls`: List files in global catalog or snapshots.
  - `get`: Restore files from snapshots or global catalog.
  - `map`: Display block mappings for a file.
  - `snapshot`: Create snapshots of specified files.
  - `delete-snapshot`: Remove snapshots and their metadata.
  - `delete`: Remove files from global catalog with safe block cleanup.
  - `cleanup-blocks`: Remove unreferenced blocks to reclaim storage.
- **S3 Compatibility**: Works with AWS S3, Wasabi, and other S3-compatible providers.
- **Efficient Cleanup**: Safely deletes unreferenced blocks only after checking all file maps (global and snapshot).

## Architecture
`s3stor` organizes data in an S3 bucket using a structured layout, with separate catalogs for global files and snapshots, deduplicated block storage, and a locking mechanism for concurrency.

### Components
1. **Global Catalog (`catalog.json`)**:
   - Stores metadata for files synced via `sync`.
   - Format: JSON array of entries:
     ```json
     [
       {
         "FileName": "file1.txt",
         "FileSize": 123,
         "MapKey": "maps/file1.txt.json"
       },
       {
         "FileName": "d001/f005.txt",
         "FileSize": 456,
         "MapKey": "maps/d001/f005.txt.json"
       }
     ]
     ```
   - `MapKey` points to a file map listing block hashes.

2. **File Maps (`maps/<file_name>.json`)**:
   - For each file in the global catalog, stores a list of SHA-256 block hashes:
     ```json
     {
       "Blocks": ["a1b2c3d4...", "e5f6g7h8..."]
     }
     ```
   - Blocks are stored in `blocks/<hash>`.

3. **Snapshot Catalog (`snapshots/<snapshot_id>/catalog.json`)**:
   - Created by `snapshot`, stores metadata for files in a snapshot (e.g., `sandow-sn001`).
   - Format: JSON object:
     ```json
     {
       "SnapshotID": "sandow-sn001",
       "CreatedBy": "sandow",
       "CreatedAt": "2025-07-27T22:50:00Z",
       "Files": [
         {
           "FileName": "file1.txt",
           "FileSize": 123,
           "MapKey": "snapshots/sandow-sn001/maps/file1.txt.json"
         }
       ]
     }
     ```
   - Independent of global catalog, with separate file maps.

4. **Snapshot File Maps (`snapshots/<snapshot_id>/maps/<file_name>.json`)**:
   - Similar to global file maps, lists block hashes for snapshot files.
   - Ensures snapshots are self-contained, unaffected by global catalog changes.

5. **Block Storage (`blocks/<hash>`)**:
   - Stores unique file blocks, identified by SHA-256 hashes.
   - Deduplication ensures identical blocks are stored only once, referenced by multiple file maps.

6. **Locks (`locks/global/<resource>.lock`)**:
   - S3 objects used for concurrency control (e.g., `locks/global/catalog.lock`, `locks/global/file1.txt.lock`).
   - Prevents race conditions in multi-writer scenarios (e.g., multiple `s3stor` instances).
   - Automatically expire via S3 lifecycle policy (1-day retention).

### Data Flow
- **Sync**:
  1. Read local file, split into blocks, compute SHA-256 hashes.
  2. Upload new blocks to `blocks/<hash>` if not already present.
  3. Create file map (`maps/<file_name>.json`) listing block hashes.
  4. Update `catalog.json` with file metadata.
- **Snapshot**:
  1. Use VSS (Windows) for consistent file access.
  2. Create snapshot catalog (`snapshots/<snapshot_id>/catalog.json`).
  3. Copy or create file maps in `snapshots/<snapshot_id>/maps/`.
  4. Reuse existing blocks in `blocks/<hash>`.
- **Delete**:
  1. Remove file from `catalog.json` and delete its file map.
  2. Clean up unreferenced blocks by checking all file maps (global and snapshot).
- **Get**:
  1. Read file map to get block hashes.
  2. Download blocks from `blocks/<hash>`.
  3. Reconstruct file locally.

### Deduplication
- Files are split into fixed-size blocks (implementation-specific, e.g., 4MB).
- Each block’s SHA-256 hash is computed and stored in `blocks/<hash>`.
- File maps reference these blocks, enabling deduplication across files and snapshots.
- Example: If `file1.txt` and `file2.txt` share a block, it’s stored once in `blocks/a1b2c3d4...` and referenced by both file maps.

## Installation
1. **Install Go**:
   - Download and install Go (version 1.16+): [https://go.dev/doc/install](https://go.dev/doc/install).
2. **Clone Repository**:
   ```bash
   git clone <your-repo-url>
   cd s3stor
   ```
3. **Build**:
   ```bash
   go build -o s3stor
   ```
4. **Verify**:
   ```bash
   ./s3stor
   # Output: Usage: s3stor <sync|ls|get|map|snapshot|delete-snapshot|cleanup-blocks|delete> [args...]
   ```

## Configuration
`s3stor` uses environment variables for S3 configuration. Example for Wasabi:
```bash
export S3_PROVIDER=wasabi
export S3_BUCKET=your-bucket-name
export S3_REGION=us-east-1
export S3_ENDPOINT=https://s3.us-east-1.wasabisys.com
export AWS_ACCESS_KEY_ID=your-wasabi-access-key
export AWS_SECRET_ACCESS_KEY=your-wasabi-secret-key
```

### Required S3 Permissions
Ensure your S3 credentials allow:
```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject", "s3:ListBucket"],
  "Resource": ["arn:aws:s3:::your-bucket-name/*", "arn:aws:s3:::your-bucket-name"]
}
```

### Lock Expiration
Set an S3 lifecycle policy to expire locks after 1 day:
```bash
aws s3api put-bucket-lifecycle-configuration --bucket your-bucket-name --lifecycle-configuration '{
  "Rules": [{
    "ID": "CleanLocks",
    "Status": "Enabled",
    "Filter": {"Prefix": "locks/"},
    "Expiration": {"Days": 1}
  }]
}'
```

## Usage
```bash
s3stor <command> [args...]
```

### Commands
- **sync <file_or_dir>**:
  - Uploads files to S3 with deduplication.
  - Example: `./s3stor sync test_out/file1.txt`
- **ls [snapshot_id]**:
  - Lists files in global catalog or a snapshot.
  - Example: `./s3stor ls sandow-sn001`
- **get <snapshot_id> <file_name> <output_dir>**:
  - Restores a file from a snapshot or global catalog.
  - Example: `./s3stor get sandow-sn001 file1.txt ./restore`
- **map <file_name>**:
  - Displays block mappings for a file in the global catalog.
  - Example: `./s3stor map file1.txt`
- **snapshot <source_dir> <snapshot_id> [file_names...]**:
  - Creates a snapshot of specified files using VSS (Windows).
  - Example: `./s3stor snapshot test_out sn001 file1.txt`
- **delete-snapshot <snapshot_id>**:
  - Deletes a snapshot and its metadata, with block cleanup.
  - Example: `./s3stor delete-snapshot sandow-sn001`
- **cleanup-blocks**:
  - Removes unreferenced blocks after checking all file maps.
  - Example: `./s3stor cleanup-blocks`
- **delete <file_name>**:
  - Removes a file from the global catalog, with block cleanup.
  - Example: `./s3stor delete file1.txt`

## Examples
### 1. Sync Files
Upload a file and a directory to S3:
```bash
./s3stor sync test_out/file1.txt
# Output: Synced file1.txt (123 bytes)

./s3stor sync test_out/d001
# Output: Synced d001/f005.txt (456 bytes)
```

### 2. Create a Snapshot
Create a snapshot of specific files:
```bash
./s3stor snapshot test_out sn001 file1.txt d001/f005.txt
# Output: Snapshot sandow-sn001 created with 2 files
```

### 3. List Files
List files in the global catalog:
```bash
./s3stor ls
# Output: Files in global catalog:
#         - file1.txt (123 bytes)
#         - d001/f005.txt (456 bytes)
```

List files in a snapshot:
```bash
./s3stor ls sandow-sn001
# Output: Files in snapshot sandow-sn001 (created 2025-07-27T22:50:00Z by sandow):
#         - file1.txt (123 bytes)
#         - d001/f005.txt (456 bytes)
```

### 4. Restore a File
Restore a file from a snapshot:
```bash
./s3stor get sandow-sn001 file1.txt ./restore
# Output: File reconstructed to: ./restore/file1.txt
```

### 5. Delete a File
Remove a file from the global catalog:
```bash
./s3stor delete file1.txt
# Output: Deleted file: file1.txt
#         Block cleanup completed: 0 blocks deleted
```

### 6. Delete a Snapshot
Remove a snapshot:
```bash
./s3stor delete-snapshot sandow-sn001
# Output: Deleted snapshot: sandow-sn001
#         Block cleanup completed: 1 blocks deleted
```

### 7. Clean Up Blocks
Manually clean unreferenced blocks:
```bash
./s3stor cleanup-blocks
# Output: Block cleanup completed: 2 blocks deleted
```

## S3 Bucket Structure
After running commands, your bucket (`your-bucket-name`) will have:
```
your-bucket-name/
├── catalog.json
├── maps/
│   ├── file1.txt.json
│   ├── d001/f005.txt.json
├── blocks/
│   ├── a1b2c3d4...
│   ├── e5f6g7h8...
├── snapshots/
│   ├── sandow-sn001/
│   │   ├── catalog.json
│   │   ├── maps/
│   │   │   ├── file1.txt.json
│   │   │   ├── d001/f005.txt.json
├── locks/
│   ├── global/
│   │   ├── catalog.lock
│   │   ├── file1.txt.lock
│   │   ├── cleanup.lock
```

## Locking Mechanism
- **Purpose**: Ensures thread-safety in multi-writer scenarios (e.g., multiple `s3stor` instances on `sandow` or other machines).
- **Implementation**: S3 objects (`locks/global/<resource>.lock`) act as mutexes.
  - Example: `locks/global/catalog.lock` for global catalog updates.
  - `locks/global/file1.txt.lock` for file-specific operations.
- **Acquisition**:
  - Attempts to write lock object with a unique owner (e.g., hostname `sandow`).
  - Retries (default: 3 attempts) if locked by another instance.
- **Expiration**: Locks expire after 1 day via S3 lifecycle policy, preventing deadlocks.
- **Commands Using Locks**:
  - `sync`, `delete`, `snapshot`, `delete-snapshot`, `cleanup-blocks`.

## Troubleshooting
- **Snapshot Creates 0 Files**:
  - **Cause**: Files not found in `source_dir`, VSS access denied, or lock conflicts.
  - **Fix**:
    - Verify files: `ls test_out/file1.txt`.
    - Check VSS permissions (Windows): Run as administrator.
    - List locks: `aws s3 ls s3://your-bucket-name/locks/global/`.
    - Remove stuck locks: `aws s3 rm s3://your-bucket-name/locks/global/file1.txt.lock`.
- **File Not Found in Catalog**:
  - **Cause**: File not synced or deleted.
  - **Fix**: Run `./s3stor ls` to check catalog, then `sync` the file.
- **Lock Acquisition Fails**:
  - **Cause**: Another instance holds the lock.
  - **Fix**: Wait and retry, or increase `maxLockRetries` in code (default: 3).
- **S3 Permission Errors**:
  - **Cause**: Insufficient IAM permissions.
  - **Fix**: Update policy with required actions (`PutObject`, `GetObject`, `DeleteObject`, `ListBucket`).
- **Blocks Not Cleaned Up**:
  - **Cause**: Eventual consistency in S3 or recent snapshot creation.
  - **Fix**: Retry `cleanup-blocks` or add delay (e.g., `time.Sleep(1 * time.Second)` in `deleteFile`).

## Contributing
- Fork the repository and submit pull requests.
- Report issues or suggest features via GitHub Issues.
- Enhance features:
  - Add `--dry-run` for `delete` and `cleanup-blocks`.
  - Support multiple file deletions: `./s3stor delete file1.txt file2.txt`.
  - Parallelize block cleanup for large buckets.
  - Add man page: `man s3stor`.

## License
MIT License (or your preferred license). See [LICENSE](LICENSE) for details.
