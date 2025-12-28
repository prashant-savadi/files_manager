# Files Manager Tool

A powerful, efficient Python CLI tool for managing files. It helps you identify and clean up duplicate files to save space, and synchronize directories efficiently using caching mechanisms.

## Key Features

1.  **Duplicate File Handler**
    *   **High Performance**: Uses **Multiprocessing** for heavy SHA256 hashing and **Multithreading** for fast parallel deletions.
    *   Identifies duplicates based on **Content**, not just name/size.
    *   Generates JSON reports with **Human Readable sizes** (e.g., "2.5 MB").
    *   Safely deletes duplicates (keeping one original).
    *   **Dry Run** mode to preview deletions.

2.  **Directory Synchronizer**
    *   One-way synchronization (Source -> Destination).
    *   **Parallel Scanning**: Uses **Multithreading** to traverse directory structures in parallel and **Multiprocessing** to calculate hashes, drastically reducing scan times for large and nested directory trees.
    *   **Fast Syncing**: Uses **Multithreaded** copying to maximize network/disk I/O.
    *   **Resumable & Robust**: Updates the cache on-the-fly. If the process is interrupted, next run naturally resumes from where it left off.
    *   **Smart Caching**: Tracks files to avoid re-hashing unchanged content.
    *   **Dry Run** mode available.

3.  **Advanced Logging**
    *   Tracks Script Start Time, End Time, and Total Duration.
    *   Creates unique timestamped logs for every run (e.g., `logs/files_manager_20251225_001230.log`).
26: 
27: 4.  **Multi-Language Support**
28:     *   Full support for **UTF-8** filenames and paths (e.g., Hindi, Kannada, etc.).
29:     *   Logs and JSON reports correctly handle non-English characters.

## Requirements

*   Python 3.6+
*   No external dependencies required (uses standard library).

## Usage

Run the tool from the root directory using `python -m files_manager.main`.

### 1. Duplicate Management

**Command:** `duplicates`

| Argument | Description |
| :--- | :--- |
| `-p, --path <dir>` | The directory to scan for duplicates. |
| `-o, --output-json <file>` | (Optional) Save the scan results to a JSON file. |
| `-i, --input-json <file>` | (Optional) Load duplicate data from an existing JSON report instead of scanning. |
| `-d, --delete` | (Optional) Delete the identified duplicate files. |
| `--dry-run` | (Optional) Simulate the deletion process without actually deleting files. |

#### Examples

**Scan a folder and save report:**
```bash
python -m files_manager.main duplicates --path "./photos" --output-json "dupes_report.json"
```

**Delete duplicates based on a previous report (Safe Mode):**
```bash
python -m files_manager.main duplicates --input-json "dupes_report.json" --delete --dry-run
```
*(Check the logs/console output to see what would be deleted)*

**Delete duplicates immediately after scanning:**
```bash
python -m files_manager.main duplicates --path "./downloads" --delete
```

---

### 2. Directory Synchronization

**Command:** `sync`

| Argument | Description |
| :--- | :--- |
| `source` | The source directory path. |
| `dest` | The destination directory path. |
| `-c, --cache <file>` | (Optional) Path to the cache file. Defaults to `sync_cache.json`. |
| `--enable_deep_scan` | (Optional) Enable deep scan (content hash check). Default is shallow scan (names only). |
| `--dry-run` | (Optional) Simulate operations without copying files. |

#### Examples

**Sync two folders with caching:**
```bash
python -m files_manager.main sync "./projects" "D:/backup/projects"
```
*(This creates/updates `sync_cache.json` in the current directory and updates it continuously)*
**Sync with Deep Scan (Check content changes):**
```bash
python -m files_manager.main sync "./projects" "D:/backup/projects" --enable_deep_scan
```
*(Use this when you suspect file contents have changed, not just new files added)*

**Preview sync changes (Dry Run):**
```bash
python -m files_manager.main sync "./projects" "D:/backup/projects" --dry-run
```

## Logs

Execution logs are automatically saved to the `logs/` directory. Each run creates a new file with the pattern `files_manager_YYYYMMDD_HHMMSS.log` containing detailed audit trails and timing information.
