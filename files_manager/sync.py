import os
import shutil
import concurrent.futures
import threading
import json
import logging
from files_manager.utils import get_file_hash, get_file_info

logger = logging.getLogger("files_manager")

def scan_directory(directory, cache=None):
    """
    Scans directory and builds a dictionary: { relative_path: {mtime, size, hash} }
    Uses cache to avoid re-hashing if mtime/size haven't changed.
    """
    logger.info(f"Scanning directory: {directory}")
    scan_result = {}
    if not os.path.exists(directory):
        return scan_result

    cache = cache or {}
    files_to_hash = []
    
    # Pass 1: Collect files and check cache
    try:
        for root, _, files in os.walk(directory):
            for filename in files:
                abs_path = os.path.join(root, filename)
                rel_path = os.path.relpath(abs_path, directory) # Key for comparison
                
                info = get_file_info(abs_path)
                if not info:
                    continue

                # Check cache
                cached_file = cache.get(rel_path)
                if (cached_file and 
                    cached_file['size'] == info['size'] and 
                    abs(cached_file['mtime'] - info['mtime']) < 0.001):
                    # Use cached hash
                    scan_result[rel_path] = {
                        'mtime': info['mtime'],
                        'size': info['size'],
                        'hash': cached_file.get('hash')
                    }
                else:
                    # Mark for hashing
                    scan_result[rel_path] = {
                        'mtime': info['mtime'],
                        'size': info['size'],
                        'hash': None # Placeholder
                    }
                    files_to_hash.append((rel_path, abs_path))
                    
    except Exception as e:
        logger.error(f"Error walking directory {directory}: {e}")
        return {}

    # Pass 2: Parallel Hash Calculation for cache misses
    if files_to_hash:
        logger.info(f"Hashing {len(files_to_hash)} files in '{directory}'...")
        abs_paths = [f[1] for f in files_to_hash]
        
        # Use ProcessPoolExecutor for CPU/IO intensive hashing
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # map returns results in order
            hash_results = executor.map(get_file_hash, abs_paths)
            
            for (rel_path, _), file_hash in zip(files_to_hash, hash_results):
                if file_hash:
                    scan_result[rel_path]['hash'] = file_hash
                else:
                    # Failed to hash (e.g. permission/locked)
                    # Remove from result to avoid errors downstream? 
                    # Or keep as None which might force re-copy?
                    # Generally safely remove or keep None if sync handles None.
                    # Sync checks: dest_meta['hash'] != src_meta['hash']
                    # None != None is False, None != "abc" is True.
                    pass

    return scan_result

def sync_directories(source_dir, dest_dir, cache_file=None, dry_run=False):
    """
    Syncs source_dir to dest_dir. 
    Copies files from source that are missing or different in dest.
    """
    if dry_run:
        logger.info(f"Starting DRY RUN sync from {source_dir} to {dest_dir}")
    else:
        logger.info(f"Starting sync from {source_dir} to {dest_dir}")

    if not os.path.exists(source_dir):
        logger.error(f"Source directory does not exist: {source_dir}")
        return

    if not os.path.exists(dest_dir):
        if dry_run:
            logger.info(f"[Dry Run] Would create destination directory: {dest_dir}")
        else:
            try:
                os.makedirs(dest_dir)
                logger.info(f"Created destination directory: {dest_dir}")
            except Exception as e:
                logger.error(f"Failed to create destination directory: {e}")
                return

    # Load cache
    cache_data = {}
    if cache_file and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
            logger.info("Loaded previous sync cache.")
        except Exception as e:
            logger.warning(f"Failed to load cache file: {e}")

    # Separate cache for source and dest
    source_cache = cache_data.get('source', {})
    dest_cache = cache_data.get('dest', {})

    # Scan both directories
    logger.info("Analyzing source directory...")
    current_source_state = scan_directory(source_dir, source_cache)
    
    # For dest, if dry run and dest doesn't exist, it's empty
    if dry_run and not os.path.exists(dest_dir):
        current_dest_state = {}
    else:
        logger.info("Analyzing destination directory...")
        current_dest_state = scan_directory(dest_dir, dest_cache)

    files_copied = 0
    
    cache_lock = threading.Lock()
    
    def save_cache():
        if cache_file:
            with cache_lock:
                new_cache = {
                    'source': current_source_state,
                    'dest': current_dest_state 
                }
                try:
                    with open(cache_file, 'w') as f:
                        json.dump(new_cache, f, indent=4)
                except Exception as e:
                    logger.error(f"Failed to write cache file: {e}")

    files_to_copy = []

    # Compare phase
    for rel_path, src_meta in current_source_state.items():
        dest_meta = current_dest_state.get(rel_path)
        
        reason = ""
        should_copy = False

        if not dest_meta:
            should_copy = True
            reason = "Missing in destination"
        elif dest_meta['hash'] != src_meta['hash']:
            should_copy = True
            reason = "Content mismatch"
        
        if should_copy:
            files_to_copy.append((rel_path, src_meta, reason))

    files_copied_count = 0
    
    def copy_single_file(args):
        rel_path, src_meta, reason = args
        src_abs_path = os.path.join(source_dir, rel_path)
        dest_abs_path = os.path.join(dest_dir, rel_path)
        
        if dry_run:
            logger.info(f"[Dry Run] Would copy: {rel_path} ({reason})")
            return 1

        # Ensure parent dir exists (Thread safe-ish, os.makedirs exists_ok=True helps)
        dest_parent = os.path.dirname(dest_abs_path)
        if not os.path.exists(dest_parent):
            try:
                os.makedirs(dest_parent, exist_ok=True)
            except OSError as e:
                # Might race with other threads creating same dir
                pass

        try:
            shutil.copy2(src_abs_path, dest_abs_path)
            logger.info(f"Copied: {rel_path} ({reason})")
            
            # Update dest state in memory to reflect the copy
            # We use source meta because we just copied it
            # PROTECT SHARED RESOURCE
            with cache_lock:
                current_dest_state[rel_path] = src_meta
            
            # Save cache immediately
            save_cache()
            return 1
            
        except Exception as e:
            logger.error(f"Failed to copy {src_abs_path} to {dest_abs_path}: {e}")
            return 0

    # Execute Copy with Thread Pool
    if files_to_copy:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(copy_single_file, files_to_copy)
            files_copied = sum(results)
    else:
        files_copied = 0

    if dry_run:
        logger.info(f"Dry run complete. Would copy {files_copied} files.")
    else:
        logger.info(f"Sync complete. {files_copied} files copied.")
        
        # Update Cache (Check one last time even though we saved incrementally)
        save_cache()
        if cache_file:
            logger.info(f"Updated cache file: {cache_file}")
