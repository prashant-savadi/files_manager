import os
import shutil
import concurrent.futures
import threading
import json
import logging
from files_manager.utils import get_file_hash, get_file_info, should_ignore

logger = logging.getLogger("files_manager")

def _process_file_node(abs_path, base_dir, cache, deep_scan, ignore_regexes=None):
    """
    Helper to process a single file node. 
    Returns: (rel_path, metadata_dict, file_to_hash_entry_or_None)
    """
    if should_ignore(abs_path, ignore_regexes):
        return None, None, None

    rel_path = os.path.relpath(abs_path, base_dir)
    
    if not deep_scan:
        return rel_path, {}, None

    info = get_file_info(abs_path)
    if not info:
        return None, None, None

    cached_file = cache.get(rel_path)
    # Safely check cache
    if (cached_file and 
        cached_file.get('size') == info['size'] and 
        cached_file.get('mtime') and
        abs(cached_file['mtime'] - info['mtime']) < 0.001):
        
        return rel_path, {
            'mtime': info['mtime'],
            'size': info['size'],
            'hash': cached_file.get('hash')
        }, None
    else:
        return rel_path, {
            'mtime': info['mtime'],
            'size': info['size'],
            'hash': None
        }, (rel_path, abs_path)

def _scan_subtree(args):
    """
    Worker function to scan a subdirectory recursively.
    args: (base_dir, subdir_path, cache, deep_scan, ignore_regexes)
    """
    base_dir, subdir_path, cache, deep_scan, ignore_regexes = args
    scan_result = {}
    files_to_hash = []
    
    try:
        # Check if subdir itself should be ignored
        if should_ignore(subdir_path, ignore_regexes):
            return scan_result, files_to_hash

        for root, _, files in os.walk(subdir_path):
            if should_ignore(root, ignore_regexes):
                continue

            for filename in files:
                abs_path = os.path.join(root, filename)
                rel_val, meta_val, hash_entry = _process_file_node(abs_path, base_dir, cache, deep_scan, ignore_regexes)
                
                if rel_val is not None:
                    scan_result[rel_val] = meta_val
                    if hash_entry:
                        files_to_hash.append(hash_entry)
    except Exception as e:
        logger.error(f"Error scanning subtree {subdir_path}: {e}")
        
    return scan_result, files_to_hash

def scan_directory(directory, cache=None, deep_scan=True, ignore_regexes=None):
    """
    Scans directory and builds a dictionary: { relative_path: {mtime, size, hash} }
    Uses cache to avoid re-hashing if mtime/size haven't changed.
    Uses Multithreading for directory traversal.
    """
    logger.info(f"Scanning directory: {directory}")
    scan_result = {}
    if not os.path.exists(directory):
        return scan_result

    cache = cache or {}
    files_to_hash = []

    # Identify Top-Level Items
    try:
        items = os.listdir(directory)
    except Exception as e:
        logger.error(f"Error listing directory {directory}: {e}")
        return {}

    subdirs = []
    root_files = []

    for item in items:
        abs_path = os.path.join(directory, item)
        if os.path.isdir(abs_path):
            subdirs.append(abs_path)
        elif os.path.isfile(abs_path):
            root_files.append(abs_path)

    # 1. Process Root Files (Sequential)
    for abs_path in root_files:
        rel_val, meta_val, hash_entry = _process_file_node(abs_path, directory, cache, deep_scan, ignore_regexes)
        if rel_val is not None:
            scan_result[rel_val] = meta_val
            if hash_entry:
                files_to_hash.append(hash_entry)

    # 2. Process Subdirectories (Parallel)
    if subdirs:
        # Prepare arguments: (base_dir, subdir_to_walk, cache, deep_scan, ignore_regexes)
        # Note: 'cache' is passed by reference (read-only usage is safe)
        task_args = [(directory, sd, cache, deep_scan, ignore_regexes) for sd in subdirs]
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_results = executor.map(_scan_subtree, task_args)
            
            for res_dict, res_list in future_results:
                scan_result.update(res_dict)
                files_to_hash.extend(res_list)

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

def sync_directories(source_dir, dest_dir, cache_file=None, dry_run=False, deep_scan=False, ignore_patterns=None):
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
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            logger.info("Loaded previous sync cache.")
        except Exception as e:
            logger.warning(f"Failed to load cache file: {e}")

    # Separate cache for source and dest
    source_cache = cache_data.get('source', {})
    dest_cache = cache_data.get('dest', {})

    # Scan both directories
    logger.info(f"Analyzing source directory... (Deep Scan: {deep_scan})")
    current_source_state = scan_directory(source_dir, source_cache, deep_scan=deep_scan, ignore_regexes=ignore_patterns)
    
    # For dest, if dry run and dest doesn't exist, it's empty
    if dry_run and not os.path.exists(dest_dir):
        current_dest_state = {}
    else:
        logger.info("Analyzing destination directory...")
        current_dest_state = scan_directory(dest_dir, dest_cache, deep_scan=deep_scan, ignore_regexes=ignore_patterns)

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
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(new_cache, f, indent=4, ensure_ascii=False)
                except Exception as e:
                    logger.error(f"Failed to write cache file: {e}")

    files_to_copy = []

    # Compare phase
    for rel_path, src_meta in current_source_state.items():
        dest_meta = current_dest_state.get(rel_path)
        
        reason = ""
        should_copy = False

        if dest_meta is None:
            should_copy = True
            reason = "Missing in destination"
        elif deep_scan and (dest_meta.get('hash') != src_meta.get('hash')):
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
