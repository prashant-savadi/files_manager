import os
import json
import logging
import concurrent.futures
from collections import defaultdict
from files_manager.utils import get_file_hash, get_file_info, format_size

logger = logging.getLogger("files_manager")

def find_duplicates(directory):
    """
    Scans a directory recursively to find duplicate files.
    Returns a list of dictionaries representing duplicate groups.
    """
    logger.info(f"Starting duplicate scan in: {directory}")
    if not os.path.exists(directory):
        logger.error(f"Directory not found: {directory}")
        return []

    # Phase 1: Group by size (fast initial filter)
    size_groups = defaultdict(list)
    try:
        for root, _, files in os.walk(directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                info = get_file_info(filepath)
                if info:
                    size_groups[info['size']].append(filepath)
    except Exception as e:
        logger.error(f"Error walking directory {directory}: {e}")
        return []

    # Phase 2: Group by hash for files with same size
    duplicates_report = []
    
    # Filter out unique sizes immediately
    potential_duplicates = {k: v for k, v in size_groups.items() if len(v) > 1}
    
    total_scanning = len(potential_duplicates)
    logger.info(f"Found {total_scanning} groups of files with identical sizes. Hashing content...")

    # Prepare list for parallel hashing
    files_to_hash = []
    file_map = defaultdict(list) # file_path -> size
    
    for size, file_list in potential_duplicates.items():
        for filepath in file_list:
            files_to_hash.append(filepath)
            file_map[filepath] = size

    # Mapping path -> hash
    path_hash_map = {}
    
    # Use Multiprocessing for CPU/IO heavy hashing
    if files_to_hash:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            # Map returns results in order
            results = executor.map(get_file_hash, files_to_hash)
            
            for filepath, file_hash in zip(files_to_hash, results):
                if file_hash:
                    path_hash_map[filepath] = file_hash

    # Regroup by size then hash
    for size, file_list in potential_duplicates.items():
        hash_groups = defaultdict(list)
        for filepath in file_list:
            file_hash = path_hash_map.get(filepath)
            if file_hash:
                hash_groups[file_hash].append(filepath)
        
        # Determine duplicates from hash groups
        for file_hash, paths in hash_groups.items():
            if len(paths) > 1:
                # Heuristic: Shortest path is "original", or simply the first one
                # Sorting by length of path might favor root files over nested ones
                paths.sort(key=lambda x: (len(x), x))
                main_file = paths[0]
                duplicate_files = paths[1:]
                
                duplicates_report.append({
                    "main_file": main_file,
                    "duplicates": duplicate_files,
                    "hash": file_hash,
                    "size_per_file": size,
                    "wasted_size": size * len(duplicate_files)
                })

    logger.info(f"Scan complete. Found {len(duplicates_report)} sets of duplicates.")
    return duplicates_report

def calculate_wasted_space(duplicates_data):
    total_wasted = sum(item.get("wasted_size", 0) for item in duplicates_data)
    return total_wasted

def delete_duplicates(duplicates_data, dry_run=False):
    """
    Deletes duplicate files listed in the data.
    """
    if dry_run:
        logger.info("DRY RUN: No files will be deleted.")
    else:
        logger.info("Starting deletion of duplicates...")
    
    deleted_count = 0
    space_freed = 0
    
    all_dupes = []
    for item in duplicates_data:
        for dup_path in item["duplicates"]:
            all_dupes.append((dup_path, item["size_per_file"]))

    def delete_single_file(args):
        dup_path, size = args
        try:
            if os.path.exists(dup_path):
                if dry_run:
                    logger.info(f"[Dry Run] Would delete: {dup_path}")
                    return (1, size)
                else:
                    os.remove(dup_path)
                    logger.info(f"Deleted duplicate: {dup_path}")
                    return (1, size)
            else:
                 logger.warning(f"File not found (already deleted?): {dup_path}")
                 return (0, 0)
        except Exception as e:
            logger.error(f"Failed to delete {dup_path}: {e}")
            return (0, 0)

    # Use ThreadPool for IO bound deletion
    if all_dupes:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(delete_single_file, all_dupes)
            
            for count, freed in results:
                deleted_count += count
                space_freed += freed
                
    if dry_run:
        logger.info(f"[Dry Run] Would delete {deleted_count} files. Would free {format_size(space_freed)}.")
    else:
        logger.info(f"Deletion complete. Deleted {deleted_count} files. Freed {format_size(space_freed)}.")
    return deleted_count, space_freed

def handle_duplicates_task(directory=None, input_json=None, output_json=None, delete=False, dry_run=False):
    """
    Main handler for duplicacy task.
    """
    duplicates_data = []

    # Logic: Load from JSON if provided, else scan directory
    if input_json:
        logger.info(f"Loading duplicate data from {input_json}")
        try:
            with open(input_json, 'r') as f:
                duplicates_data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load input JSON {input_json}: {e}")
            return
    elif directory:
        duplicates_data = find_duplicates(directory)
    else:
        logger.error("No directory or input JSON provided.")
        return

    # Calculate stats
    total_wasted = calculate_wasted_space(duplicates_data)
    logger.info(f"Total separate duplicate files: {sum(len(d['duplicates']) for d in duplicates_data)}")
    logger.info(f"Total potential wasted space: {format_size(total_wasted)}")

    # Output JSON if requested
    if output_json:
        try:
            with open(output_json, 'w') as f:
                json.dump(duplicates_data, f, indent=4)
            logger.info(f"Report saved to {output_json}")
        except Exception as e:
            logger.error(f"Failed to save output JSON {output_json}: {e}")

    # Delete if requested
    if delete:
        delete_duplicates(duplicates_data, dry_run=dry_run)
