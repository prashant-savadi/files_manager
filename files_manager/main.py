import argparse
import os
import sys
import datetime
import logging
from files_manager.utils import setup_logger, compile_ignore_patterns
from files_manager.duplicates import handle_duplicates_task
from files_manager.sync import sync_directories

logo = """
    ______ _ _             __  __                                   
   |  ____(_) |           |  \/  |                                  
   | |__   _| | ___ ___   | \  / | __ _ _ __   __ _  __ _  ___ _ __ 
   |  __| | | |/ _ \/ __| | |\/| |/ _` | '_ \ / _` |/ _` |/ _ \ '__|
   | |    | | |  __/\__ \ | |  | | (_| | | | | (_| | (_| |  __/ |   
   |_|    |_|_|\___||___/ |_|  |_|\__,_|_| |_|\__,_|\__, |\___|_|   
                                                     __/ |          
                                                    |___/           
"""

def main():
    print(logo)
    
    start_time = datetime.datetime.now()
    
    parser = argparse.ArgumentParser(description="Files Manager Tool")
    # Add ignore-patterns as a parent parser argument so it can be added to subcommands easily or just repeated.
    # Actually, argparse requires adding it to each subparser to show up in specific help unless using parents.
    # Let's add it to both explicitly for clarity in help.
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Duplicates Command
    dup_parser = subparsers.add_parser("duplicates", help="Find and handle duplicate files")
    dup_parser.add_argument("--path", "-p", type=str, help="Directory to scan")
    dup_parser.add_argument("--input-json", "-i", type=str, help="Load duplicate data from JSON")
    dup_parser.add_argument("--output-json", "-o", type=str, help="Save duplicate report to JSON (default: out_<timestamp>.json)")
    dup_parser.add_argument("--delete", "-d", action="store_true", help="Delete duplicate files")
    dup_parser.add_argument("--dry-run", action="store_true", help="Simulate deletion without deleting")
    dup_parser.add_argument("--ignore-patterns", type=str, help="Comma separated regex patterns to ignore files/directories")

    # Sync Command
    sync_parser = subparsers.add_parser("sync", help="Synchronize two directories")
    sync_parser.add_argument("source", type=str, help="Source directory")
    sync_parser.add_argument("dest", type=str, help="Destination directory")
    sync_parser.add_argument("--cache", "-c", type=str, default="sync_cache.json", help="Path to cache file (default: sync_cache.json)")
    sync_parser.add_argument("--enable_deep_scan", action="store_true", help="Enable deep scan (hash/metadata check). Default is shallow scan (names only).")
    sync_parser.add_argument("--dry-run", action="store_true", help="Simulate sync without copying")
    sync_parser.add_argument("--ignore-patterns", type=str, help="Comma separated regex patterns to ignore files/directories")

    args = parser.parse_args()

    # Setup Logging
    if not os.path.exists("logs"):
        os.makedirs("logs")
    
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join("logs", f"files_manager_{timestamp}.log")
    logger = setup_logger(log_file=log_file)
    
    logger.info(f"Script Started at: {start_time.isoformat()}")

    # Parse ignore patterns globally (it's in args now)
    ignore_patterns = []
    if hasattr(args, 'ignore_patterns') and args.ignore_patterns:
        logger.info(f"Ignoring patterns: {args.ignore_patterns}")
        ignore_patterns = compile_ignore_patterns(args.ignore_patterns)

    try:
        if args.command == "duplicates":
            if not args.path and not args.input_json:
                print("Error: Either --path or --input-json must be provided.")
                dup_parser.print_help()
                sys.exit(1)
                
            # Default output_json to out_<timestamp>.json if not provided
            output_json = args.output_json
            if not output_json:
                output_json = f"out_{timestamp}.json"

            handle_duplicates_task(
                directory=args.path,
                input_json=args.input_json,
                output_json=output_json,
                delete=args.delete,
                dry_run=args.dry_run,
                ignore_patterns=ignore_patterns
            )

        elif args.command == "sync":
            sync_directories(
                source_dir=args.source,
                dest_dir=args.dest,
                cache_file=args.cache,
                dry_run=args.dry_run,
                deep_scan=args.enable_deep_scan,
                ignore_patterns=ignore_patterns
            )
        else:
            parser.print_help()
            
    finally:
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        logger.info(f"Script Started at: {start_time.isoformat()}")
        logger.info(f"Script Ended at: {end_time.isoformat()}")
        logger.info(f"Total Run Time: {duration}")

if __name__ == "__main__":
    main()
