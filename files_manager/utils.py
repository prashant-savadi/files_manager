import hashlib
import logging
import os
import sys

def setup_logger(name="files_manager", log_file=None, level=logging.INFO):
    """Sets up a logger that writes to console and optionally to a file."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    # File handler
    if log_file:
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    return logger

def get_file_hash(filepath, block_size=65536):
    """Calculates SHA256 hash of a file."""
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                sha256.update(data)
        return sha256.hexdigest()
    except Exception as e:
        logging.getLogger("files_manager").error(f"Error calculating hash for {filepath}: {e}")
        return None

def get_file_info(filepath):
    """Returns file size and modification time."""
    try:
        stat = os.stat(filepath)
        return {
            'size': stat.st_size,
            'mtime': stat.st_mtime
        }
    except Exception as e:
        logging.getLogger("files_manager").error(f"Error getting file info for {filepath}: {e}")
        return None

def format_size(size_bytes):
    """Formats bytes into human readable string."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = 0
    p = float(size_bytes)
    import math
    if p > 0:
      i = int(math.floor(math.log(p, 1024)))
    else:
      i = 0
      
    p = round(p / (1024 ** i), 2)
    return "%s %s" % (p, size_name[i])
