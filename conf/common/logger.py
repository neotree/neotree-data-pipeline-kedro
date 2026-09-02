import logging
import os

def setup_logger(log_file_path: str, logger_name: str = "validation_logger") -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    file_handler = logging.FileHandler(log_file_path, mode='a')
    file_handler.setLevel(logging.INFO)

    # These logs can contain patient data (see sql_functions.py error paths) --
    # restrict to the owner rather than leaving them at the process umask default.
    try:
        os.chmod(log_file_path, 0o600)
    except OSError:
        pass

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.propagate = False  # Prevent logs from bubbling to root logger (e.g. system logs)

    return logger
