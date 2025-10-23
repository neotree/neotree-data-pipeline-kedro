import logging

def setup_logger(log_file_path: str, logger_name: str = "validation_logger") -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if logger is reused
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path, mode='a')
        file_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.propagate = False  # Prevent logs from bubbling to root logger (e.g. system logs)

    return logger