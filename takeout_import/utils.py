import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def log_execution_time(subject="Total"):
    """Decorator to log the execution time of a function."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                elapsed_time = time.time() - start_time
                minutes, seconds = divmod(int(elapsed_time), 60)
                if minutes > 0:
                    logger.info(f"{subject} execution time: {minutes}m {seconds}s")
                else:
                    logger.info(f"{subject} execution time: {seconds}s")
        return wrapper
    return decorator
