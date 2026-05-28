import logging

def setup_logging(config: dict) -> logging.Logger:
    """
    Configure and return the root logger.

    Safe to call multiple times: adds a StreamHandler only when the root
    logger has no handlers yet, preventing duplicate log lines while
    ensuring every call correctly applies the configured level.
    (logging.basicConfig() is a no-op after the first call in a process,
    which caused the level/format to be silently ignored on re-invocation.)

    Parameters
    ----------
    config : dict
        Must include config["logging"]["level"] and config["logging"]["format"].

    Returns
    -------
    logging.Logger
        The configured root logger.
    """
    level_name = config["logging"]["level"].upper()
    log_level  = getattr(logging, level_name, logging.INFO)
    log_format = config["logging"]["format"]

    root = logging.getLogger()
    root.setLevel(log_level)

    if not root.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(log_format))
        root.addHandler(handler)

    return root