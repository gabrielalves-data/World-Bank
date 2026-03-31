import logging

# Logging Setup
def setup_logging(config: dict) -> logging.Logger:
    """
    Configure and initialize the application logger.

    Parameters
    ----------
    config : dict
        Configuration dictionary containing logging settings.
        Must include:
        - config["logging"]["level"] : str
        - config["logging"]["format"] : str

    Returns
    -------
    logging.Logger
        Configured logger instance for the current module.

    Notes
    -----
    The logging level is dynamically resolved using ``getattr(logging, level)``.
    This function should be called once at the start of execution.
    """
    logging.basicConfig(
        level=getattr(logging, config["logging"]["level"]),
        format=config["logging"]["format"]
    )

    return logging.getLogger(__name__)