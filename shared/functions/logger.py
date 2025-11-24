import logging
import logging.config
import os
import yaml

def get_logging_cfg(app_name=None, cfg_path=None):
    """
    * Get the logging config.
    """
    cfg_path = "{home}/objects/config/logging.yaml".format(home=os.environ["HOME"]) if cfg_path is None else cfg_path
    with open(cfg_path, "r") as f:
        log_cfg = yaml.safe_load(f)
    return log_cfg

def get_logger(app_name=None, cfg_path=None):
    """
    * Return configured logger.
    """
    app_name = os.environ["APP_NAME"] if app_name is None else app_name
    log_cfg = get_logging_cfg(app_name, cfg_path)
    logging.config.dictConfig(log_cfg)
    return logging.getLogger(app_name)
