################
# logging setup
################


import logging
import logging.config as logging_config


logging.captureWarnings(True)
logging.getLogger("requests").setLevel(logging.WARNING)

DEBUG_LOGGER_NAME = 'synapseclient_debug'
DEFAULT_LOGGER_NAME = 'synapseclient_default'
SILENT_LOGGER_NAME = 'synapseclient_silent'


class LoggingInfoOnlyFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO


class LoggingIgnoreInfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno != logging.INFO


####################################################
# logging levels from high to low:
# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG
# NOTSET
#
# see https://docs.python.org/2/library/logging.html
######################################################
# TODO: debug file also or only write errors to log?
logging_config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'debug_format': {
            'format': '%(asctime)s [%(module)s:%(lineno)d - %(levelname)s]: %(message)s'
        },
        'brief_format': {
            'format': '%(message)s'
        },
        'warning_format': {
            'format': '[%(levelname)s] %(message)s'
        }
    },
    'filters': {
        'info_only': {
            '()': LoggingInfoOnlyFilter
        },
        'ignore_info': {
            '()': LoggingIgnoreInfoFilter
        }
    },
    'handlers': {
        'info_only_stdout': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'brief_format',
            'stream': 'ext://sys.stdout',
            'filters': ['info_only']
        },
        'debug_stderr': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'debug_format',
            'stream': 'ext://sys.stderr'
        },
        'warning_stderr': {
            'level': 'WARNING',
            'class': 'logging.StreamHandler',
            'formatter': 'warning_format',
            'stream': 'ext://sys.stderr',
        },
        # ,
        # "error_to_file": {
        #     "class": "logging.handlers.RotatingFileHandler",
        #     "level": "ERROR",
        #     "formatter": "debug_format",
        #     "filename": _errlog_path,
        #     "maxBytes": 10485760, #10 MB
        #     "backupCount": 15,
        #     "encoding": "utf8"
        # }
    },
    'loggers': {
        DEFAULT_LOGGER_NAME: {
            # TODO: add 'error_to_file' back in after we find solution for multi process logging
            'handlers': ['info_only_stdout', 'warning_stderr'],
            'level': 'INFO',
            'propagate': True
        },
        DEBUG_LOGGER_NAME: {
            'handlers': ['info_only_stdout', 'debug_stderr'],
            'level': 'DEBUG',
            'propagate': True
        },
        SILENT_LOGGER_NAME: {
            'handlers': [],
            'level': 'INFO',
            'propagate': False
        }
    }
})

