from config.runtime import LOG_LEVEL

LEVELS = {
    "DEBUG": 3,
    "INFO": 2,
    "TRADE": 1,
    "SILENT": 0
}

def log(msg, level="INFO"):
    if LEVELS[level] <= LEVELS[LOG_LEVEL]:
        print(msg)