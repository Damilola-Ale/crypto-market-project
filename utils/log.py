from config.runtime import RUN_MODE

def debug(msg):
    if RUN_MODE == "DEBUG":
        print(msg)

def info(msg):
    if RUN_MODE in ["DEBUG", "LIVE"]:
        print(msg)

def trade(msg):
    # trades should ALWAYS print
    print(msg)

def error(msg):
    print(msg)