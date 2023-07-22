import os
import logging
import logging.config
import json
import requests #Airport codes eventually
import csv #Airport codes eventually

# logging
LOGS_PATH = os.path.join(os.path.dirname(__file__), "logs")
LOG_LEVEL = "INFO"
LOGGING_CONFIG_DICT = {
    'version': 1,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'level': LOG_LEVEL,
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'standard',
            'filename': os.path.join(LOGS_PATH, "logs.log"),
            "when": "W6", # sunday, 
            'backupCount': 8
        },
    },
    'root': {
        'handlers': ['console', 'file'],
        'level': LOG_LEVEL
    }
}


def create_logs_folder():
    if not os.path.isdir(LOGS_PATH):
        os.mkdir(LOGS_PATH)
        
def setup_logger(logger_name):
    create_logs_folder()
    logging.getLogger('WDM').setLevel(logging.NOTSET) # suppress WDM (Webdrive Manager) logs
    logging.config.dictConfig(LOGGING_CONFIG_DICT)
    return logging.getLogger(logger_name)


def get_routes_from_config(config_obj):
    """
    Returns a list of routes from the config file.
    """
    routes = []
    for route in config_obj["routes"]:
        routes.append(json.loads(config_obj["routes"][route]))

    return routes


# TODO: I want to do something with these codes. Not sure what yet...
def updateAirportCodes(pullNewData=True):
    
    if pullNewData:
        # Step 1: Pull data from the URL
        url = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
        response = requests.get(url)
        data = response.text

        # Step 2: Save data to a local CSV file
        filename = "airport_data.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            lines = data.split('\n')
            for line in lines:
                writer.writerow(line.split(','))

    # Step 3: Load data from the CSV file
    airport_data = []
    with open(filename, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            airport_data.append([row[3], row[4]])
    
    return airport_data


# A quick check on config.ini formats. Not perfect but will catch most.
def checkRoutes(routes):
    om=0
    nm=0
    nnm=0
    count = len(routes)
    for i in routes:
        #if we find something NOt an int, we know it's not the old method.
        if not (isinstance(i[2], int)):
            # check next slot which would be. If not there, it must be round trip.
            if not (isinstance(i[3], int)):
                if not (isinstance(i[4], int)):
                    raise ValueError("Something wrong with config.ini")
                nnm+=1
                continue
            nm+=1
            continue
        om+=1
    if om==count:
        return False, False
    elif nm==count:
        return True, False
    elif nnm==count:
        return False, True
    else:
        raise ValueError("There's something wrong with config.ini")