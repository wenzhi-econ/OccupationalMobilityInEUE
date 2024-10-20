import os

data_root = "E:\\Projects\\OccupationalMobilityInEUE-Data"

RAWDATA_PATH = os.path.join(data_root, "rawdata")
TEMPDATA_PATH = os.path.join(data_root, "tempdata")
FINALDATA_PATH = os.path.join(data_root, "finaldata")

def rawdata(*args):
    return os.path.join(RAWDATA_PATH, *args)

def tempdata(*args):
    return os.path.join(TEMPDATA_PATH, *args)

def finaldata(*args):
    return os.path.join(FINALDATA_PATH, *args)