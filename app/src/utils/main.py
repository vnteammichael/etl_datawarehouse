from .log import LOGGER
from datetime import timedelta, datetime
import os
import time
from natsort import natsorted

def currentMillisecondsTime():
    return round(time.time() * 1000)

def display_results(rows):
    """Log results of query to console."""
    for row in rows:
        LOGGER.info(row)

def date_range(start_date, end_date):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    if start_date==end_date:
        yield start_date_obj

    for n in range(int((end_date_obj - start_date_obj).days)):
        yield start_date_obj + timedelta(n)



def get_list_of_files(dirName):
    if dirName=="":
        return list()
    # create a list of file and sub directories
    # names in the given directory
    listOfFile = os.listdir(dirName)
    allFiles = list()

    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + get_list_of_files(fullPath)
        else:
            allFiles.append(fullPath)

    allFiles_sorted = natsorted(allFiles)
    # allFiles_sorted.reverse()

    stt = 0
    total_files = len(allFiles_sorted)
    totalAll = list()
    for log_filename in allFiles_sorted:
        stt = stt + 1
        # Không import file cuối cùng, vì có khả năng file tiếp tục được append logs
        if stt==total_files and total_files > 24:
            continue
        totalAll.append(log_filename)

    return totalAll


def generateCaseSegment(fieldName,rangeValue):
    rangeTxt = "CASE"

    for key in rangeValue:
        rangeTxt = rangeTxt + " WHEN " + fieldName + " <= " + str(rangeValue[key]) + " THEN '" + key + "'"

    rangeTxt = rangeTxt + " END "
    return rangeTxt