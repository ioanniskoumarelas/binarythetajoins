import sys;
import os;
from os import listdir;
from os.path import isfile, join;

datasetParamPath = sys.argv[1]; # 'parameters/cloudSolarAltitude19/solarAltitude19'
threadsNum=sys.argv[2];#'2';
threadsSleepTime=sys.argv[3];#'5';
maxJavaHeapMB = "-Xmx" + sys.argv[4] + "m";

jarFileName = "btj_producer.jar";
jarCommand="java -jar" + " " + maxJavaHeapMB + " " + jarFileName;

cmdParts = [jarCommand, datasetParamPath, threadsNum, threadsSleepTime];
cmdToExecute = " ".join(cmdParts);
print cmdToExecute;
os.system(cmdToExecute);
