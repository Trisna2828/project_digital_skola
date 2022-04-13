#import libraries
import os

def datalake_file_config():
        config_file = "\config\data_lake.conf"
        return config_file

def data_config(fileconf):
        config = {}
        rfile = open(os.getcwd() + fileconf, "r")
        for line in rfile:
                line = line.replace("\n","")
                length = len(line)

                if (length != 0):
                        line = line.replace(" ", "")
                        (name, value) = line.split("=")
                        config[name] = value
        return config



        