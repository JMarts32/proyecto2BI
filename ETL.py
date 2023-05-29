from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

pwd = os.getcwd()
uid = os.getlogin()
server = 'localhost'
database = 'asma'
port = '5432'
dir = './data/ETL/'

def extract():
    try:
        directory = dir
        for filename in os.listdir(directory):
            file = os.path.splitext(filename)[0]
