import urllib
from urllib import request
import os
import tarfile
import pandas as pd
import time

class Probe:
    def __init__(self, spark, db_name, probe_table, month, folder_path):
        self.spark = spark
        self.db_name = db_name
        self.probe_table = probe_table

        self.month = month
        self.folder_path = folder_path
        self.url = f"https://itic.longdo.com/opendata/probe-data/PROBE-{month}.tar.bz2"
        self.file_path = f"{folder_path}PROBE-{month}.tar.bz2"

    def get_data(self):
        try:
            request.urlretrieve(self.url, self.file_path)
            print(f"File downloaded successfully: {self.file_path}")
        except urllib.error.URLError as e:
            print(f"Error downloading file: {e}")

    def extract_file(self):
        """
            file_path: 'dataset/probe/PROBE-202301.tar.bz2'
            folder_path: 'dataset/probe/'
        """
        try:
            with tarfile.open(self.file_path, "r:bz2") as tf:
                tf.extractall(path=self.folder_path)
                print(f"Successfully extracted {self.file_path.split('/')[-1]} to {self.folder_path}")
        except (tarfile.ReadError, tarfile.NotADirectoryError, tarfile.TarError) as e:
            print(f"Error extracting {self.file_path.split('/')[-1]}: {e}")

    def insert_data(self, start_date='', end_date=''):
        file_paths = [p for p in sorted(os.listdir(f"{self.folder_path}PROBE-{self.month}/")) if '.csv' in p]
        column_lst = ['VehicleID', 'gpsvalid', 'lat', 'lon', 'timestamp', 'speed', 'heading', 'for_hire_light', 'engine_acc']

        if start_date != '' and end_date != '':
            file_paths = [p for p in file_paths if (p[:8] >= start_date and p[:8] <= end_date)]

        for path in file_paths:
            start = time.time()
        
            # Read file to Pandas and create Spark temp table
            current_date = path.split('.')[0]
            df = pd.read_csv(f"{self.folder_path}PROBE-{self.month}/{path}", names=column_lst)
            probe = self.spark.createDataFrame(df)
            probe.createOrReplaceTempView('probe')
        
            self.spark.sql(
                f"""
                INSERT INTO {self.db_name}.{self.probe_table}
                PARTITION (partition_month)
                SELECT 
                    `VehicleID` AS vehicle_id, 
                    `gpsvalid`, 
                    `lat`, `lon`,
                    TIMESTAMP(`timestamp`) AS date_time, 
                    `speed`, 
                    `heading`, 
                    `for_hire_light`, 
                    `engine_acc`,
                    DATE_FORMAT(`timestamp`,'yyyyMM') AS partition_month
                FROM probe
                WHERE DATE_FORMAT(`timestamp`,'yyyyMMdd') = '{current_date}'
                """)
        
            end = time.time()
            print(f"Insert Probe data on {current_date} successfuly! | Process time: {(end-start)/60:.2f} mins")