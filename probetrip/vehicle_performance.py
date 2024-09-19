from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, StringType

import time
import ast
import numpy as np
from datetime import datetime

from probetrip.utils import get_date_range

class VehiclePerformance:
    def __init__(self, spark, db_name, trip_point_table, speed_acc_table):
        self.spark = spark
        self.trip_point_table = f'{db_name}.{trip_point_table}'
        self.speed_acc_table = f'{db_name}.{speed_acc_table}'

    @f.udf(ArrayType(StringType()))
    def cal_point_speed_acc(point_list):
        def cal_distance(p1, p2):
            '''
                param p1, p2: point
                return: distance in kilometers
            '''
            p1 = {'lat': p1[0], 'lon': p1[1]}
            p2 = {'lat': p2[0], 'lon': p2[1]}
            raw_distance = np.arccos((np.sin(np.radians(p1['lat'])) * np.sin(np.radians(p2['lat']))) + np.cos(np.radians(p1['lat'])) * \
                                      np.cos(np.radians(p2['lat'])) * (np.cos(np.radians(p2['lon']) - np.radians(p1['lon']))))
            distance = 0 if np.isnan(raw_distance) else raw_distance * 6371
            return np.round(distance, 2)
        
        def cal_time_diff(t1, t2, fmt='%H:%M:%S'):
            '''
                param t1, t2: time (e.g. "00:00:01") 
                return time in minutes
            '''
            tdelta = datetime.strptime(t2, fmt) - datetime.strptime(t1, fmt)
            return np.round(tdelta.total_seconds() / 60, 2)
        
        points = ast.literal_eval(point_list)
    
        n_point = len(points)
        i = 1
        prev_speed = 0
        coors_speed_acc = [] # [time, lat, lon, speed]
        while i < n_point:
            distance = cal_distance(points[i-1][1:], points[i][1:])
            duration = cal_time_diff(points[i-1][0], points[i][0])
            speed = np.round(60*distance/duration if duration != 0 else 0, 2)
            hour = datetime.strptime(points[i][0], '%H:%M:%S').hour
    
            if i > 1:
                acc = ((speed - prev_speed) / (duration / 60)) / 12960 if duration != 0 else 0
                coors_speed_acc.append(str({'hour': hour, 'lat': points[i][1], 'lon': points[i][2], 'speed': speed, 'acc': acc}))
            prev_speed = speed
            i = i+1
        return coors_speed_acc

    def generate_speed_acc(self, start_date, end_date):
        date_range = get_date_range(start_date, end_date)

        self.spark.udf.register('cal_point_speed_acc', self.cal_point_speed_acc)

        for d in date_range:
            start = time.time()
            
            coors_speed_acc = self.spark.sql(
                f"""
                SELECT
                    id,
                    trip_date,
                    CAST(GET_JSON_OBJECT(p_speed_acc, '$.hour') AS INT) AS hour,
                    CAST(GET_JSON_OBJECT(p_speed_acc, '$.lat') AS FLOAT) AS lat,
                    CAST(GET_JSON_OBJECT(p_speed_acc, '$.lon') AS FLOAT) AS lon,
                    CAST(GET_JSON_OBJECT(p_speed_acc, '$.speed') AS FLOAT) AS speed,
                    CAST(GET_JSON_OBJECT(p_speed_acc, '$.acc') AS FLOAT) AS acc
                FROM
                    (SELECT 
                        id,
                        trip_date,
                        CAL_POINT_SPEED_ACC(point_list) AS point_speed_acc
                    FROM 
                        {self.trip_point_table}
                    WHERE mobility_type = 'MOVE' AND total_points > 2 AND
                        DATE_FORMAT(`trip_date`,'yyyyMMdd') = '{d}')
                LATERAL VIEW EXPLODE(point_speed_acc) AS p_speed_acc
                """)

            coors_speed_acc.createOrReplaceTempView("coors_speed_acc")
            self.spark.sql(
                f"""
                INSERT INTO {self.speed_acc_table}
                SELECT 
                    `id`,
                    `trip_date`,
                    `hour`,
                    `lat`,
                    `lon`,
                    `speed`,
                    `acc`,
                    DATE_FORMAT(`trip_date`,'yyyyMM') AS partition_month
                FROM coors_speed_acc
                """)
            
            end = time.time()
            print(f"Generate Speed-Acceleration on {d} successfuly! \t | Process time: {(end-start)/60:.2f} mins")