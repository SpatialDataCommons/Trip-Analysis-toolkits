from pyspark.sql import functions as f
from pyspark.sql.types import StringType, ArrayType

from probetrip.utils import get_date_range

import ast
import time
from datetime import datetime, date, timedelta
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

class Trip:

    def __init__(self, spark, db_name, probe_table, trip_point_table, shape_path):
        global tha_shape
        tha_shape = gpd.read_file(shape_path)
        tha_shape = tha_shape[['ADM3_EN', 'ADM2_EN', 'ADM1_EN', 'geometry']]
        tha_shape.columns = ['sub_district', 'district', 'province', 'geometry']
        
        self.spark = spark
        self.db_name = db_name
        self.probe_table = f'{db_name}.{probe_table}'
        self.trip_point_table = f'{db_name}.{trip_point_table}'

        self.temp_data_point_table = f'{db_name}.temp_data_point_view'
        self.temp_stay_point_table = f'{db_name}.temp_stay_point_view'
        self.temp_trip_point_table = f'{db_name}.temp_trip_point_view'
        self.temp_trip_explode_table = f'{db_name}.temp_trip_explode_view'

        self.spark.udf.register('time_point_lst', self.time_point_lst)
        self.spark.udf.register('cal_staypoints', self.cal_staypoints)
        self.spark.udf.register('cal_trips', self.cal_trips)

    @f.udf(ArrayType(StringType()))
    def time_point_lst(points):
        items = []
        points = np.array(points)
        points = points[points[:, 0].argsort()]
        for i, p in enumerate(points):
            point_time = p[0]
            lat, lon = float(p[1]), float(p[2])
            for_hire = int(p[3])
            items.append(str([i+1, point_time, lat, lon, for_hire]))
        return items
    
    # @staticmethod
    @f.udf(ArrayType(StringType()))
    def cal_staypoints(points, max_distance, max_duration):
        """
        Calculate Stay point information
        Args:
            points (list):  list of point [sequence, time, lat, lon, for_hire_light]
            max_distance (float): max distance threshold in kilometers
            max_duration (float): max duration threshold in minutes
        Returns: 
            staypoints (String): A list of staypoint
        Examples:
            >>> cal_staypoints([[1, '00:02:49', 14.03297, 100.786, 1], ...], 0.15, 15)
            [{
             'mobility_type': 'STAY', 
             'passenger': 0, 
             'total_distance': 0.0, 
             'total_time': 318.53, 
             'overall_speed': 0.0, 
             'average_speed': 0, 
             'start_time': '00:02:49', 
             'end_time': '05:21:21', 
             'total_points': 121, 
             'location': '{"sub_district":"Lam Phak Kut","district":"Thanyaburi","province":"Pathum Thani"}', 
             'point_list': [[14.032970, 100.786000]]
             }, ...]
        """

        def cal_distance(p1, p2):
            """
            Calculate the distance between 2 points in kilometers
            Args:  
                p1, p2 (list): A list of lat and lon
            Returns: 
                float: The distance in kilometers
            Examples:
                >>> cal_distance([14.03297, 100.786], [14.03347, 100.78609])
                0.05
            """
            p1 = {'lat': p1[0], 'lon': p1[1]}
            p2 = {'lat': p2[0], 'lon': p2[1]}
            raw_distance = np.arccos((np.sin(np.radians(p1['lat'])) * np.sin(np.radians(p2['lat']))) + np.cos(np.radians(p1['lat'])) * \
                                      np.cos(np.radians(p2['lat'])) * (np.cos(np.radians(p2['lon']) - np.radians(p1['lon']))))
            distance = 0 if np.isnan(raw_distance) else raw_distance * 6371
            return np.round(distance, 2)

        def cal_time_diff(t1, t2, fmt='%H:%M:%S'):
            """
            Calculate the duration between 2 times in minutes
            Args:  
                t1, t2 (String): A String of time
            Returns: 
                float: The duration in minutes
            Examples:
                >>> cal_time_diff('00:02:49', '01:02:49')
                60
            """
            tdelta = datetime.strptime(t2, fmt) - datetime.strptime(t1, fmt)
            return np.round(tdelta.total_seconds() / 60, 2)
        
        def cal_stat(p1, p2):
            """
            Calculate the statistics between 2 points
            Args:  
                p1, p2 (list): A list of point detail [sequence, time, lat, lon, for_hire_light]
            Returns: 
                distance(km), duration(mins), speed(km/h) (float): The statistics between 2 points
            Examples:
                >>> cal_stat([1, '00:00:00', 14.03297, 100.786, 0], [2, '01:00:00', 14.03347, 100.786, 0])
                0.05, 60, 0.05
            """
            p1 = {'time': p1[1], 'coor': p1[2:4]}
            p2 = {'time': p2[1], 'coor': p2[2:4]}
            distance = cal_distance(p1['coor'], p2['coor'])
            duration = cal_time_diff(p1['time'], p2['time'])
            speed = np.round(60*distance/duration if duration != 0 else 0, 2)
            return distance, duration, speed
        
        def cal_centroid(points):
            """
            Calculate the centroid point and total point among points
            Args:  
                point (list of list): A list of points detail [sequence, time, lat, lon, for_hire_light]
            Returns: 
                centroid (list), total_point (int): The statistics between 2 points
            Examples:
                >>> cal_centroid([[1, '00:00:00', 14.03297, 100.786, 0], [2, '01:00:00', 14.03347, 100.786, 0], ...])
                [14.03297, 100.786], 2
            """
            point_lst = np.array([[seq[2], seq[3]] for seq in points])
            length = len(point_lst)
            cen_lat = np.sum(point_lst[:, 0]) / length
            cen_lon = np.sum(point_lst[:, 1]) / length
            return [cen_lat, cen_lon], length
        
        def search_location(tha_shape, point):
            """
            Identify the region that the point is in
            Args:  
                tha_shape (geometry): Geometry shape of Thailand
                point (list): A list of lat and lon
            Returns: 
                location (String): A Sting of location in JSON form
            Examples:
                >>> search_location(tha_shape, [14.03347, 100.78609])
                '{"sub_district":"","district":"","province":""}'
            """
            lat = point[0]
            lon = point[1]
            geo_serie = tha_shape.contains(Point(lon, lat))
            if geo_serie.any():
                idx = geo_serie[geo_serie].index[0]
                return tha_shape.iloc[idx, :3].to_json()
            return '{"sub_district":"","district":"","province":""}'
        
        def create_staypoint_dict(start_point, end_point, total_distance, speed_lst, centroid, total_point, max_duration):
            start_point = {'time': start_point[1]}
            end_point = {'time': end_point[1]}
    
            staypoint = dict()
            staypoint['mobility_type'] = 'STAY'
            staypoint['passenger'] = 0
            staypoint['total_distance'] = np.round(total_distance, 2)
            staypoint['total_time'] = cal_time_diff(start_point['time'], end_point['time'])
            staypoint['overall_speed'] = np.round(60*staypoint['total_distance'] / staypoint['total_time'] if staypoint['total_time'] != 0 else 0, 2)
            staypoint['average_speed'] = np.median(speed_lst) if speed_lst != [] else 0
            staypoint['start_time'] = start_point['time']
            staypoint['end_time'] = end_point['time']
            staypoint['total_points'] = total_point
            staypoint['location'] = search_location(tha_shape, centroid)
            staypoint['point_list'] = [[centroid[0], centroid[1]]]
            return staypoint
        
        points = [ast.literal_eval(item) for item in points]
        staypoints = []
        n_point = len(points)
        checkpoint = 0
        while checkpoint < n_point - 1:
            total_distance = 0
            speed_lst = []
            walk = checkpoint + 1
            while walk < n_point and checkpoint < walk:
                distance, duration, speed = cal_stat(points[walk-1], points[walk])
                total_distance += distance
                speed_lst.append(speed)
    
                p_checkpoint = {'time': points[checkpoint][1], 'coor': points[checkpoint][2:4]}
                p_current = {'time': points[walk][1], 'coor': points[walk][2:4]}
    
                distance_checkpoint = cal_distance(p_checkpoint['coor'], p_current['coor'])
                if distance_checkpoint > max_distance:
                    time_checkpoint = cal_time_diff(p_checkpoint['time'], p_current['time'])
                    # STAY period
                    if time_checkpoint > max_duration:
                        centroid, total_point = cal_centroid(points[checkpoint:walk])
                        # speed_lst = remove_outlier(speed_lst[:-1] if len(speed_lst) > 1 else speed_lst)
                        staypoint = create_staypoint_dict(points[checkpoint], points[walk-1], total_distance - distance, speed_lst, centroid, total_point, max_duration)
                        # Get only STAY which the duration more than threshold
                        if staypoint['total_time'] >= max_duration:
                            staypoints.append(str(staypoint))
                    checkpoint = walk
                    continue
                # End with STAY
                elif walk >= n_point - 1:
                    time_checkpoint = cal_time_diff(p_checkpoint['time'], p_current['time'])
                    if time_checkpoint > max_duration:
                        centroid, total_point = cal_centroid(points[checkpoint:])
                        # speed_lst = remove_outlier(speed_lst)
                        staypoint = create_staypoint_dict(points[checkpoint], points[walk], total_distance, speed_lst, centroid, total_point, max_duration)
                        # Get only STAY which the duration more than threshold
                        if staypoint['total_time'] >= max_duration:
                            staypoints.append(str(staypoint))
                    checkpoint = walk
                    continue
                walk += 1
        return staypoints

    @f.udf(ArrayType(StringType()))
    def cal_trips(points, staypoints):

        def cal_distance(p1, p2):
            """
            Calculate the distance between 2 points in kilometers
            Args:  
                p1, p2 (list): A list of lat and lon
            Returns: 
                float: The distance in kilometers
            Examples:
                >>> cal_distance([14.03297, 100.786], [14.03347, 100.78609])
                0.05
            """
            p1 = {'lat': p1[0], 'lon': p1[1]}
            p2 = {'lat': p2[0], 'lon': p2[1]}
            raw_distance = np.arccos((np.sin(np.radians(p1['lat'])) * np.sin(np.radians(p2['lat']))) + np.cos(np.radians(p1['lat'])) * \
                                      np.cos(np.radians(p2['lat'])) * (np.cos(np.radians(p2['lon']) - np.radians(p1['lon']))))
            distance = 0 if np.isnan(raw_distance) else raw_distance * 6371
            return np.round(distance, 2)
        
        def cal_time_diff(t1, t2, fmt='%H:%M:%S'):
            """
            Calculate the duration between 2 times in minutes
            Args:  
                t1, t2 (String): A String of time
            Returns: 
                float: The duration in minutes
            Examples:
                >>> cal_time_diff('00:02:49', '01:02:49')
                60
            """
            tdelta = datetime.strptime(t2, fmt) - datetime.strptime(t1, fmt)
            return np.round(tdelta.total_seconds() / 60, 2)
        
        def cal_stat(p1, p2):
            """
            Calculate the statistics between 2 points
            Args:  
                p1, p2 (list): A list of point detail [sequence, time, lat, lon, for_hire_light]
            Returns: 
                distance(km), duration(mins), speed(km/h) (float): The statistics between 2 points
            Examples:
                >>> cal_stat([1, '00:00:00', 14.03297, 100.786, 0], [2, '01:00:00', 14.03347, 100.786, 0])
                0.05, 60, 0.05
            """
            p1 = {'time': p1[1], 'coor': p1[2:4]}
            p2 = {'time': p2[1], 'coor': p2[2:4]}
            distance = cal_distance(p1['coor'], p2['coor'])
            duration = cal_time_diff(p1['time'], p2['time'])
            speed = np.round(60*distance/duration if duration != 0 else 0, 2)
            return distance, duration, speed
    
        def create_movepoint_dict(start_point, end_point, total_distance, speed_lst, movepoint_list, trip_seq, passenger_state=0):
            start_point = {'time': start_point[1]}
            end_point = {'time': end_point[1]}
            
            movepoint = dict()
            movepoint['mobility_type'] = 'MOVE'
            movepoint['passenger'] = passenger_state
            movepoint['total_distance'] = np.round(total_distance, 2)
            movepoint['total_time'] = cal_time_diff(start_point['time'], end_point['time'])
            movepoint['overall_speed'] = np.round(60*movepoint['total_distance'] / movepoint['total_time'] if movepoint['total_time'] != 0 else 0, 2)
            movepoint['average_speed'] = np.median(speed_lst) if speed_lst != [] else 0
            movepoint['start_time'] = start_point['time']
            movepoint['end_time'] = end_point['time']
            movepoint['total_points'] = len(movepoint_list)
            movepoint['location'] = '{"sub_district":"","district":"","province":""}'
            movepoint['point_list'] = movepoint_list
            movepoint['trip_sequence'] = trip_seq
            return movepoint
        
        # convert string to list
        points = [ast.literal_eval(item) for item in points]
        staypoints = [ast.literal_eval(item) for item in staypoints]
    
        trips = []
        trip_seq = 1
        checkpoint = 0
        st_idx = 0
        n_point = len(points)
        while checkpoint < n_point - 1:
            total_distance = 0
            speed_lst = []
            movepoint_list = []
            walk = checkpoint
            
            point = {'for_hire': points[walk][4]}
            curr_passenger_state = 0 if point['for_hire'] == 1 else 1
            
            while walk < n_point and checkpoint <= walk:
                if walk > 0:
                    distance, duration, speed = cal_stat(points[walk-1], points[walk])
                    total_distance += distance
                    speed_lst.append(speed)
                    
                point = {'time_coor': points[walk][1:4], 'for_hire': points[walk][4]}
                movepoint_list.append(point['time_coor'])
                passenger_state = 0 if point['for_hire'] == 1 else 1
    
                # End with MOVE OR switch delivery/idle state -> Add MOVE to list
                if walk >= n_point - 1 or passenger_state != curr_passenger_state:
                    movepoint = create_movepoint_dict(points[checkpoint], points[walk], total_distance, speed_lst, movepoint_list, trip_seq, curr_passenger_state)
                    trips.append(str(movepoint))
                        
                    # reset stat
                    total_distance = 0
                    speed_lst = []
                    movepoint_list = [movepoint_list[-1]]
                    curr_passenger_state = passenger_state
                    checkpoint = walk
                    trip_seq += 1
    
                # Point is inside STAY -> Add MOVE and STAY to list
                elif len(staypoints) != 0: 
                    if points[walk][1] >= staypoints[st_idx]['start_time'] and points[walk][1] < staypoints[st_idx]['end_time']:
                        if walk - checkpoint > 0:
                            movepoint = create_movepoint_dict(points[checkpoint], points[walk], total_distance, speed_lst, movepoint_list, trip_seq, curr_passenger_state)
                            trips.append(str(movepoint))
                            trip_seq += 1
                        staypoints[st_idx]['trip_sequence'] = trip_seq
                        trips.append(str(staypoints[st_idx]))
                        checkpoint = walk + staypoints[st_idx]['total_points'] - 1
                        st_idx = st_idx + 1 if st_idx < len(staypoints) - 1 else st_idx
                        trip_seq += 1
                
                walk += 1
        return trips

    def create_trip_table(self):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.trip_point_table} (
              `id` STRING,
              `trip_date` DATE,
              `trip_sequence` INT,
              `mobility_type` STRING,
              `passenger` INT,
              `total_distance` FLOAT,
              `total_time` FLOAT,
              `overall_speed` FLOAT,
              `average_speed` FLOAT,
              `start_time` STRING, 
              `end_time` STRING, 
              `total_points` INT,
              `sub_district` STRING,
              `district` STRING,
              `province` STRING,
              `point_list` STRING,
              `partition_month` STRING)
            USING PARQUET
            PARTITIONED BY (partition_month)
            """)

    def generate_trip_daily(self, start_date, end_date, max_distance, max_duration, verbose=False):
        date_range = get_date_range(start_date, end_date)

        # Clear temp tables
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_data_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_stay_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_explode_table}")
        
        for d in date_range:
            start = time.time()
            curr_time = start

            with tqdm(total=6) as pbar:
                # === STAGE 1 =======================
                userdata_view = self.spark.sql(
                    f"""
                    SELECT 
                        vehicle_id AS id, date_time, lat, lon, for_hire_light
                    FROM 
                        {self.probe_table}
                    WHERE 
                        (lat <> 0 AND lon <> 0) AND 
                        DATE_FORMAT(`date_time`,'yyyyMMdd') = '{d}'
                    """)
                userdata_view.createOrReplaceTempView("userdata_view")
                if verbose:
                    self.spark.sql("SELECT * FROM userdata_view").limit(1).show()
                    print(f"Step 1: User data \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
                curr_time = time.time()
                pbar.set_description(f"Step 1")
                pbar.update()
                
                # === STAGE 2 =======================
                data_point_view = self.spark.sql(
                    """
                    SELECT 
                        id, DATE(date_time) AS trip_date,
                        DATE_FORMAT(MIN(date_time), 'HH:mm:ss') AS start_time,
                        DATE_FORMAT(MAX(date_time), 'HH:mm:ss') AS end_time,
                        TIME_POINT_LST(COLLECT_LIST(ARRAY(DATE_FORMAT(date_time, 'HH:mm:ss'), lat, lon, for_hire_light))) AS points
                    FROM userdata_view
                    GROUP BY id, DATE(date_time)
                    """)
                data_point_view.createOrReplaceTempView("data_point_view")
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_data_point_table} USING PARQUET AS SELECT * FROM data_point_view")
                if verbose:
                    self.spark.sql(f"SELECT * FROM {self.temp_data_point_table}").limit(1).show()
                    print(f"Step 2: Data point \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
                self.spark.catalog.dropTempView('userdata_view')
                self.spark.catalog.dropTempView('data_point_view')
                curr_time = time.time()
                pbar.set_description(f"Step 2")
                pbar.update()
            
                # === STAGE 3 =======================
                stay_point_view = self.spark.sql(
                    f"""
                    SELECT 
                        id, trip_date,
                        CAL_STAYPOINTS(points, {max_distance}, {max_duration}) AS staypoints
                    FROM {self.temp_data_point_table}
                    """)
                stay_point_view.createOrReplaceTempView("stay_point_view")
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_stay_point_table} USING PARQUET AS SELECT * FROM stay_point_view")
                if verbose:
                    self.spark.sql(f"SELECT * FROM {self.temp_stay_point_table}").limit(1).show()
                    print(f"Step 3: Stay point \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
                self.spark.catalog.dropTempView('stay_point_view')
                curr_time = time.time()
                pbar.set_description(f"Step 3")
                pbar.update()
            
                # === STAGE 4 =======================    
                trip_point_view = self.spark.sql(
                    f"""
                    SELECT 
                        dp.id, dp.trip_date,
                        CAL_TRIPS(dp.points, sp.staypoints) AS trip
                    FROM 
                        {self.temp_data_point_table} dp,
                        {self.temp_stay_point_table} sp
                    WHERE dp.id = sp.id AND dp.trip_date = sp.trip_date
                    """)
                trip_point_view.createOrReplaceTempView("trip_point_view")
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_trip_point_table} USING PARQUET AS SELECT * FROM trip_point_view")
                if verbose:
                    self.spark.sql(f"SELECT * FROM {self.temp_trip_point_table}").limit(1).show()
                    print(f"Step 4: Trip point \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
                self.spark.catalog.dropTempView('trip_point_view')
                curr_time = time.time()
                pbar.set_description(f"Step 4")
                pbar.update()
                
                # === STAGE 5 =======================
                trip_explode_view = self.spark.sql(
                    f"""
                    SELECT id, trip_date,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.trip_sequence') AS INT) AS trip_sequence,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.mobility_type') AS STRING) AS mobility_type,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.passenger') AS INT) AS passenger,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.total_distance') AS FLOAT) AS total_distance,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.total_time') AS FLOAT) AS total_time,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.overall_speed') AS FLOAT) AS overall_speed,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.average_speed') AS FLOAT) AS average_speed,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.start_time') AS STRING) AS start_time,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.end_time') AS STRING) AS end_time,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.total_points') AS INT) AS total_points,
                        CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(trip_explode, '$.location'), '$.sub_district') AS STRING) AS sub_district,
                        CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(trip_explode, '$.location'), '$.district') AS STRING) AS district,
                        CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(trip_explode, '$.location'), '$.province') AS STRING) AS province,
                        CAST(GET_JSON_OBJECT(trip_explode, '$.point_list') AS STRING) AS point_list
                    FROM {self.temp_trip_point_table}
                    LATERAL VIEW EXPLODE(trip) AS trip_explode
                    """)
                trip_explode_view.createOrReplaceTempView("trip_explode_view")
                self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_trip_explode_table} USING PARQUET AS SELECT * FROM trip_explode_view")
                if verbose:
                    self.spark.sql(f"SELECT * FROM {self.temp_trip_explode_table}").limit(1).show()
                    print(f"Step 5: Trip explode and extract features \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
                self.spark.catalog.dropTempView('trip_explode_view')
                curr_time = time.time()
                pbar.set_description(f"Step 5")
                pbar.update()
            
                # === STAGE 6 =======================
                self.spark.sql(
                    f"""
                    INSERT INTO {self.trip_point_table}
                    PARTITION (partition_month)
                    SELECT  
                        `id`,
                        `trip_date`,
                        `trip_sequence`, 
                        `mobility_type`, 
                        `passenger`,
                        `total_distance`, 
                        `total_time`, 
                        `overall_speed`, 
                        `average_speed`, 
                        `start_time`, 
                        `end_time`, 
                        `total_points`, 
                        `sub_district`,
                        `district`,
                        `province`,
                        `point_list`, 
                        DATE_FORMAT(`trip_date`,'yyyyMM') as partition_month
                    FROM {self.temp_trip_explode_table}
                    """)
                if verbose:
                    print(f"Step 6: Insert data into {self.trip_point_table} \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
                pbar.set_description(f"Step 6")
                pbar.update()
                
                # Drop the temp tables
                self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_data_point_table}")
                self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_stay_point_table}")
                self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_point_table}")
                self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_explode_table}")
            
                end = time.time()
                print(f"Generate Probe trip point by user on {d} successfuly! \t | Process time: {(end-start)/60:.2f} mins")
                
    def generate_trip(self, start_date, end_date, max_distance, max_duration, verbose=False, spark_stat_table=''):
        date_range = get_date_range(start_date, end_date)

        # Clear temp tables
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_data_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_stay_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_explode_table}")
        
        
        start = time.time()
        curr_time = start

        with tqdm(total=6) as pbar:
            # === STAGE 1 =======================
            userdata_view = self.spark.sql(
                f"""
                SELECT 
                    vehicle_id AS id, date_time, lat, lon, for_hire_light
                FROM 
                    {self.probe_table}
                WHERE 
                    (lat <> 0 AND lon <> 0) AND 
                    DATE_FORMAT(`date_time`,'yyyyMMdd') >= '{start_date}' AND
                    DATE_FORMAT(`date_time`,'yyyyMMdd') <= '{end_date}'
                """)
            userdata_view.createOrReplaceTempView("userdata_view")
            if verbose:
                self.spark.sql("SELECT * FROM userdata_view").limit(1).show()
                print(f"Step 1: User data \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
            curr_time = time.time()
            pbar.set_description(f"Step 1")
            pbar.update()
            
            # === STAGE 2 =======================
            data_point_view = self.spark.sql(
                """
                SELECT 
                    id, DATE(date_time) AS trip_date,
                    DATE_FORMAT(MIN(date_time), 'HH:mm:ss') AS start_time,
                    DATE_FORMAT(MAX(date_time), 'HH:mm:ss') AS end_time,
                    TIME_POINT_LST(COLLECT_LIST(ARRAY(DATE_FORMAT(date_time, 'HH:mm:ss'), lat, lon, for_hire_light))) AS points
                FROM userdata_view
                GROUP BY id, DATE(date_time)
                """)
            data_point_view.createOrReplaceTempView("data_point_view")
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_data_point_table} USING PARQUET AS SELECT * FROM data_point_view")
            if verbose:
                self.spark.sql(f"SELECT * FROM {self.temp_data_point_table}").limit(1).show()
                print(f"Step 2: Data point \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
            self.spark.catalog.dropTempView('userdata_view')
            self.spark.catalog.dropTempView('data_point_view')
            curr_time = time.time()
            pbar.set_description(f"Step 2")
            pbar.update()
        
            # === STAGE 3 =======================
            stay_point_view = self.spark.sql(
                f"""
                SELECT 
                    id, trip_date,
                    CAL_STAYPOINTS(points, {max_distance}, {max_duration}) AS staypoints
                FROM {self.temp_data_point_table}
                """)
            stay_point_view.createOrReplaceTempView("stay_point_view")
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_stay_point_table} USING PARQUET AS SELECT * FROM stay_point_view")
            if verbose:
                self.spark.sql(f"SELECT * FROM {self.temp_stay_point_table}").limit(1).show()
                print(f"Step 3: Stay point \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
            self.spark.catalog.dropTempView('stay_point_view')
            curr_time = time.time()
            pbar.set_description(f"Step 3")
            pbar.update()
        
            # === STAGE 4 =======================    
            trip_point_view = self.spark.sql(
                f"""
                SELECT 
                    dp.id, dp.trip_date,
                    CAL_TRIPS(dp.points, sp.staypoints) AS trip
                FROM 
                    {self.temp_data_point_table} dp,
                    {self.temp_stay_point_table} sp
                WHERE dp.id = sp.id AND dp.trip_date = sp.trip_date
                """)
            trip_point_view.createOrReplaceTempView("trip_point_view")
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_trip_point_table} USING PARQUET AS SELECT * FROM trip_point_view")
            if verbose:
                self.spark.sql(f"SELECT * FROM {self.temp_trip_point_table}").limit(1).show()
                print(f"Step 4: Trip point \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
            self.spark.catalog.dropTempView('trip_point_view')
            curr_time = time.time()
            pbar.set_description(f"Step 4")
            pbar.update()
            
            # === STAGE 5 =======================
            trip_explode_view = self.spark.sql(
                f"""
                SELECT id, trip_date,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.trip_sequence') AS INT) AS trip_sequence,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.mobility_type') AS STRING) AS mobility_type,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.passenger') AS INT) AS passenger,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.total_distance') AS FLOAT) AS total_distance,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.total_time') AS FLOAT) AS total_time,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.overall_speed') AS FLOAT) AS overall_speed,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.average_speed') AS FLOAT) AS average_speed,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.start_time') AS STRING) AS start_time,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.end_time') AS STRING) AS end_time,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.total_points') AS INT) AS total_points,
                    CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(trip_explode, '$.location'), '$.sub_district') AS STRING) AS sub_district,
                    CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(trip_explode, '$.location'), '$.district') AS STRING) AS district,
                    CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(trip_explode, '$.location'), '$.province') AS STRING) AS province,
                    CAST(GET_JSON_OBJECT(trip_explode, '$.point_list') AS STRING) AS point_list
                FROM {self.temp_trip_point_table}
                LATERAL VIEW EXPLODE(trip) AS trip_explode
                """)
            trip_explode_view.createOrReplaceTempView("trip_explode_view")
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.temp_trip_explode_table} USING PARQUET AS SELECT * FROM trip_explode_view")
            if verbose:
                self.spark.sql(f"SELECT * FROM {self.temp_trip_explode_table}").limit(1).show()
                print(f"Step 5: Trip explode and extract features \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
            self.spark.catalog.dropTempView('trip_explode_view')
            curr_time = time.time()
            pbar.set_description(f"Step 5")
            pbar.update()
        
            # === STAGE 6 =======================
            self.spark.sql(
                f"""
                INSERT INTO {self.trip_point_table}
                PARTITION (partition_month)
                SELECT  
                    `id`,
                    `trip_date`,
                    `trip_sequence`, 
                    `mobility_type`, 
                    `passenger`,
                    `total_distance`, 
                    `total_time`, 
                    `overall_speed`, 
                    `average_speed`, 
                    `start_time`, 
                    `end_time`, 
                    `total_points`, 
                    `sub_district`,
                    `district`,
                    `province`,
                    `point_list`, 
                    DATE_FORMAT(`trip_date`,'yyyyMM') as partition_month
                FROM {self.temp_trip_explode_table}
                """)
            if verbose:
                print(f"Step 6: Insert data into {self.trip_point_table} \t | Process time: {(time.time() - curr_time)/60:.2f} mins")
            pbar.set_description(f"Step 6")
            pbar.update()
            
            end = time.time()

        # Record Spark performace
        if spark_stat_table != '':
            probe_stat = self.spark.sql(
                f"""
                SELECT COUNT(*) AS records
                FROM {self.probe_table}
                WHERE DATE_FORMAT(`date_time`,'yyyyMMdd') >= '{start_date}' AND DATE_FORMAT(`date_time`,'yyyyMMdd') <= '{end_date}'
                """).collect()
            
            trip_stat = self.spark.sql(
                f"""
                SELECT COUNT(DISTINCT trip_date) AS total_date,
                    COUNT(*) AS records
                FROM {self.temp_trip_explode_table}
                WHERE DATE_FORMAT(`trip_date`,'yyyyMMdd') >= '{start_date}' AND DATE_FORMAT(`trip_date`,'yyyyMMdd') <= '{end_date}'
                """).collect()
            
            self.spark.sql(
                f"""
                INSERT INTO {self.db_name}.{spark_stat_table}
                VALUES (
                    CURRENT_TIMESTAMP(), 
                    'trip', 
                    '{start_date}', 
                    '{end_date}', 
                    {trip_stat[0].__getitem__('total_date')}, 
                    {probe_stat[0].__getitem__('records')}, 
                    {trip_stat[0].__getitem__('records')},
                    {self.spark.sparkContext.getConf().get('spark.executor.instances')},
                    {self.spark.sparkContext.getConf().get('spark.executor.cores')},
                    '{self.spark.sparkContext.getConf().get('spark.executor.memory')}',
                    {(end-start)/60:.2f},
                    DATE_FORMAT(CURRENT_TIMESTAMP(),'yyyyMM') as partition_month)
                """)
            
        # Drop the temp tables
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_data_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_stay_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_point_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.temp_trip_explode_table}")
        
        print(f"Generate Probe trip point by user on {start_date} to {end_date} successfuly! \t | Process time: {(end-start)/60:.2f} mins")