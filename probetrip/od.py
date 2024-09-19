from pyspark.sql import functions as f
from pyspark.sql.types import StringType

import ast
import geopandas as gpd
from datetime import datetime
import time

from shapely.geometry import Point

class OD:
    def __init__(self, spark, db_name, trip_point_table, od_table, shape_path):
        global tha_shape
        tha_shape = gpd.read_file(shape_path)
        tha_shape = tha_shape[['ADM3_EN', 'ADM2_EN', 'ADM1_EN', 'geometry']]
        tha_shape.columns = ['sub_district', 'district', 'province', 'geometry']
        
        self.spark = spark
        self.trip_point_table = f'{db_name}.{trip_point_table}'
        self.od_table = f'{db_name}.{od_table}'

    @f.udf(StringType())
    def get_origin_dest_point(points):
        
        def search_location(point):
            lat = point[0]
            lon = point[1]
            geo_serie = tha_shape.contains(Point(lon, lat))
            if geo_serie.any():
                idx = geo_serie[geo_serie].index[0]
                location_dict = tha_shape.iloc[idx, :3].to_dict()
                location_dict['lat'] = str(lat)
                location_dict['lon'] = str(lon)
                return location_dict
            return {"lat": str(lat), "lon": str(lon),  "sub_district": "", "district": "", "province": ""}
        
        points = ast.literal_eval(points)
        
        origin_dest = dict()
        origin_dest['origin'] = search_location(points[0][1:3])
        origin_dest['origin']['hour'] = datetime.strptime(points[0][0], '%H:%M:%S').hour
        origin_dest['destination'] = search_location(points[-1][1:3])
        origin_dest['destination']['hour'] = datetime.strptime(points[-1][0], '%H:%M:%S').hour
        return str(origin_dest)

    def generate_od(self, month):

        self.spark.udf.register('get_origin_dest_point', self.get_origin_dest_point)

        start = time.time()
        delivery_od = self.spark.sql(
            f"""
            SELECT 
                id, trip_date, trip_sequence,
                CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.origin'), '$.hour') AS INT) AS origin_hour,
                CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.origin'), '$.lat') AS FLOAT) AS origin_lat,
                CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.origin'), '$.lon') AS FLOAT) AS origin_lon,
                GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.origin'), '$.sub_district') AS origin_sub_district,
                GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.origin'), '$.district') AS origin_district,
                GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.origin'), '$.province') AS origin_province,
                CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.destination'), '$.hour') AS INT) AS destination_hour,
                CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.destination'), '$.lat') AS FLOAT) AS destination_lat,
                CAST(GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.destination'), '$.lon') AS FLOAT) AS destination_lon,
                GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.destination'), '$.sub_district') AS destination_sub_district,
                GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.destination'), '$.district') AS destination_district,
                GET_JSON_OBJECT(GET_JSON_OBJECT(origin_dest, '$.destination'), '$.province') AS destination_province
            FROM 
                (SELECT 
                    id, trip_date, trip_sequence, 
                    GET_ORIGIN_DEST_POINT(point_list) AS origin_dest
                FROM 
                    {self.trip_point_table}
                WHERE 
                    mobility_type = 'MOVE' AND passenger = 1 AND total_distance > 0 AND total_points > 2 AND 
                    DATE_FORMAT(`trip_date`,'yyyyMM') = '{month}')
            """)
        
        delivery_od.createOrReplaceTempView("delivery_od")
        self.spark.sql(
            f"""
            INSERT INTO {self.od_table}
            SELECT 
                `id`, 
                `trip_date`, 
                `trip_sequence`, 
                `origin_hour`, 
                `origin_lat`, 
                `origin_lon`,
                `origin_sub_district`, 
                `origin_district`, 
                `origin_province`,
                `destination_hour`, 
                `destination_lat`, 
                `destination_lon`,
                `destination_sub_district`, 
                `destination_district`, 
                `destination_province`,
                DATE_FORMAT(`trip_date`,'yyyyMM') AS partition_month
            FROM delivery_od
            """)

        end = time.time()
        print(f"Generate Origin-Destination on {month} successfuly! \t | Process time: {(end-start)/60:.2f} mins")