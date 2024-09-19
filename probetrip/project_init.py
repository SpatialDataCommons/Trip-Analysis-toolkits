class ProjectInit:
    def __init__(self, spark, db_name):
        self.spark = spark
        self.db_name = db_name

    def create_probe_table(self, probe_table):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{probe_table} (
                `vehicle_id` STRING,
                `gpsvalid` INT,
                `lat` FLOAT,
                `lon` FLOAT,
                `date_time` TIMESTAMP,
                `speed` FLOAT,
                `heading` INT,
                `for_hire_light` INT,
                `engine_acc` INT)
            PARTITIONED BY (partition_month STRING)
            STORED AS PARQUET
            """)

    def create_trip_table(self, trip_table):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{trip_table} (
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

    def create_od_table(self, od_table):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{od_table} (
                `id` STRING,
                `trip_date` DATE,
                `trip_sequence` INT,
                `origin_hour` INT, 
                `origin_lat` FLOAT, 
                `origin_lon` FLOAT,
                `origin_sub_district` STRING, 
                `origin_district` STRING, 
                `origin_province` STRING,
                `destination_hour` INT, 
                `destination_lat` FLOAT, 
                `destination_lon` FLOAT,
                `destination_sub_district` STRING, 
                `destination_district` STRING, 
                `destination_province` STRING,
                `partition_month` STRING)
            STORED AS PARQUET
            PARTITIONED BY (partition_month)
            """)
        
    def create_speed_acc_table(self, speed_acc_table):
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{speed_acc_table} (
                `id` STRING,
                `trip_date` DATE, 
                `hour` INT,
                `lat` FLOAT,
                `lon` FLOAT,
                `speed` FLOAT,
                `acc` FLOAT,
                `partition_month` STRING)
            STORED AS PARQUET
            PARTITIONED BY (partition_month)
            """)

    def create_spark_stat_table(self, spark_stat_table):     
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.db_name}.{spark_stat_table} (
                `process_ts` TIMESTAMP,
                `name` STRING,
                `start_date` STRING,
                `end_date` STRING,
                `total_date` INT,
                `input_record` INT,
                `output_record` INT,
                `executor_instances` INT,
                `executor_cores` INT,
                `executor_memory` STRING,
                `process_time` FLOAT,
                `partition_month` STRING)
            STORED AS PARQUET
            PARTITIONED BY (partition_month)
            """)