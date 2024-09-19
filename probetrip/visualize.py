from pyspark.sql import functions as f
from pyspark.sql.types import StringType, IntegerType, FloatType, ArrayType

import numpy as np
import pandas as pd
import geopandas as gpd
import ast
from datetime import datetime

import matplotlib.pyplot as plt
import seaborn as sns
from pyproj import Transformer
from collections import Counter
from shapely import to_geojson
from shapely.geometry import Point, Polygon
import plotly.express as px
import plotly.graph_objs as go

from sklearn.cluster import DBSCAN

class StatVis:
    
    def __init__(self, spark, db_name, trip_point_table, od_table, speed_acc_table, spark_stat_table):
        self.spark = spark
        self.db_name = db_name
        self.trip_point_table = f'{db_name}.{trip_point_table}'
        self.od_table = f'{db_name}.{od_table}'
        self.speed_acc_table = f'{db_name}.{speed_acc_table}'
        self.spark_stat_table = f'{db_name}.{spark_stat_table}'

        self.spark.udf.register('get_stay_coors', self.get_stay_coors)
        self.spark.udf.register('one_hot_hour', self.one_hot_hour)
        self.spark.udf.register('coors_dbscan', self.coors_dbscan)

        # define update menu
        self.updatemenus = [
            {'buttons': [
                {'args': [None, {'frame': {'duration': 1000, 'redraw': True}, 'mode': 'immediate', 'fromcurrent': True, 'transition': {'duration': 1000, 'easing': 'linear'}}], 
                     'label': '&#9654;', 'method': 'animate'},
                {'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'fromcurrent': True, 'transition': {'duration': 0, 'easing': 'linear'}}], 
                     'label': '&#9724;', 'method': 'animate'}],
             'direction': 'left', 'showactive': False, 'type': 'buttons',
             'pad': {'r': 10, 't': 30, 'b': 30}, 'x': 0.1, 'xanchor': 'right',  'y': 0, 'yanchor': 'top'}]
        # define slider
        self.sliders = [dict(
            steps=[
                dict(method='animate', label=f'{h}', args=[[f'frame{h}'], dict(mode='immediate', frame=dict(duration=0, redraw=True), transition=dict(duration=0, easing='linear'))]) 
                for h in range(24)],
            currentvalue=dict(prefix='Hour: '),
            len=1.0, active=0,
            x=0.1, xanchor='left', y=0, yanchor='top')]

    ###############################################################
    ###   Total trip
    ###############################################################

    def get_boundary(self, num_lst):
        num_lst = [n for n in num_lst if str(n) != 'nan']
        q1 = np.percentile(num_lst, 25)
        q2 = np.percentile(num_lst, 50)
        q3 = np.percentile(num_lst, 75)
        iqr = q3 - q1
    
        lower_bound = q1 - 1.5*iqr
        upper_bound = q3 + 1.5*iqr
        q_cut = {0.25: q1, 0.5: q2, 0.75: q3}
        return lower_bound, upper_bound, q_cut
    
    def classify_num_range(self, num, q_cut):
        if num < q_cut[0.25]:
            return 'Group1'
        elif num >= q_cut[0.25] and num < q_cut[0.5]:
            return 'Group2'
        elif num >= q_cut[0.5] and num < q_cut[0.75]:
            return 'Group3'
        elif num >= q_cut[0.75]:
            return 'Group4'

    def total_trip(self, month):
        days = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        trip_per_day_df = self.spark.sql(
            f"""
            SELECT 
                trip_date, 
                CASE WHEN passenger = 1 THEN 'Busy' ELSE 'Vacant' END AS `trip_type`, 
                COUNT(*) AS total_trip
            FROM 
                {self.trip_point_table}
            WHERE 
                mobility_type = 'MOVE' AND total_points > 2 AND
                partition_month = '{month}'
            GROUP BY 
                id, trip_date, passenger
            """).toPandas()

        trip_per_day_df['day'] = trip_per_day_df['trip_date'].apply(lambda x: x.strftime('%a'))

        # plot graph
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        b1 = sns.boxplot(data=trip_per_day_df, x='trip_type', y='total_trip', hue='trip_type', gap=0.1, whis=(0, 100), ax=axes[0])
        b2 = sns.boxplot(data=trip_per_day_df, x='day', y='total_trip', hue='trip_type', order=days, gap=0.1, whis=(0, 100), ax=axes[1])

        axes[0].set(xlabel='Trip type', ylabel='Number of trip')
        axes[1].set(xlabel='Day', ylabel='Number of trip')
        sns.move_legend(axes[1], "lower center", bbox_to_anchor=(.5, 1), ncol=2, title=None, frameon=False)
        
        medians = trip_per_day_df.groupby(['trip_type'])['total_trip'].median()
        for xtick in [t.get_text() for t in b1.get_xticklabels()]:
            b1.text(xtick, medians[xtick] + 1, medians[xtick], horizontalalignment='center', size='x-small', color='w', weight='semibold')
        
        medians = trip_per_day_df.groupby(['day', 'trip_type'])['total_trip'].median()
        for i, xtick in enumerate([t.get_text() for t in b2.get_xticklabels()]):
            b2.text(i + 0.2, medians[xtick]['Busy'] + 0.2, medians[xtick]['Busy'], horizontalalignment='center',size='x-small',color='w',weight='semibold')
            b2.text(i - 0.2, medians[xtick]['Vacant'] + 0.2, medians[xtick]['Vacant'], horizontalalignment='center',size='x-small',color='w',weight='semibold')

        return fig

    ###############################################################
    ###   Trip distance, duration, and speed
    ###############################################################

    def trip_distribution(self, name, month):
        name_dict = {'distance': 'total_distance', 'duration': 'total_time', 'speed': 'overall_speed'}
        xlabel_dict = {'distance': 'Distance (kilometers)', 'duration': 'Duration (minutes)', 'speed': 'Speed (km/h)'}
        hue_order = ['Busy, Vacant']
        
        move_df = self.spark.sql(
            f"""
            SELECT 
                {name_dict[name]},
                CASE WHEN passenger = 1 THEN 'Busy' ELSE 'Vacant' END AS `trip_type`
            FROM 
                {self.trip_point_table}
            WHERE 
                mobility_type = 'MOVE' AND total_points > 2 AND
                partition_month = '{month}'
            """).toPandas()

        lb, ub, q_cut = self.get_boundary(move_df[f'{name_dict[name]}'])
        move_df = move_df[(move_df[f'{name_dict[name]}'] > lb) & (move_df[f'{name_dict[name]}'] < ub)].reset_index(drop=True)
        move_df[f'{name_dict[name]}_group'] = move_df[f'{name_dict[name]}'].apply(lambda x: self.classify_num_range(x, q_cut))

        # plot graph
        bins = range(0, int(ub), 2)
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        sns.histplot(data=move_df, x=name_dict[name], bins=bins, hue='trip_type', ax=axes[0])
        box_plot = sns.boxplot(data=move_df, x=f'{name_dict[name]}_group', y=f'{name_dict[name]}', hue='trip_type', 
                               order=['Group1', 'Group2', 'Group3', 'Group4'], gap=0.1, whis=(0, 100), ax=axes[1])

        axes[0].set(xlabel=xlabel_dict[name], ylabel='Count')
        axes[1].set(xlabel='Group', ylabel=xlabel_dict[name])
        sns.move_legend(axes[0], "lower center", bbox_to_anchor=(.5, 1), ncol=2, title=None, frameon=False)
        sns.move_legend(axes[1], "lower center", bbox_to_anchor=(.5, 1), ncol=2, title=None, frameon=False)

        medians = move_df.groupby([f'{name_dict[name]}_group', 'trip_type'])[f'{name_dict[name]}'].median()
        for xtick in box_plot.get_xticks():
            box_plot.text(xtick + 0.2, medians[2*xtick] + 0.2, medians[2*xtick], horizontalalignment='center',size='x-small',color='w',weight='semibold')
            box_plot.text(xtick - 0.2, medians[2*xtick+1] + 0.2, medians[2*xtick+1], horizontalalignment='center',size='x-small',color='w',weight='semibold')
        
        return fig

    ###############################################################
    ###   OD heatmap
    ###############################################################

    def od_heatmap(self, province, month):
        od_province_df = self.spark.sql(
            f"""
            SELECT 
                origin_province, 
                origin_district, 
                destination_province, 
                destination_district,
                COUNT(*) AS total_count
            FROM 
                {self.od_table}
            WHERE 
                origin_province = '{province}' AND destination_province = '{province}' AND
                partition_month = '{month}'
            GROUP BY 
                origin_province, origin_district, destination_province, destination_district
            """).toPandas()

        count_matrix = pd.pivot_table(od_province_df, index=['origin_province', 'origin_district'], 
                                      columns=['destination_province', 'destination_district'],
                                      values='total_count', aggfunc='sum', fill_value=0)

        # plot graph
        fig = plt.figure(figsize=(15,15))
        ax1 = plt.subplot2grid((20,20), (1,0), colspan=19, rowspan=19)
        ax2 = plt.subplot2grid((20,20), (0,0), colspan=19, rowspan=1)
        ax3 = plt.subplot2grid((20,20), (1,19), colspan=1, rowspan=19)

        sns.heatmap(count_matrix, cmap='Reds', cbar = False,
                    xticklabels=count_matrix.columns.get_level_values(1),
                    yticklabels=count_matrix.index.get_level_values(1), ax=ax1)
        sns.heatmap(pd.DataFrame(count_matrix.sum(axis=0)).transpose(), cmap='Reds',
                    cbar=False, xticklabels=False, yticklabels=False, ax=ax2)
        sns.heatmap(pd.DataFrame(count_matrix.sum(axis=1)), cmap='Reds',
                    cbar=False, xticklabels=False, yticklabels=False, ax=ax3)
        
        ax1.set_xlabel('Destination')
        ax1.set_ylabel('Origin')
        ax2.set_ylabel('')    
        ax2.set_xlabel('')
        ax3.set_ylabel('')    
        ax3.set_xlabel('')
        
        return fig

    ###############################################################
    ###   density map
    ###############################################################

    @f.udf(StringType())
    def coors_dbscan(coors, eps=500, min_samples=50):
        """
            Clustering the coordinated
            Args:
                coors (list):  list of coordinates [lat, lon]
                eps (float): the maximum distance between two samples
                min_samples (float): a number of samples in a neighborhood
            Returns: 
                cluster (String): A list of cluster [[lat, lon, weight, cluster]]
            Examples:
                >>> dbscan([[14.03297, 100.786], ...], 1000, 50)
                [[14.03297, 100.786, 0.75, 0], ...]
        """
        def epsg_4326_to_3857(coors):
            transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857")
            
            new_coors = []
            for c in coors:
                lat = c[0]
                lon = c[1]
                c_3857 = transformer.transform(lat, lon)
                new_coors.append([c_3857[0], c_3857[1]])
            return new_coors
    
        coors = np.array(coors)
        cluster = np.zeros((len(coors), 4))
        cluster[:, :2] = coors
        coors_3857 = epsg_4326_to_3857(coors)
    
        dbs = DBSCAN(eps=eps, min_samples=min_samples, algorithm='ball_tree', metric='euclidean')
        dbs.fit(coors_3857)
    
        k_rank = Counter([i for i in dbs.labels_])
        k_cluter = len(k_rank.keys()) - 1
        for i, c in enumerate(dbs.labels_):
            weight = 0
            if c == -1:
                cluster[i, 2:] = [0, c]
            else:
                weight = (1 - c / k_cluter)
                cluster[i, 2:] = [weight, c]
        cluster = np.array([c for c in cluster if c[2] > 0])
        
        return str(cluster.tolist())

    def tranform_plotly(self, coors_df):
        def get_id(hour, cluster):
            return f'{int(hour):02d}-{int(cluster):03d}'
    
        def to_cluster_geojson(geometry, hour, cluster):
            coors_json = ast.literal_eval(to_geojson(geometry))
            geo_json = {'type': 'Feature',
                       'geometry': {
                           'type': 'Polygon',
                           'coordinates': coors_json['coordinates']},
                       'id': get_id(hour, cluster)
                       }
            return geo_json
        
        def get_polygon(coors_list):
            geo = pd.DataFrame(columns=['geometry', 'weight', 'cluster', 'hour'])
            for i, hour in enumerate(coors_list):
                coors = pd.DataFrame(hour, columns=['lat', 'lon', 'weight', 'cluster'])
                k_cluster = int(max(coors['cluster']) + 1)
                
                polygons = []
                for k in range(k_cluster):
                    coors_k = coors[coors['cluster'] == k]
                    weight = np.median(coors_k['weight'])
            
                    coors_k = [Point(x, y) for [x, y] in coors_k[['lon', 'lat']].values.tolist()]
                    polygon = Polygon([[p.x, p.y] for p in coors_k]).convex_hull
                    polygons.append([polygon, weight, k]) # [Polygon, weight, cluster]
                gdf = gpd.GeoDataFrame(polygons, columns=['geometry', 'weight', 'cluster']).set_crs('epsg:4326')
                gdf['hour'] = i
                geo = pd.concat([geo, gdf], axis=0)
            
            geo = geo.reset_index(drop=True)
            geo['geojson'] = geo[['geometry', 'hour', 'cluster']].apply(lambda x: to_cluster_geojson(x['geometry'], x['hour'], x['cluster']), axis=1)
            geojson = {'type': 'FeatureCollection', 'features': geo['geojson'].values.tolist()}
            return geojson
        
        hour_cluster = coors_df.copy()
        hour_cluster['cluster'] = hour_cluster['cluster'].apply(lambda x: ast.literal_eval(x))
        hour_cluster = hour_cluster.sort_values(by='hour').reset_index(drop=True)
        coors_list = hour_cluster['cluster'].values.tolist()
    
        geojson = get_polygon(coors_list)
    
        # prepare coordinates
        coors_extracted = pd.DataFrame(columns=['lat', 'lon', 'weight', 'cluster', 'hour'])
        for index, row in hour_cluster.iterrows():
            df = pd.DataFrame(row['cluster'], columns=['lat', 'lon', 'weight', 'cluster'])
            df['hour'] = index
        
            coors_extracted = pd.concat([coors_extracted, df], axis=0)
        coors_extracted = coors_extracted.reset_index(drop=True)
        coors_extracted['id'] = coors_extracted[['hour', 'cluster']].apply(lambda x: get_id(x['hour'], x['cluster']), axis=1)
    
        return coors_extracted, geojson

    def od_density_map(self, province, type, month, eps=150, min_samples=25):

        self.spark.udf.register('coors_dbscan', self.coors_dbscan)
        
        coors_df = self.spark.sql(
            f"""
            SELECT 
                {type}_hour AS hour,
                COORS_DBSCAN(COLLECT_LIST(ARRAY({type}_lat, {type}_lon)), {eps}, {min_samples}) AS cluster
            FROM 
                {self.od_table}
            WHERE 
                (origin_province = '{province}' OR destination_province = '{province}') AND
                origin_district <> destination_district AND
                partition_month = '{month}'
            GROUP BY {type}_hour
            """).toPandas()

        coors_extracted, geojson = self.tranform_plotly(coors_df)

        
        # define update menu
        updatemenus = [
            {'buttons': [
                {'args': [None, {'frame': {'duration': 1000, 'redraw': True}, 'mode': 'immediate', 'fromcurrent': True, 'transition': {'duration': 1000, 'easing': 'linear'}}], 
                     'label': '&#9654;', 'method': 'animate'},
                {'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'fromcurrent': True, 'transition': {'duration': 0, 'easing': 'linear'}}], 
                     'label': '&#9724;', 'method': 'animate'}],
             'direction': 'left', 'showactive': False, 'type': 'buttons',
             'pad': {'r': 10, 't': 30, 'b': 30}, 'x': 0.1, 'xanchor': 'right',  'y': 0, 'yanchor': 'top'}]
        # define slider
        sliders = [dict(steps=[
                dict(method='animate', label=f'{h}',
                     args=[[f'frame{h}'], dict(mode='immediate',
                                           frame=dict(duration=0, redraw=True),
                                           transition=dict(duration=0, easing='linear'))])
                for h in range(24)],
              currentvalue=dict(prefix='Hour: '),
              len=1.0, active=0,
              x=0.1, xanchor='left', y=0, yanchor='top')]

        # plot graph
        density = px.density_mapbox(coors_extracted, lat='lat', lon='lon', z='weight', radius=5, animation_frame='hour')
        c_shape = px.choropleth_mapbox(coors_extracted, geojson=geojson, color='weight', locations='id', opacity=0.2, animation_frame='hour')
        
        frames = [
            go.Frame(data=c_shape.frames[i].data + f.data, name=f'frame{i}')
            for i, f in enumerate(density.frames)]
        
        fig = go.Figure(data=frames[0].data, frames=frames)
        fig.update_layout(mapbox_style="carto-positron", mapbox_zoom=10, 
                          mapbox_center={"lat": 13.75971, "lon": 100.50859},
                          width=1400, height=600, margin={"r":0,"t":0,"l":0,"b":0},
                          coloraxis={'colorbar': {'title': {'text': 'weight'}},
                                     'colorscale': px.colors.sequential.Rainbow,
                                     'cmin': 0, 'cmax': 1},
                          updatemenus=updatemenus,
                          sliders=sliders
                          )
        fig.show()

    def od_density_map_overall(self, province, month, eps=150, min_samples=25):

        density = []
        for type in ['origin', 'destination']:
            coors_df = self.spark.sql(
                f"""
                SELECT 
                    {type}_hour AS hour,
                    COORS_DBSCAN(COLLECT_LIST(ARRAY({type}_lat, {type}_lon)), {eps}, {min_samples}) AS cluster
                FROM 
                    {self.od_table}
                WHERE 
                    (origin_province = '{province}' OR destination_province = '{province}') AND
                    origin_district <> destination_district AND
                    partition_month = '{month}'
                GROUP BY {type}_hour
                """).toPandas()
    
            coors_extracted, geojson = self.tranform_plotly(coors_df)
            density.append(px.density_mapbox(coors_extracted, lat='lat', lon='lon', z='weight', radius=5, opacity=0.8, animation_frame='hour'))

        frames = []
        for i_frame, f in enumerate(density[0].frames):
            frame = []
            for i_density in range(len(density)):
                d_copy = density[i_density].frames[i_frame].data[0]
                d_copy['coloraxis'] = f'coloraxis{i_density+1}' if i_density > 0 else 'coloraxis'
                frame.append(d_copy)
            frames.append(go.Frame(data=tuple(frame), name=f'frame{i_frame}'))
        
        fig = go.Figure(data=frames[0].data, frames=frames)
        fig.update_layout(mapbox_style="carto-positron", mapbox_zoom=10, 
                          mapbox_center={"lat": 13.75971, "lon": 100.50859},
                          width=1600, height=800, margin={"r":0,"t":0,"l":0,"b":0},
                          coloraxis={'colorscale': px.colors.sequential.Mint, 'colorbar': {'title': 'Origin', 'x': 1}, 'cmin': 0, 'cmax': 1},
                          coloraxis2={'colorscale': px.colors.sequential.Reds, 'colorbar': {'title': 'Destination', 'x': 1.1}, 'cmin': 0, 'cmax': 1},
                          updatemenus=self.updatemenus,
                          sliders=self.sliders
                          )
        fig.show()

    ###############################################################
    ###   stay hourly volume
    ###############################################################

    @f.udf(ArrayType(IntegerType()))
    def get_hour_vol(time_list):
        '''
            params time_list: list of start and end time in the date
                ex: [[start_time, end_time], ...]
            return list of staypoint
        '''
        hour_list = []
        for t in time_list:
            start = datetime.strptime(t[0], '%H:%M:%S').hour
            end = datetime.strptime(t[1], '%H:%M:%S').hour
            hour_list.append([i for i in range(start, end+1)])
    
        # Flaten the hour list
        hour_list_flated = [i for h in hour_list for i in h]
        counts, bin_edges = np.histogram(hour_list_flated, bins=range(0,25))
        return counts.tolist()
        
    def stay_hourly_volume(self, month):
        hour_range = [f'{i}' for i in range(0, 24)]
        days = [ 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

        self.spark.udf.register('get_hour_vol', self.get_hour_vol)
        
        hour_vol_df = self.spark.sql(
            f"""
            SELECT 
                trip_date, 
                GET_HOUR_VOL(COLLECT_LIST(ARRAY(start_time, end_time))) AS hour_freq_lists
            FROM 
                {self.trip_point_table}
            WHERE 
                mobility_type = 'STAY' AND
                partition_month = '{month}'
            GROUP BY 
                trip_date
            """).toPandas()

        hour_vol_df['weekday'] = hour_vol_df['trip_date'].apply(lambda x: x.strftime('%a'))
        hour_vol_df[hour_range] = pd.DataFrame(hour_vol_df['hour_freq_lists'].tolist(), index=hour_vol_df.index)
        hour_vol_df = hour_vol_df[['weekday']+hour_range].groupby('weekday').agg('median').reindex(days)

        # plot graph
        fig, axes = plt.subplots(2, 1, figsize=(16, 12))

        sns.heatmap(hour_vol_df, cmap='Greens', cbar_kws={'label': 'Count'}, ax=axes[0])
        axes[0].set(xlabel='Hour', ylabel='Day')
        axes[0].set_yticklabels(axes[0].get_yticklabels(), rotation=0)
        
        sns.lineplot(hour_vol_df.T)
        axes[1].set(xlabel='Hour', ylabel='Count')
        sns.move_legend(axes[1], "lower center", bbox_to_anchor=(.5, 1), ncol=7, title=None, frameon=False)
        
        return fig

    ###############################################################
    ###   stay spot
    ###############################################################

    @f.udf(FloatType())
    def get_stay_coors(point_list, c_type):
        coors = ast.literal_eval(point_list)[0]
        
        return coors[0] if c_type == 'lat' else coors[1]

    @f.udf(ArrayType(IntegerType()))
    def one_hot_hour(period):
        start = datetime.strptime(period[0], '%H:%M:%S').hour
        end = datetime.strptime(period[1], '%H:%M:%S').hour
    
        hours = [0] * 24
        for i in range(24):
            hours[i] = 1 if (i >= start and i <= end) else 0
        
        return hours

    def epsg_4326_to_3857(self, coors):
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857")
        
        new_coors = []
        for c in coors:
            lat = c[0]
            lon = c[1]
            c_3857 = transformer.transform(lat, lon)
            new_coors.append([c_3857[0], c_3857[1]])
        return new_coors

    def stay_dbscan(self, coors, eps=1000, min_samples=10):
        coors = np.array(coors)
        cluster = np.zeros((len(coors), 4))
        cluster[:, :2] = coors
        coors_3857 = self.epsg_4326_to_3857(coors)
    
        dbs = DBSCAN(eps=eps, min_samples=min_samples, algorithm='ball_tree', metric='euclidean')
        dbs.fit(coors_3857)
    
        k_rank = Counter([i for i in dbs.labels_])
        k_cluter = len(k_rank.keys())
        for i, c in enumerate(dbs.labels_):
            if c == -1:
                cluster[i, 2:] = [0, c]
            else:
                # weight = 1 / k_rank[c] 
                weight = (1 - c / k_cluter)
                cluster[i, 2:] = [weight, c]
        cluster = np.array([c for c in cluster if c[3] > -1]) # remove noises
        return str(cluster.tolist())

    def cluster_stay(self, stay_coors_df, eps, min_samples):
        hour_range = [f'{i}' for i in range(0, 24)]
        
        coors_df = stay_coors_df.copy()
        coors_df[hour_range] = pd.DataFrame(coors_df['hour'].tolist(), index=coors_df.index)
        coors_list = coors_df[['lat', 'lon']].values.tolist()
    
        coors_hour_df = pd.DataFrame(columns=['hour', 'cluster'])
        for h in hour_range:
            coors = [coors_list[i] for i, f in enumerate(coors_df[f'{h}'].values.tolist()) if f == 1]
    
            cluster = self.stay_dbscan(coors, eps, min_samples)
            df = pd.DataFrame([[h, cluster]], columns=['hour', 'cluster'])
            coors_hour_df = pd.concat([coors_hour_df, df], axis=0)
        return coors_hour_df.reset_index(drop=True)

    def stay_spot(self, duration, month, eps=100, min_samples=50):
        # define duration query condition
        if '-' in duration:
            d = duration.split('-')
            duration = f'total_time > {d[0]} AND total_time <= {d[1]}'
        else:
            duration = f'total_time {duration}'

        self.spark.udf.register('get_stay_coors', self.get_stay_coors)
        self.spark.udf.register('one_hot_hour', self.one_hot_hour)
        
        stay_coors_df = self.spark.sql(
            f"""
            SELECT
                GET_STAY_COORS(point_list, 'lat') AS lat,
                GET_STAY_COORS(point_list, 'lon') AS lon,
                ONE_HOT_HOUR(ARRAY(start_time, end_time)) AS hour
            FROM 
                {self.trip_point_table}
            WHERE 
                mobility_type = 'STAY' AND
                province = 'Bangkok' AND
                partition_month = '{month}' AND
                {duration}
            """).toPandas()

        coors_df = self.cluster_stay(stay_coors_df, eps, min_samples)
        coors_extracted, geojson = self.tranform_plotly(coors_df)

        # define update menu
        updatemenus = [
            {'buttons': [
                {'args': [None, {'frame': {'duration': 1000, 'redraw': True}, 'mode': 'immediate', 'fromcurrent': True, 'transition': {'duration': 1000, 'easing': 'linear'}}], 
                     'label': '&#9654;', 'method': 'animate'},
                {'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate', 'fromcurrent': True, 'transition': {'duration': 0, 'easing': 'linear'}}], 
                     'label': '&#9724;', 'method': 'animate'}],
             'direction': 'left', 'showactive': False, 'type': 'buttons',
             'pad': {'r': 10, 't': 30, 'b': 30}, 'x': 0.1, 'xanchor': 'right',  'y': 0, 'yanchor': 'top'}]
        # define slider
        sliders = [dict(steps=[
                dict(method='animate', label=f'{h}',
                     args=[[f'frame{h}'], dict(mode='immediate',
                                           frame=dict(duration=0, redraw=True),
                                           transition=dict(duration=0, easing='linear'))])
                for h in range(24)],
              currentvalue=dict(prefix='Hour: '),
              len=1.0, active=0,
              x=0.1, xanchor='left', y=0, yanchor='top')]
        
        density = px.density_mapbox(coors_extracted, lat='lat', lon='lon', z='weight', radius=5, animation_frame='hour')
        c_shape = px.choropleth_mapbox(coors_extracted, geojson=geojson, color='weight', locations='id', opacity=0.2, animation_frame='hour')
        
        frames = [
            go.Frame(data=c_shape.frames[i].data + f.data, name=f'frame{i}')
            for i, f in enumerate(density.frames)]
        
        fig = go.Figure(data=frames[0].data, frames=frames)
        fig.update_layout(mapbox_style="carto-positron", mapbox_zoom=10, 
                          mapbox_center={"lat": 13.75971, "lon": 100.50859},
                          width=1400, height=600, margin={"r":0,"t":0,"l":0,"b":0},
                          coloraxis={'colorbar': {'title': {'text': 'weight'}},
                                     'colorscale': px.colors.sequential.Reds,
                                     'cmin': 0, 'cmax': 1},
                          updatemenus=updatemenus,
                          sliders=sliders
                          )
        fig.show()

    def stay_spot_overall(self, duration_list, month, eps_list, min_samples_list):

        density = []
        colors = [px.colors.sequential.Reds, px.colors.sequential.Purp, 
                  px.colors.sequential.Mint, px.colors.sequential.solar]
        for i, duration in enumerate(duration_list):
            if '-' in duration:
                d = duration.split('-')
                duration = f'total_time > {d[0]} AND total_time <= {d[1]}'
            else:
                duration = f'total_time {duration}'
    
            stay_coors_df = self.spark.sql(
                f"""
                SELECT
                    GET_STAY_COORS(point_list, 'lat') AS lat,
                    GET_STAY_COORS(point_list, 'lon') AS lon,
                    ONE_HOT_HOUR(ARRAY(start_time, end_time)) AS hour
                FROM 
                    {self.trip_point_table}
                WHERE 
                    mobility_type = 'STAY' AND
                    province = 'Bangkok' AND
                    partition_month = '{month}' AND
                    {duration}
                """).toPandas()
    
            coors_df = self.cluster_stay(stay_coors_df, eps_list[i], min_samples_list[i])
            coors_extracted, geojson = self.tranform_plotly(coors_df)

            density.append(px.density_mapbox(coors_extracted, lat='lat', lon='lon', z='weight', radius=5, animation_frame='hour'))
            
        frames = []
        for i_frame, f in enumerate(density[0].frames):
            frame = []
            for i_density in range(len(density)):
                d_copy = density[i_density].frames[i_frame].data[0]
                d_copy['coloraxis'] = f'coloraxis{i_density+1}' if i_density > 0 else 'coloraxis'
                frame.append(d_copy)
            frames.append(go.Frame(data=tuple(frame), name=f'frame{i_frame}'))

        fig = go.Figure(data=frames[0].data, frames=frames)
        
        fig.update_layout(mapbox_style="carto-positron", mapbox_zoom=10, 
                          mapbox_center={"lat": 13.75971, "lon": 100.50859},
                          width=1600, height=600, margin={"r":0,"t":0,"l":0,"b":0},
                          coloraxis={'colorscale': px.colors.sequential.Reds, 'colorbar': {'title': f'{duration_list[0]} mins', 'x': 1}, 'cmin': 0, 'cmax': 1},
                          coloraxis2={'colorscale': px.colors.sequential.Purp, 'colorbar': {'title': f'{duration_list[1]} mins', 'x': 1.1}, 'cmin': 0, 'cmax': 1},
                          coloraxis3={'colorscale': px.colors.sequential.Mint, 'colorbar': {'title': f'{duration_list[2]} mins', 'x': 1.2}, 'cmin': 0, 'cmax': 1},
                          updatemenus=self.updatemenus,
                          sliders=self.sliders
                          )
        fig.show()

    ###############################################################
    ###   Speed Acc area
    ###############################################################

    def speed_acc_area(self, type, trip_date, filter_percentage):
        
        speed_acc_df = self.spark.sql(
            f"""
            SELECT
                hour, lat, lon, {type}
            FROM 
                {self.speed_acc_table}
            WHERE 
                trip_date = '{trip_date}'
            """).toPandas()

        lb, ub, q_cut = self.get_boundary(speed_acc_df[f'{type}'])
        speed_acc_df = speed_acc_df[(speed_acc_df[f'{type}'] > lb) & (speed_acc_df[f'{type}'] < ub)].reset_index(drop=True)
        speed_acc_df = speed_acc_df.sort_values(by='hour').reset_index(drop=True)

        q_lower = np.percentile(speed_acc_df[f'{type}'], filter_percentage)
        q_upper = np.percentile(speed_acc_df[f'{type}'], 100 - filter_percentage)
        if type == 'speed':
            speed_acc_df = speed_acc_df[speed_acc_df[f'{type}'] > q_upper]
            color_scale = px.colors.sequential.Reds
        else:
            speed_acc_df = speed_acc_df[(speed_acc_df[f'{type}'] < q_lower) | (speed_acc_df[f'{type}'] > q_upper)]
            color_scale = px.colors.sequential.Bluered

        fig = px.scatter_mapbox(speed_acc_df, lat="lat", lon="lon", animation_frame = 'hour', color=f"{type}", hover_data = ['lat', 'lon', f'{type}'])
        fig.update_layout(mapbox_style="carto-positron", mapbox_zoom=10, 
                          mapbox_center={"lat": 13.75971, "lon": 100.50859},
                          width=1400, height=600, margin={"r":0,"t":0,"l":0,"b":0},
                          coloraxis={'colorbar': {'title': {'text': f'{type}'}},
                                     'colorscale': color_scale},
                          )
        fig.show()

    ###############################################################
    ###   Spark Stat
    ###############################################################

    def spark_stat(self, confs):
        command = ' OR '.join([f"(executor_instances = {c[0]} AND executor_cores = {c[1]} AND executor_memory = '{c[2]}g')" for c in confs])
        
        s_stat = self.spark.sql(
            f"""
            SELECT 
                total_date,
                MAX(input_record) AS input_record,
                MAX(output_record) AS output_record,
                executor_instances, 
                executor_cores,
                executor_memory,
                MAX(process_time) AS process_time
            FROM 
                {self.spark_stat_table}
            WHERE
                {command}
            GROUP BY
                total_date,
                executor_instances, 
                executor_cores,
                executor_memory
            """).toPandas()

        s_stat['worker'] = s_stat.apply(lambda x: x['executor_instances'] * x['executor_cores'], axis=1)
        s_stat['conf'] = s_stat.apply(lambda x: f"instance: {x['executor_instances']}\n exr_core: {x['executor_cores']}\n exr_mem: {x['executor_memory']}", axis=1)
        s_stat = s_stat.sort_values('worker')

        fig = plt.figure(figsize=(12,6))
        ax = sns.barplot(s_stat, x='conf', y='process_time', hue='total_date', hue_order=sorted(set(s_stat['total_date']), reverse=True))
        ax.set(xlabel='', ylabel='Process time (mins)')
        [ax.bar_label(ax.containers[i], fontsize=10) for i in range(len(set(s_stat['total_date'])))]
        sns.move_legend(ax, "lower center", bbox_to_anchor=(.5, 1), ncol=3, title='Total date', frameon=False)
        
        return fig