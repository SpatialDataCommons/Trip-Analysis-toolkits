import numpy as np
from datetime import datetime, date, timedelta

def get_date_range(start_date, end_date):
    '''
        Input: '20221201', '20221231'
        Output: ['20221201', ..., '20221231']
    '''
    """
    Generate a list of date range
    Args:  
        start_date, end_date (String): A String of start and end date
    Returns: 
        date_range (list): A list of date range
    Examples:
        >>> get_date_range('20221201', '20221231')
        ['20221201', ..., '20221231']
    """
    try:
        start_date = date(int(start_date[:4]), int(start_date[4:6]), int(start_date[6:]))
        end_date = date(int(end_date[:4]), int(end_date[4:6]), int(end_date[6:]))
        delta = end_date - start_date 

        date_range = []
        for d in range(delta.days + 1):
            day = start_date + timedelta(days=d)
            date_range.append(day.strftime("%Y%m%d"))

        return date_range
    
    except Exception as e:
        print('Error:', e)

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

def remove_outlier(num_lst):
    """
    Remove outliers from list of numbers
    Args:  
        num_lst (list of number): A list of number
    Returns: 
        numbers (list): A list of number without outliers
    Examples:
        >>> remove_outlier([[1, 2, 2, 76, 132, 3, 5, 7])
        [1, 2, 2, 3, 5, 7]
    """
    num_lst = [n for n in num_lst if str(n) != 'nan']
    q1 = np.percentile(num_lst, 25)
    q3 = np.percentile(num_lst, 75)
    iqr = q3 - q1
    return [n for n in num_lst if (n > q1 - 1.5*iqr and n < q3 + 1.5*iqr)]

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



