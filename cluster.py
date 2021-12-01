from pandas.core.frame import DataFrame
import random
import pandas as pd

class Cluster:
    def get_cluster_center(self):
        pass


    def get_clusters_df(
        self, 
        clusters: list, 
        not_clustered_points: list,
        table_name: str,
        get_location_info_by_id: callable) -> DataFrame:

        def get_random_color() -> str:
            return "#"+''.join([random.choice('0123456789ABCDEF') for _ in range(6)])

        id_list = []
        location_name_list = []
        lon_list = []
        lat_list = []
        population_list = []
        color_list = []
        size_list = []

        number_of_colors = len(clusters)
        colors = [get_random_color() for _ in range(number_of_colors)]
        not_clustered_point_color = '#FF00FF'
        point_size = 1;

        for i, cluster in enumerate(clusters):
            color = colors[i]
            for point in cluster:
                id, name, longitude, latitude, population = get_location_info_by_id(table_name, point)
                id_list.append(id)
                location_name_list.append(name)
                lon_list.append(longitude)
                lat_list.append(latitude)
                population_list.append(population)
                color_list.append(color)
                size_list.append(point_size)

        for point in not_clustered_points:
            id, name, longitude, latitude, population = get_location_info_by_id(table_name, point)
            id_list.append(id)
            location_name_list.append(name)
            lon_list.append(longitude)
            lat_list.append(latitude)
            population_list.append(population)
            color_list.append(not_clustered_point_color)
            size_list.append(point_size)

        data = {
            'id': id_list,
            'location_name': location_name_list,
            'lon': lon_list,
            'lat': lat_list,
            'population': population_list,
            'color': color_list,
            'size': size_list
        }

        return pd.DataFrame(data)  


    def cluster_by_duration(
        self, 
        points: list, 
        durations: DataFrame, 
        duration_hours: float, 
        table_name: str,
        get_location_info_by_id: callable) -> DataFrame:

        in_cluster = { point : False for point in points}
        clusters = []

        def get_pair():
            for point in points:
                if not in_cluster[point]:
                    near_points = durations.loc[
                        (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration_hours)]
                    if not near_points.empty:
                        pair_point = None
                        for _, row in near_points.iterrows():
                            b_id = row['b_location_id']
                            if b_id in in_cluster and (not in_cluster[b_id] and row['duration_hours'] != 0):
                                pair_point = int(row['b_location_id'])
                                break
                        if pair_point:
                            cluster = [point, pair_point]
                            in_cluster[point] = True
                            in_cluster[pair_point] = True
                            return cluster
            
            return None

        cluster_i = 0
        clusters.append(get_pair())
        point_added = True
        cluster_added = False
        cluster_filled = {cluster_i: False}

        while point_added or cluster_added:
            cluster_added = False
            for i, cluster in enumerate(clusters):
                while not cluster_filled[i]:
                    point_added = False
                    near_points = []
                    for point in cluster:
                        near_points_df = durations.loc[
                        (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration_hours)]
                        near_points.append(set(near_points_df['b_location_id'].tolist()))
                    points_intersection = None
                    for iter_points in near_points:
                        if points_intersection == None:
                            points_intersection = iter_points
                            continue
                        points_intersection &= iter_points
                    if points_intersection:
                        for point in points_intersection:
                            if (point in in_cluster) and (not in_cluster[point]):
                                cluster.append(point)
                                in_cluster[point] = True
                                point_added = True

                    if not point_added:
                        cluster_filled[cluster_i] = True
                
                new_cluster = get_pair()
                if new_cluster:
                    clusters.append(new_cluster)
                    cluster_added = True
                    cluster_i += 1
                    cluster_filled[cluster_i] = False

        not_clustered_points = []
        for point, is_in_cluster in list(in_cluster.items()):
            if not is_in_cluster:
                not_clustered_points.append(point)

        return self.get_clusters_df(clusters, not_clustered_points, table_name, get_location_info_by_id)