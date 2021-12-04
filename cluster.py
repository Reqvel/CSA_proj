from pandas.core.frame import DataFrame
import pandas as pd

class Cluster:
    def get_cluster_centroid(self, lon: list, lat: list, population: list) -> tuple:
        cluster_lon = 0
        cluster_lat = 0
        cluster_population = 0
        for lo, la, pop in zip(lon, lat, population):
            cluster_lon += lo * pop
            cluster_lat += la * pop
            cluster_population += pop
        
        cluster_lon /= cluster_population
        cluster_lat /= cluster_population

        return cluster_lon, cluster_lat, cluster_population
        


    def get_clusters_df(
        self, 
        clusters: list, 
        not_clustered_points: list,
        table_name: str,
        get_location_info_by_id: callable) -> DataFrame:

        id_list = []
        location_name_list = []
        lon_list = []
        lat_list = []
        population_list = []
        color_info_list = []
        size_list = []

        clusters_num = len(clusters)
        colors_info = [f'Cluster #{i}' for i in range(clusters_num)]
        not_clustered_color_info = 'Not In Cluster'
        point_size = 3;
        centroid_size = 10;

        for i, cluster in enumerate(clusters):
            color_info = colors_info[i]
            cluster_id_list = []
            cluster_location_name_list = []
            cluster_lon_list = []
            cluster_lat_list = []
            cluster_population_list = []
            cluster_color_info_list = []
            cluster_size_list = []
            for point in cluster:
                id, name, longitude, latitude, population = get_location_info_by_id(table_name, point)
                cluster_id_list.append(id)
                cluster_location_name_list.append(name)
                cluster_lon_list.append(longitude)
                cluster_lat_list.append(latitude)
                cluster_population_list.append(population)
                cluster_color_info_list.append(color_info)
                cluster_size_list.append(point_size)
            
            cluster_lon, cluster_lat, cluster_population = self.get_cluster_centroid(
                                                                                cluster_lon_list, 
                                                                                cluster_lat_list, 
                                                                                cluster_population_list)

            id_list.extend([*cluster_id_list, 0])
            location_name_list.extend([*cluster_location_name_list, 'Centroid'])
            lon_list.extend([*cluster_lon_list, cluster_lon])
            lat_list.extend([*cluster_lat_list, cluster_lat])
            population_list.extend([*cluster_population_list, cluster_population])
            color_info_list.extend([*cluster_color_info_list, color_info])
            size_list.extend([*cluster_size_list, centroid_size])

        for point in not_clustered_points:
            id, name, longitude, latitude, population = get_location_info_by_id(table_name, point)
            id_list.append(id)
            location_name_list.append(name)
            lon_list.append(longitude)
            lat_list.append(latitude)
            population_list.append(population)
            color_info_list.append(not_clustered_color_info)
            size_list.append(point_size)

        data = {
            'id': id_list,
            'location_name': location_name_list,
            'lon': lon_list,
            'lat': lat_list,
            'population': population_list,
            'color_info': color_info_list,
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

        def check_if_within_duration(point_a, point_b):
            df = durations.loc[
                    (durations['a_location_id'] == point_a) & 
                    (durations['b_location_id'] == point_b) &
                    (durations['duration_hours'] <= duration_hours)]
            if df.empty: return False
            else: return True
            

        cluster_i = 0
        clusters.append(get_pair())
        point_added = True
        cluster_added = True

        while point_added or cluster_added:
            point_added = False
            cluster_added = False
            cluster = clusters[cluster_i]
            near_points = []
            for point in cluster:
                near_points_df = durations.loc[
                (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration_hours)]
                near_points.extend(near_points_df['b_location_id'].tolist())
            for near_point in near_points:
                if (near_point in in_cluster) and (not in_cluster[near_point]):
                    within_duration = True
                    for point in cluster:
                        within = check_if_within_duration(near_point, point)
                        if not within:
                            within_duration = False
                            break
                    if within_duration:
                        cluster.append(near_point)
                        in_cluster[near_point] = True
                        point_added = True
            if not point_added:
                new_cluster = get_pair()
                if new_cluster:
                    clusters.append(new_cluster)
                    cluster_added = True
                    cluster_i += 1
                  
        not_clustered_points = []
        for point, is_in_cluster in list(in_cluster.items()):
            if not is_in_cluster:
                not_clustered_points.append(point)
        
        return self.get_clusters_df(clusters, not_clustered_points, table_name, get_location_info_by_id)
