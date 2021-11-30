from pandas.core.frame import DataFrame

class Cluster:
    def cluster_by_duration(self, points: list, durations: DataFrame, duration: float) -> list:
        in_cluster = { point : False for point in points}
        clusters = []

        def get_pair():
            for point in points:
                if not in_cluster[point]:
                    near_points = durations.loc[
                        (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration)]
                    if not near_points.empty:
                        nearest_point = None
                        for _, row in near_points.iterrows():
                            if not in_cluster[row['b_location_id']]:
                                nearest_point = int(row['b_location_id'])
                                break
                        if nearest_point:
                            cluster = [point, nearest_point]
                            in_cluster[point] = True
                            in_cluster[nearest_point] = True
                            return cluster
            
            return None

        clusters.append(get_pair())
        i = 0
        cluster_filled = {i: False}
        added = True
        cluster_added = False

        while added or cluster_added:
            added = False
            cluster_added = False
            for j, cluster in enumerate(clusters):
                if not cluster_filled[j]:
                    near_points = []
                    for point in cluster:
                        near_points_df = durations.loc[
                        (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration)]
                        near_points.append(set(near_points_df['b_location_id'].tolist()))
                    points_intersection = None
                    for i_points in near_points:
                        if points_intersection == None:
                            points_intersection = i_points
                        points_intersection &= i_points
                    for point in points_intersection:
                        if not in_cluster[point]:
                            cluster.append(point)
                            in_cluster[point] = True
                            added = True

                    if not added:
                        cluster_filled[i] = True
                        i += 1
                        cluster_filled[i] = False
                
            if not added:
                new_cluster = get_pair()
                if new_cluster:
                    clusters.append(new_cluster)
                    cluster_added = True
            
        return clusters