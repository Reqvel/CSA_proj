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
                        pair_point = None
                        for _, row in near_points.iterrows():
                            if not in_cluster[row['b_location_id']] and row['duration_hours'] != 0:
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
            point_added = False
            cluster_added = False
            for i, cluster in enumerate(clusters):
                if not cluster_filled[i]:
                    near_points = []
                    for point in cluster:
                        near_points_df = durations.loc[
                        (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration)]
                        near_points.append(set(near_points_df['b_location_id'].tolist()))
                    points_intersection = None
                    for iter_points in near_points:
                        if points_intersection == None:
                            points_intersection = iter_points
                            continue
                        points_intersection &= iter_points
                    if points_intersection:
                        for point in points_intersection:
                            if not in_cluster[point]:
                                cluster.append(point)
                                in_cluster[point] = True
                                point_added = True

                    if not point_added:
                        cluster_filled[cluster_i] = True
                
            if not point_added:
                new_cluster = get_pair()
                if new_cluster:
                    clusters.append(new_cluster)
                    cluster_added = True
                    cluster_i += 1
                    cluster_filled[cluster_i] = False
            
        return clusters