import pandas as pd
from pandas.core.frame import DataFrame

class Cluster:
    def cluster_by_duration(self, points: list, durations: DataFrame, duration: float) -> list:
        in_cluster = { point : False for point in points}
        clusters = []
        for point in points:
            if not in_cluster[point]:
                near_points = durations.loc[
                    (durations['a_location_id'] == point) & (durations['duration_hours'] <= duration)]
                nearest_point = None
                for _, row in near_points.iterrows():
                    if not in_cluster[row['b_location_id']]:
                        nearest_point = int(row['b_location_id'])
                        break
                if nearest_point:
                    cluster = [point]
                    cluster.append(nearest_point)
                    clusters.append(cluster)
                    in_cluster[point] = True
                    in_cluster[nearest_point] = True
            
        return clusters