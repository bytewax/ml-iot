from river import anomaly

class AnomalyDetector:
    def __init__(self, n_trees=10, height=8, window_size=72, seed=11):
        self.detector = anomaly.HalfSpaceTrees(
            n_trees=n_trees,
            height=height,
            window_size=window_size,
            limits={'x': (0.0, 1200)},  # ensure these limits make sense for your data
            seed=seed
        )
    
    def update(self, data):
        # Check if 'pm1.0_cf_1' is not None and is a floatable type
        if data['pm1.0_cf_1'] is not None:
            try:
                value = float(data['pm1.0_cf_1'])
                score = self.detector.score_one({'x': value})
                self.detector.learn_one({'x': value})
                data['score'] = score
            except ValueError:
                print(f"Skipping entry, invalid data for pm1.0_cf_1: {data['pm1.0_cf_1']}")
        else:
            print(f"Skipping entry, missing data for pm1.0_cf_1: {data}")
        return data