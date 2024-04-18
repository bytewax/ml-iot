import bytewax.operators as op

from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

from bytewax.inputs import StatelessSourcePartition, DynamicSource


import json
from bytewax.testing import run_main

import requests
import json
from datetime import datetime, timezone
from river import anomaly
from river import preprocessing
from river import stats

# Opening JSON file
url = 'https://raw.githubusercontent.com/bytewax/ml-iot/main/data.json'

resp = requests.get(url)
data = json.loads(resp.text)

temp_imputer = preprocessing.StatImputer(("temperature", stats.Mean()))
humidity_imputer = preprocessing.StatImputer(("humidity", stats.Mean()))
pressure_imputer = preprocessing.StatImputer(("pressure", stats.Mean()))
pm1_imputer = preprocessing.StatImputer(("pm1.0_cf_1", stats.Mean()))

class SerializedData(StatelessSourcePartition):
    """
    Emit serialized data directly for simplicity. This class will serialize
    each entry in the 'data' list by mapping it to the corresponding 'fields'.
    """
    def __init__(self, full_data):
        self.fields = full_data['fields']
        self.data_entries = full_data['data']
        self.metadata = {k: v for k, v in full_data.items() if k not in ['fields', 'data']}
        self._it = iter(self.data_entries)

    def next_batch(self):
        try:
            data_entry = next(self._it)
            # Map each entry in 'data' with the corresponding field in 'fields'
            data_dict = dict(zip(self.fields, data_entry))
            # Merge metadata with data_dict to form the complete record
            complete_record = {**self.metadata, **{"data": data_dict}}
            # Serialize the complete record
            serialized = json.dumps(complete_record).encode('utf-8')
            return [serialized]
        except StopIteration:
            raise StopIteration


class SerializedInput(DynamicSource):
    """
    Dynamic data source that partitions the input data among workers.
    """
    def __init__(self, data):
        self.data = data
        self.total_entries = len(data['data'])

    def build(self, step_id, worker_index, worker_count):
        # Calculate the slice of data each worker should handle
        part_size = self.total_entries // worker_count
        start = part_size * worker_index
        end = start + part_size if worker_index != worker_count - 1 else self.total_entries

        # Create a partition of the data for the specific worker
        # Note: This partitions only the 'data' array. Metadata and fields are assumed
        # to be common and small enough to be replicated across workers.
        data_partition = {
            "api_version": self.data['api_version'],
            "time_stamp": self.data['time_stamp'],
            "data_time_stamp": self.data['data_time_stamp'],
            "max_age": self.data['max_age'],
            "firmware_default_version": self.data['firmware_default_version'],
            "fields": self.data['fields'],
            "data": self.data['data'][start:end]
        }

        return SerializedData(data_partition)
    

def process_and_impute_data(byte_data):
    """Deserialize byte data, impute missing values, and prepare for stateful processing."""
    # Deserialize the byte data
    record = json.loads(byte_data.decode('utf-8'))
    sensor_data = record['data']
    key = str(sensor_data.get("sensor_index", "default"))

    # Impute missing values
    for item in [temp_imputer, humidity_imputer, pressure_imputer, pm1_imputer]:
      item.learn_one(sensor_data)
      sensor_data = item.transform_one(sensor_data)
    temp_imputer.learn_one(sensor_data)  # Update imputer with current data
    sensor_data = temp_imputer.transform_one(sensor_data)  # Impute missing values

    # Return the processed data with the key
    return (key, sensor_data)

class StatefulAnomalyDetector:
    """
    This class is a stateful object that encapsulates an anomaly detection model
    from the River library and provides a method that uses this model to detect
    anomalies in streaming data. The detect_anomaly method of this object is
    passed to op.stateful_map, so the state is maintained across calls to this
    method.
    """
    def __init__(self, n_trees=10, height=8, window_size=72, seed=11, limit=(0.0, 1200)):
        self.detector = anomaly.HalfSpaceTrees(
            n_trees=n_trees,
            height=height,
            window_size=window_size,
            limits={'pm1.0_cf_1': limit},  # Ensure these limits make sense for your data
            seed=seed
        )

    def detect_anomaly(self, key, data):
        """
        Detect anomalies in sensor data and update the anomaly score in the data.
        """
        value = data.get('pm1.0_cf_1')
        if value is not None:
            try:
                value = float(value)
                score = self.detector.score_one({'pm1.0_cf_1': value})
                self.detector.learn_one({'pm1.0_cf_1': value})
                data['anomaly_score'] = score
            except ValueError:
                print(f"Skipping entry, invalid data for pm1.0_cf_1: {data['pm1.0_cf_1']}")
                data['anomaly_score'] = None
        else:
            data['anomaly_score'] = None
        return key, data


def filter_high_anomaly(data_tuple):
    """Filter entries with high anomaly scores."""
    key, data = data_tuple
    # Check if 'anomaly_score' is greater than 0.7
    return data.get('anomaly_score', 0) > 0.7

# Setup the dataflow
flow = Dataflow("air-quality-flow")
inp = op.input("inp", flow, SerializedInput(data))
impute_deserialize = op.map("impute_deserialize", inp, process_and_impute_data)


# Add anomaly detection to the dataflow
anomaly_detector = StatefulAnomalyDetector()
detect_anomalies_step = op.stateful_map("detect_anomalies", impute_deserialize, anomaly_detector.detect_anomaly)

# Detect anomalies within threshold
filter_anomalies = op.filter("filter_high_anomalies", detect_anomalies_step, filter_high_anomaly)

# Output or further processing
op.inspect("inspect_filtered_anomalies", filter_anomalies)

run_main(flow)