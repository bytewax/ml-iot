from clean_data import serialize, deserialize, impute_data_with_knn
import json 
from anomaly import AnomalyDetector
import requests 

if __name__=="__main__":

    # Opening JSON file
    url = 'https://raw.githubusercontent.com/bytewax/ml-iot/main/data.json'

    resp = requests.get(url)
    data = json.loads(resp.text)

    # Begin data processing
    # Serialize the data to bytes
    serialized_entries = serialize(data)
    # Deserialize the data and transform epoch  
    deserialized_data = deserialize(serialized_entries)

    # Perform KNN imputation on deserialized data
    imputed_data = impute_data_with_knn(deserialized_data)

    anomaly_detector = AnomalyDetector(n_trees=4, height=3, window_size=50, seed=11)

    # Iterate over each deserialized data entry
    for entry in imputed_data[0:10000]:
        updated_entry = anomaly_detector.update(entry)
        if updated_entry['score']>0.7:
            print(updated_entry)