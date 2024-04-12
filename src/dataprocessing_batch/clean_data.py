import json
from datetime import datetime, timezone
from sklearn.impute import KNNImputer
import pandas as pd 
import numpy as np 

def serialize(data):
    """
    This function serializes the data by converting it 
    to a JSON string and then encoding it to bytes.

    Args:
    data: A dictionary containing the data to be serialized.

    Returns:
    A list of serialized data in bytes format.
    """
    headers = data['fields']
    serialized_data = []
    
    for entry in data['data']:
        try:
            # Create a dictionary for each entry, matching fields with values
            entry_data = {headers[i]: entry[i] for i in range(len(headers))}
            # Convert the dictionary to a JSON string and then encode it to bytes
            entry_bytes = json.dumps(entry_data).encode('utf-8')
            serialized_data.append(entry_bytes)
        except IndexError:
            # This block catches cases where the entry might not have all the fields
            print("IndexError with entry:", entry)
            continue

    return serialized_data

def deserialize(byte_objects_list):
    """
    This function deserializes the data by decoding the bytes
    it converts epoch time to a datetime object and converts
    "pm2.5_cf_1" to a float.
    
    Args:
    byte_objects_list: A list of byte objects to be deserialized.
    
    Returns:
    A list of dictionaries containing the deserialized data.
    """
    results = []  # List to hold the processed sensor data
    for byte_object in byte_objects_list:
        if byte_object:  # Check if byte_object is not empty
            sensor_data = json.loads(byte_object.decode('utf-8'))  # Decode and load JSON from bytes

            # Convert "pm2.5_cf_1" to a float, check if the value exists and is not None
            if 'pm2.5_cf_1' in sensor_data and sensor_data['pm2.5_cf_1'] is not None:
                sensor_data['pm2.5_cf_1'] = float(sensor_data['pm2.5_cf_1'])
            
            # Convert "date_created" from Unix epoch time to a datetime object, check if the value exists
            if 'date_created' in sensor_data and sensor_data['date_created'] is not None:
                sensor_data['date_created'] = datetime.fromtimestamp(sensor_data['date_created'], tz=timezone.utc)
            
            results.append(sensor_data)  # Add the processed data to the results list

    return results


def impute_data_with_knn(deserialized_data):
    """
    Takes a list of dictionaries from deserialized data, converts it into a DataFrame,
    performs KNN imputation, and converts it back into a list of dictionaries.

    Args:
    deserialized_data: A list of dictionaries containing sensor data.

    Returns:
    A list of dictionaries with imputed data.
    """
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(deserialized_data)

    # Ensure all numeric columns are in appropriate data types
    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column] = pd.to_numeric(df[column])
            except ValueError:
                continue  # Keep non-convertible columns as object if needed
    
    # Apply KNN imputer to numeric columns
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    imputer = KNNImputer(n_neighbors=5, weights='uniform')
    imputed_array = imputer.fit_transform(df[numeric_columns])
    
    # Update numeric columns in DataFrame with imputed values
    df[numeric_columns] = imputed_array

    # Convert DataFrame back to a list of dictionaries
    imputed_data = df.to_dict(orient='records')

    return imputed_data
