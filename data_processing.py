import json
from datetime import datetime, timezone


def serialize(data):
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

def deserialize(key_bytes_payload_bytes):
    key_bytes, payload_bytes = key_bytes_payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    sensor_data = json.loads(payload_bytes) if payload_bytes else None
    
    # Convert "pm2.5_cf_1" to a float
    if 'pm2.5_cf_1' in sensor_data:
        sensor_data['pm2.5_cf_1'] = float(sensor_data['pm2.5_cf_1'])
    
    # Convert "date_created" from Unix epoch time to a datetime object
    if 'date_created' in sensor_data:
        sensor_data['date_created'] = datetime.fromtimestamp(sensor_data['date_created'], tz=timezone.utc)
    
    return key, sensor_data
