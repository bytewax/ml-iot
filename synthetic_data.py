import openai
import json
from dotenv import load_dotenv
import os
from openai import OpenAI
import re

load_dotenv(".env")
# Load your API key from an environment variable or direct input (not secure)
openai_key = os.getenv("OPEN_AI_KEY")
# Define a function to load JSON data from a file
def load_json_file(filepath):
    with open(filepath, 'r') as file:
        data = json.load(file)
    return data

# Define a function to generate synthetic data using OpenAI based on JSON input
def generate_synthetic_data(client, data):
    try:
        # Construct a prompt considering the specific keys from your JSON schema
        prompt = f"""
            You are a helpful synthetic data generator that uses only the information
            given to generate a synthetic dataset in JSON format 
            Create a synthetic dataset based on the following specifications. 
            Ensure your synthetic data does not contain null or none values and the values
            are sensible and consistent with the sample provided.
            Your task is to generate a single entry for the 'data' field within a JSON object with the following information:
            JSON keys: 'api_version', 'time_stamp', 'data_time_stamp', 'max_age', 'firmware_default_version', 'fields', 'data'
            Your task is to return a synthetic dataset based on the 'fields' values and the 'data' provided.
            JSON 'fields' values: {data['fields']}
            JSON 'data' entry samples: {data['data'][0:500]}
            Other keys in the JSON object: 'api_version', 'time_stamp', 'data_time_stamp', 'max_age', 'firmware_default_version'
        """

        question = "Please generate a single synthetic data entry  based on the content provided."
        # Call the OpenAI API
        response =  client.chat.completions.create(
        model="gpt-4-turbo-preview",
        seed = 42, 
        temperature=0.1,
        messages=[
            {"role": "system", "content": "Your task is to generate \
             synthetic data in the form of an array based on the user specifications:"},
            {"role": "user", "content": question},
            {"role": "assistant", "content": prompt}
            ]
        )
        answer = response.choices[0].message.content
        #json_answer = json.loads(answer)
        return answer
    except Exception as e:
        return str(e)



def extract_and_load_json(text):
    # Regex pattern to find text within ```json``` tags, accounting for optional spaces after ```json
    pattern = r"```json\s*\n([\s\S]*?)\n```"
    matches = re.findall(pattern, text)
    
    json_objects = []

    for match in matches:
        try:
            # Load the JSON string into a Python dictionary
            json_object = json.loads(match)
            json_objects.append(json_object)
        except json.JSONDecodeError as e:
            # Capture the error if the JSON is invalid
            print(f"Error parsing JSON: {e}")

    return json_objects


# Main function to orchestrate the flow
def main():
    # Load data from a JSON file
    json_data = load_json_file('data.json')

    # Initialize the OpenAI client
    client = OpenAI(api_key=openai_key)

    all_data = []  # List to store all generated data entries

    # Generate multiple synthetic data entries
    for _ in range(500000):  
        synthetic_data = generate_synthetic_data(client, json_data)
        json_answer = extract_and_load_json(synthetic_data)

        # Only append the 'data' values from each generated entry
        for item in json_answer:
            all_data.append(item['data'][0])  # Append only the 'data' list from each item

    # Final JSON structure to be saved
    final_json = {
        'api_version': '1.0',  
        'time_stamp': json_data['time_stamp'],  
        'data_time_stamp': json_data['data_time_stamp'],  
        'max_age': json_data['max_age'],  
        'firmware_default_version': '1.0.0',  
        'fields': json_data['fields'], 
        'data': all_data  
    }

    # Write the final JSON to a file
    with open('synthetic_data.json', 'w') as f:
        json.dump(final_json, f, indent=4)

if __name__ == "__main__":
    main()