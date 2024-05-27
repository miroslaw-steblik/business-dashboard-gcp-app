# import requests
# import json

# # Define the API URL
# api_url = "http://localhost:8000/api/public/health"

# # Define the source and destination IDs
# source_id = "6aab877d-0442-4964-9dac-5b791e228cbc"
# destination_id = "60b9acdf-7bd3-49ef-8f14-72fcbc76bb19"

# # Define the connection configuration
# config = {
#     "sourceId": source_id,
#     "destinationId": destination_id,
#     "syncCatalog": {
#         "streams": [
#             {"name": "client_aum"},
#             {"name": "client_fee"},
#             {"name": "client_main"}
#         ]
#     }
# }

# # Make the API request
# response = requests.post(api_url + "connections/create", data=json.dumps(config))

# # Print the response
# print(response.json())

###########################################################

import requests
import base64

# Define the API URL and endpoint
api_url = "http://localhost:8001/api/public/v1/jobs"


# Define your username and password
username = "airbyte"
password = "password"

# Encode the username and password in Base64
credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")

# Define the headers, including the Authorization header
headers = {
    "accept": "application/json",
    "content-type": "application/json",
        "user-agent": "string",
    "Authorization": f"Basic {credentials}"
}



# Define the payload with the connection ID
payload = {
    "jobType": "sync", 
    "connectionId": "dde4de92-cf7e-461a-b237-cf069ce07d4a"
}

# Make the POST request to the /jobs/syncs endpoint
response = requests.post(api_url, json=payload, headers=headers)

# Print the response
print(response.text)


