

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


