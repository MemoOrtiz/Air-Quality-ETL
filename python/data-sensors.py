import os
from dotenv import load_dotenv
import requests

#Load the .env file
load_dotenv()

# Load API Key 
api_key = os.getenv('OPENAQ_API_KEY')
if api_key:
    print("API Key loaded successfully.")

# Use the API Key in a request header
headers = {
    'X-API-Key': api_key,  # Aquí sí usas el formato original
    'Content-Type': 'application/json'
}
# Example API endpoint
url = "https://api.openaq.org/v3/locations?bbox=-100.60,25.50,-99.95,25.85&limit=1000"
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print("Data fetched successfully")
else:
    print(f"Error: {response.status_code} - {response.text}")


