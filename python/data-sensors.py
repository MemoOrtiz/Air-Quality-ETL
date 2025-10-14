import os
from dotenv import load_dotenv
import requests

#Load the .env file
load_dotenv()

# Load API Key 
api_key = os.getenv('X_API_KEY')
if api_key:
    print("API Key loaded successfully.")


