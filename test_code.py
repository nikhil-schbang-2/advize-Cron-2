import json
import requests
import time


# API credentials
APIFY_TOKEN = "apify_api_eEAWIIBVlGdguTukdYxKPJZb878pI04c75e8"

# Task ID
TASK_ID = "nikhil.pandey~my-video-scrapper"
country = "IN"
page_id = "102396233156564"
# Start the task run

ACTOR_STATUS = "https://api.apify.com/v2/actor-runs/{}?token={}"
DATASET_ITEMS = "https://api.apify.com/v2/datasets/{}/items?token={}&clean=true"
RUN_ACTOR = "https://api.apify.com/v2/acts/{}/runs?token={}"

def start_apify_actor(iframe_url: str, media_type="video"):
    """Start the Facebook Ads Scraper task with custom input"""
    
    if not iframe_url:
        return "Not a valid url."

    # Construct Facebook Ads Library URL
    input_payload = {
        "media_type": media_type,
        "start_urls": [
            {
                "url": iframe_url,
                "method": "GET"
            }
        ]
    }
    
    # Start the task
    start_run_url = RUN_ACTOR.format(TASK_ID, APIFY_TOKEN)
    response = requests.post(start_run_url, json=input_payload)
    response.raise_for_status()
    
    run = response.json()
    run_id = run["data"]["id"]
    
    return run_id


def get_actor_status(run_id):
    """Wait for the task run to complete"""
    status = "RUNNING"
    while status in ["RUNNING", "READY", "PENDING"]:
        time.sleep(5)
        run_status_url = ACTOR_STATUS.format(run_id, APIFY_TOKEN)
        run_status = requests.get(run_status_url).json()
        status = run_status["data"]["status"]

    return run_status

def actor_dataset_items(run_id):
    """Download the results dataset"""
    status = "RUNNING"
    dataset_id = None

    while status in ["RUNNING", "READY", "PENDING"]:
        time.sleep(5)
        run_status_url = ACTOR_STATUS.format(run_id, APIFY_TOKEN)
        run_status = requests.get(run_status_url).json()
        status = run_status["data"]["status"]
        dataset_id = run_status.get("data", {}).get("defaultDatasetId")

    dataset_url = DATASET_ITEMS.format(dataset_id, APIFY_TOKEN)

    dataset = requests.get(dataset_url).json()
    return dataset

def media_scrapper(iframe_url, media_type = "video"):
    try:
        run_id = start_apify_actor(iframe_url, media_type)

        dataset = actor_dataset_items(run_id)
        return dataset[0]

    except Exception as error:
        print("Error: ", error)
        return None
    