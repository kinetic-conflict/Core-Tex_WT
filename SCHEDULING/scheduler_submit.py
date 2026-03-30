import requests
import json

SCHEDULER_URL = "http://127.0.0.1:8002"

command = input("Enter command to schedule: ").strip()

try:
    res = requests.post(
        f"{SCHEDULER_URL}/submit-task",
        json={"command": command},
        timeout=30
    )

    print(f"\nStatus code: {res.status_code}")
    print(f"Content-Type: {res.headers.get('content-type')}\n")

    try:
        print(json.dumps(res.json(), indent=2))
    except Exception:
        print("Response was not JSON:")
        print(res.text)

except Exception as e:
    print(f"Request failed: {e}")