import redis
import os
import uuid
import json
from dotenv import load_dotenv
import time
load_dotenv()
import uuid
import json
import base64

redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB")),
    decode_responses=True
)

def determine_complexity(task_type: str, data: str) -> str:
    """
    Returns 'small' or 'large' based on type and payload size/characteristics.
    """
    if task_type == "text":
        # Count words
        word_count = len(data.split())
        return "large" if word_count > 200 else "small"

    elif task_type == "image":
        # Decode base64 image and check resolution
        try:
            img_bytes = base64.b64decode(data)
            import cv2
            import numpy as np
            npimg = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(npimg, cv2.IMREAD_COLOR)
            if img is not None:
                h, w = img.shape[:2]
                return "large" if (h > 1000 or w > 1000) else "small"
        except Exception:
            pass
        return "small"  # default fallback

    else:
        # Default rule for unknown task types
        return "small"

def enqueue_task(task_type: str, data):
    task_id = str(uuid.uuid4())

    # If the task is an image, ensure Base64-encoded string
    if task_type == "image":
        if isinstance(data, bytes):
            # Convert raw bytes to Base64 string
            data = base64.b64encode(data).decode("utf-8")
        elif not isinstance(data, str):
            raise ValueError("Image data must be bytes or Base64 string")

    elif task_type == "text":
        if not isinstance(data, str):
            raise ValueError("Text data must be a string")

    # Determine complexity
    complexity = determine_complexity(task_type, data)

    task_payload = {
        "task_id": task_id,
        "type": task_type,
        "data": data,  # Always Base64 for images, plain string for text
        "complexity": complexity
    }

    queue = f"{task_type}_queue"

    # Store metadata in Redis
    redis_conn.hset(f"task:{task_id}", mapping={
        "status": "pending",
        "payload": json.dumps(task_payload),
        "complexity": complexity,
        "retries": 0,
        "start_time": 0,
        "end_time": 0,
        "result": ""  # Use empty string for results instead of 0
    })

    # Push task to the correct queue
    redis_conn.rpush(queue, json.dumps(task_payload))

    return task_id


def get_status(task_id: str):
    status = redis_conn.hget(f"task:{task_id}", "status")
    if status:
        return {"task_id": task_id, "status": status}
    return {"task_id": task_id, "status": "not found"}

def get_result(task_id: str):
    # Get only the 'result' field from Redis
    result_data = redis_conn.hget(f"task:{task_id}", "result")
    if not result_data:
        return {"task_id": task_id, "status": "not found"}

    # Decode if it's JSON, otherwise return as is
    try:
        result_data = json.loads(result_data)
    except (json.JSONDecodeError, TypeError):
        pass  # keep it as string if not JSON

    return {"task_id": task_id, "result": result_data}