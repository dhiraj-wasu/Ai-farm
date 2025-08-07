import redis
import os
import uuid
import json
from dotenv import load_dotenv
import time
load_dotenv()

redis_conn = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT")),
    db=int(os.getenv("REDIS_DB")),
    decode_responses=True
)

def enqueue_task(task_type: str, data: str):
    task_id = str(uuid.uuid4())
    task_payload = {
        "task_id": task_id,
        "type": task_type,
        "data": data
    }
    queue = f"{task_type}_queue"
    redis_conn.hset(f"task:{task_id}", mapping={
    "status": "pending",
    "payload": json.dumps(task_payload),
    "retries": 0,
    "start_time": 0,
    "end_time": 0,
    "result": None
     })

    redis_conn.rpush(queue, json.dumps(task_payload))
    return task_id

def get_status(task_id: str):
    status = redis_conn.hget(f"task:{task_id}", "status")
    if status:
        return {"task_id": task_id, "status": status}
    return {"task_id": task_id, "status": "not found"}

def get_result(task_id: str):
    task_data = redis_conn.get(task_id)
    if task_data:
        task_info = json.loads(task_data)
        if task_info["status"] == "completed":
            return {"task_id": task_id, "result": task_info["result"]}
        else:
            return {"task_id": task_id, "status": "not completed"}
    return {"task_id": task_id, "status": "not found"}