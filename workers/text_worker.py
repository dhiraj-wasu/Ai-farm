import json
import time
import threading
from app.redis_queue import redis_conn
from transformers import pipeline

print("Text Worker: loading model...")
model = pipeline("sentiment-analysis")

MAX_RETRIES = 3
TIMEOUT_LIMIT = 30  # seconds

WORKER_ID = f"text-worker-{int(time.time())}"
tasks_in_progress = 0

def heartbeat():
    while True:
        redis_conn.hset(f"worker:{WORKER_ID}", mapping={
            "last_seen": time.time(),
            "tasks_in_progress": tasks_in_progress
        })
        redis_conn.expire(f"worker:{WORKER_ID}", 10)
        time.sleep(5)

threading.Thread(target=heartbeat, daemon=True).start()

while True:
    _, task_json = redis_conn.brpop("text_queue")
    task = json.loads(task_json)
    task_id = task["id"]
    task_key = f"task:{task_id}"

    print(f"Processing task {task_id}")
    tasks_in_progress += 1

    retries = int(redis_conn.hget(task_key, "retries") or 0)

    start_time = redis_conn.hget(task_key, "start_time")
    if start_time and (time.time() - float(start_time)) > TIMEOUT_LIMIT:
        print(f"Task {task_id} timed out")
        if retries < MAX_RETRIES:
            redis_conn.hincrby(task_key, "retries", 1)
            redis_conn.rpush("text_queue", json.dumps(task))
        else:
            redis_conn.hset(task_key, "status", "timeout")
            redis_conn.rpush("dead_letter_queue", json.dumps(task))
        tasks_in_progress -= 1
        continue

    redis_conn.hset(task_key, mapping={
        "status": "processing",
        "start_time": time.time(),
        "retries": retries
    })

    try:
        result = model(task["data"])[0]

        redis_conn.hset(task_key, mapping={
            "status": "done",
            "end_time": time.time(),
            "result": json.dumps(result)
        })
        print(f"✅ Task {task_id} completed")
    except Exception as e:
        print(f"Error in task {task_id}: {e}")
        if retries < MAX_RETRIES:
            redis_conn.hincrby(task_key, "retries", 1)
            redis_conn.rpush("text_queue", json.dumps(task))
        else:
            redis_conn.hset(task_key, "status", "failed")
            redis_conn.rpush("dead_letter_queue", json.dumps(task))

    tasks_in_progress -= 1
