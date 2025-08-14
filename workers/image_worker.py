import json
import cv2
import numpy as np
import base64
import time
import threading
from app.redis_queue import redis_conn

print("Image Worker: loading model...")
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

MAX_RETRIES = 3
TIMEOUT_LIMIT = 30  # seconds

WORKER_ID = f"image-worker-{int(time.time())}"
tasks_in_progress = 0

# Heartbeat sender
def heartbeat():
    global tasks_in_progress
    while True:
        redis_conn.hset(f"worker:{WORKER_ID}", mapping={
            "last_seen": time.time(),
            "tasks_in_progress": tasks_in_progress
        })
        redis_conn.expire(f"worker:{WORKER_ID}", 10)  # Mark offline if no update for >10s
        time.sleep(5)

# Start heartbeat thread
threading.Thread(target=heartbeat, daemon=True).start()

while True:
    _, task_json = redis_conn.brpop("image_queue")
    task = json.loads(task_json)
    task_id = task["task_id"]
    task_key = f"task:{task_id}"

    print(f"Processing task {task_id}")
    tasks_in_progress += 1

    # CHANGE ✅ — Decode retries to int safely
    retries = int(redis_conn.hget(task_key, "retries") or 0)

    # CHANGE ✅ — Decode start_time to float only if exists
    start_time_val = redis_conn.hget(task_key, "start_time")
    start_time = float(start_time_val) if start_time_val else None

    if start_time and (time.time() - start_time) > TIMEOUT_LIMIT:
        print(f"Task {task_id} timed out")
        if retries < MAX_RETRIES:
            redis_conn.hincrby(task_key, "retries", 1)
            redis_conn.rpush("image_queue", json.dumps(task))
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
        # CHANGE ✅ — Ensure base64 decoding works properly
        img_data = base64.b64decode(task["data"])
        npimg = np.frombuffer(img_data, dtype=np.uint8)
        img = cv2.imdecode(npimg, cv2.IMREAD_COLOR)

        faces = face_cascade.detectMultiScale(img, 1.1, 4)
        result = {"faces_detected": len(faces)}

        # CHANGE ✅ — Store result as JSON string in Redis
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
            redis_conn.rpush("image_queue", json.dumps(task))
        else:
            redis_conn.hset(task_key, "status", "failed")
            redis_conn.rpush("dead_letter_queue", json.dumps(task))

    tasks_in_progress -= 1
