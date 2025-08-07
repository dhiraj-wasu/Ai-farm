import json
import time
from app.redis_queue import redis_conn
from transformers import pipeline   

print("Text Worker: loading model...")
model = pipeline("sentiment-analysis")

while True:
    _, task_json = redis_conn.brpop("text_queue")
    task = json.loads(task_json)
    print(f"Processing task {task['id']}")
    redis_conn.hset(f"task:{task['id']}", "status", "processing")
    # Inference
    result = model(task["data"])[0]
    redis_conn.hset(f"task:{task['id']}", mapping={
    "status": "done",
    "end_time": time.time(),
    "result": json.dumps(result)
})

