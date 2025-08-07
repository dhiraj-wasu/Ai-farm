import json
import cv2
import numpy as np
from app.redis_queue import redis_conn
import base64
import time
print("Image Worker: loading model...")
face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')

while True:
    _, task_json = redis_conn.brpop("image_queue")
    task = json.loads(task_json)
    print(f"Processing task {task['id']}")
    redis_conn.hset(f"task:{task['id']}", "status", "processing")
    redis_conn.hset(f"task:{task['id']}", "start_time", time.time())

    # Decode base64 image
    img_data = base64.b64decode(task["data"])
    npimg = np.frombuffer(img_data, dtype=np.uint8)
    img = cv2.imdecode(npimg, cv2.IMREAD_COLOR)

    faces = face_cascade.detectMultiScale(img, 1.1, 4)
    result = {"faces_detected": len(faces)}
    redis_conn.hset(f"task:{task['id']}", mapping={
    "status": "done",
    "end_time": time.time(),
    "result": json.dumps(result)
   })

