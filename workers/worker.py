# from app.redis_queue import redis_conn
# import time
# import json



# print("Worker is running...")

# while True:
#     _, task = redis_conn.brpop("task_queue")
#     task_data = json.loads(task)
#     task_id = task_data["task_id"]

#     print(f"Processing task {task_id}")
#     redis_conn.set(f"task:{task_id}", json.dumps({"status": "processing"}))

#     # Simulate work
#     time.sleep(3)

#     # Save result
#     result = {"output": f"Task {task_id} completed"}
#     redis_conn.set(f"result:{task_id}", json.dumps(result))
#     redis_conn.set(f"task:{task_id}", json.dumps({"status": "done"}))
