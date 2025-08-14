from fastapi import FastAPI
from app.models import TaskRequest
from app.redis_queue import enqueue_task,get_status, get_result
from redis_queue import redis_conn
import time
app = FastAPI()

@app.post("/submit-task")
def submit_task(task: TaskRequest):
    task_id = enqueue_task(task.type, task.data)
    return {"task_id": task_id}

@app.get("/status/{task_id}")
def status(task_id: str):
    return get_status(task_id)


@app.get("/result/{task_id}")
def result(task_id: str):

    return get_result(task_id)

@app.get("/workers")
def get_workers():
    keys = redis_conn.keys("worker:*")
    workers = []
    now = time.time()

    for key in keys:
        info = redis_conn.hgetall(key)
        last_seen = float(info.get("last_seen", 0))
        status = "healthy" if now - last_seen <= 10 else "offline"
        workers.append({
            "id": key.split(":")[1],
            "last_seen": last_seen,
            "tasks_in_progress": int(info.get("tasks_in_progress", 0)),
            "status": status
        })

    return workers
