from fastapi import FastAPI
from app.models import TaskRequest
from app.redis_queue import enqueue_task,get_status, get_result

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
