import json
import time
import subprocess
from app.redis_queue import redis_conn

# Worker scaling limits (per type)
MAX_WORKERS = 8
MIN_WORKERS = 1
BASE_WORKERS = 2

# Task complexity weighting
WEIGHT_SMALL = 1
WEIGHT_LARGE = 3

# Downscale control
DOWNSCALE_IDLE_CYCLES = 3  # how many idle checks before reducing workers

# Backlog scaling factor
BACKLOG_FACTOR = 0.7  # start scaling up when queue > 70% of capacity

# Cooldown between scaling changes (seconds)
COOLDOWN_SECONDS = 60

idle_counters = {
    "image_worker": 0,
    "text_worker": 0
}

current_workers = {
    "image_worker": BASE_WORKERS,
    "text_worker": BASE_WORKERS
}

last_scale_time = {
    "image_worker": 0,
    "text_worker": 0
}

def get_queue_load(queue_name):
    """Get total weighted load of tasks in a queue."""
    tasks = redis_conn.lrange(queue_name, 0, -1)
    total_weight = 0
    for t in tasks:
        try:
            task = json.loads(t)
            if task.get("complexity") == "large":
                total_weight += WEIGHT_LARGE
            else:
                total_weight += WEIGHT_SMALL
        except:
            total_weight += WEIGHT_SMALL
    return total_weight

def scale_worker_type(worker_name, target_count):
    """Scale a single worker type with cooldown check."""
    now = time.time()

    # Respect cooldown
    if now - last_scale_time[worker_name] < COOLDOWN_SECONDS:
        print(f"⏳ Cooldown active for {worker_name}, skipping scaling change.")
        return

    if target_count != current_workers[worker_name]:
        subprocess.run([
            "docker-compose",
            "up",
            "--scale", f"{worker_name}={target_count}",
            "-d"
        ])
        print(f"🔄 {worker_name}: {current_workers[worker_name]} → {target_count}")
        current_workers[worker_name] = target_count
        last_scale_time[worker_name] = now

if __name__ == "__main__":
    while True:
        for worker_name, queue_name in [
            ("image_worker", "image_queue"),
            ("text_worker", "text_queue")
        ]:
            load = get_queue_load(queue_name)

            if load == 0:
                idle_counters[worker_name] += 1
                if idle_counters[worker_name] >= DOWNSCALE_IDLE_CYCLES:
                    scale_worker_type(worker_name, MIN_WORKERS)
            else:
                idle_counters[worker_name] = 0

                # Apply backlog factor for earlier scaling
                if load < int(5 * BACKLOG_FACTOR):
                    scale_worker_type(worker_name, BASE_WORKERS)
                else:
                    target = min(MAX_WORKERS, BASE_WORKERS + load // 3)
                    scale_worker_type(worker_name, target)

        time.sleep(10)
