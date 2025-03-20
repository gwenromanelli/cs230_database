import os
import json
from fastapi import FastAPI
from utils.node import Node
import time
import threading

app = FastAPI()

config_path = os.path.join(os.path.dirname(__file__), "node.config.json")
with open(config_path, "r") as f:
    config = json.load(f)

t1 = threading.Thread(target=os.system, args=("uvicorn node1:app --reload --host 0.0.0.0 --port 8000",))
t2 = threading.Thread(target=os.system, args=("uvicorn node2:app --reload --host 0.0.0.0 --port 8001",))
t3 = threading.Thread(target=os.system, args=("uvicorn node2:app --reload --host 0.0.0.0 --port 8002",))

t1.start()
t2.start()
t3.start()

refresh_rate = 300 # 5 minutes

while(True):
    print("Listening")
    time.sleep(5)