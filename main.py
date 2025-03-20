import os
import json
from fastapi import FastAPI
from utils.node import Node
from utils.engine import NodeEngine
import time
import threading
import uvicorn

# Getting config for node on this instance

config_path = os.path.join(os.path.dirname(__file__), "node.config.json")
with open(config_path, "r") as f:
    config = json.load(f)

# Creating fastapi instance
app = FastAPI()
node = Node(config)
app.include_router(node.router)

if __name__ == "__main__":     
    node_engine = NodeEngine(node)
    node_engine.start()
    #Main node
    uvicorn.run("main:app", host='0.0.0.0', port=8000, reload=True)
    
    node_engine.stopped = True