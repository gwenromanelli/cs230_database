import os
import json
from fastapi import FastAPI
from utils.node import Node
import time

app = FastAPI()

config_path = os.path.join(os.path.dirname(__file__), "node.config.json")
with open(config_path, "r") as f:
    config = json.load(f)

node1 = Node(
    DB_HOST=config["DB_HOST"],
    DB_NAME=config["DB_NAME"],
    DB_USER=config["DB_USER"],
    DB_PASSWORD=config["DB_PASSWORD"],
    DB_PORT=config.get("DB_PORT", 5432),
    NODE_ID=config["NODE_ID"],
    own_url=config["own_url"],
    local_state=config.get("local_state", {}),
    OTHER_NODES=config.get("OTHER_NODES", [{"url": "http://127.0.0.1:8000", "node_id": 2}])
)

app.include_router(node1.router)
