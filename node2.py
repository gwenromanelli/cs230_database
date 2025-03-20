import os
import json
from fastapi import FastAPI
from utils.node import Node
import uvicorn
import asyncio

#This should go in the engine class but idgaf anymore
stopped = False
async def engine(node): 
    try:
        while not stopped:
            await node.snapshot_listen()
            await asyncio.sleep(node.refresh_rate)

    except Exception as e:
        print(f"Exception occured while listening at node {node.NODE_ID}: {e}")

def run():
    asyncio.run(engine(node))


# Getting config for node on this instance
config_path = os.path.join(os.path.dirname(__file__), "node2.config.json")
with open(config_path, "r") as f:
    config = json.load(f)


# Creating fastapi instance
node2 = FastAPI()
node = Node(config, config["OTHER_NODES"])
node2.include_router(node.router)

@node2.get("/")
def main():
    return {"message": "Hello World"}

async def main():
    asyncio.create_task(engine(node))
    config = uvicorn.Config(node2, host='0.0.0.0',port=8001,reload=True)
    server = uvicorn.Server(config)
    await server.serve()
    stopped = True


if __name__ == "__main__":     
    asyncio.run(main())