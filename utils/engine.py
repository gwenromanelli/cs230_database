import threading
import time
from utils import node

class NodeEngine(threading.Thread):
    def __init__(self, node: node, *args, **kwargs): 
        super().__init__(*args, **kwargs)
        self.node = node
        self.stopped = False

    def run(self, *args, **kwargs): 
        try:
            while not self.stopped:
                self.node.snapshot_listen()
                time.sleep(self.node.refresh_rate)

        except Exception as e:
            print(f"Exception occured while listening at node {self.node.NODE_ID}: {e}")
                    
    def snapshot_heuristic(self):
        #TODO: Implement a heuristic to determine the snapshot frequency from 1 to 10 in
        #ascending order of latency
        return 0


