import os
import psycopg2
import requests
import uuid
import json
from fastapi import FastAPI, HTTPException, Request, APIRouter
from pydantic import BaseModel

class Node():
    def __init__(self, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, NODE_ID, own_url, local_state, OTHER_NODES = []):
        self.DB_HOST = DB_HOST
        self.DB_NAME = DB_NAME
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.DB_PORT = DB_PORT
        self.OTHER_NODES = OTHER_NODES
        self.NODE_ID = NODE_ID
        self.own_url = own_url
        self.local_state = local_state
        self.snapshots = {}
        self.recorded_snapshot = set()
        self.in_transit_messages = {}
        self.master = None

        self.router = APIRouter()

        #snapshotting endpoints
        self.router.add_api_route("/", self.read_root, methods=["GET"])
        self.router.add_api_route("/start_snapshot", self.start_snapshot, methods=["POST"])
        self.router.add_api_route("/receive_marker", self.receive_marker, methods=["POST"])
        self.router.add_api_route("/send_data", self.send_data, methods=["POST"])
        self.router.add_api_route("/snapshots", self.get_snapshots, methods=["GET"])

        #database endpoints
        self.router.add_api_route("/hc_facilities/{oshpd_id}/", self.query_put, methods=["PUT"])
        self.router.add_api_route("/hc_facilities/{oshpd_id}/", self.query_get, methods=["GET"])
    
    def get_db_connection(self):
        """Create a new database connection using environment variables."""
        return psycopg2.connect(
            host=self.DB_HOST,
            database=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASSWORD,
            port=self.DB_PORT
        )
    
    #start of snapshotting functions
    def send_markers(self, snapshot_id: str):
        for node_url in self.OTHER_NODES:
            try:
                requests.post(f"{node_url}/receive_marker", json={
                    "snapshot_id": snapshot_id,
                    "origin_node": self.NODE_ID
                })

            except Exception as e:
                print(f"Failed to send marker to {node_url}: {e}")

    def record_local_snapshot(self, snapshot_id: str):
        try:
            conn = self.get_db_connection()
            conn.set_session(isolation_level='REPEATABLE READ')
            cur = conn.cursor()
            cur.execute("SELECT total_number_beds FROM hc_facilities WHERE total_number_beds IS NOT NULL")
            data = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            snapshot_data = {
                "columns": colnames,
                "rows": data
            }
        except Exception as e:
            print(f"Error fetching snapshot data from DB: {e}")
            snapshot_data = {}
        finally:
            cur.close()
            conn.close()

        self.recorded_snapshot.add(snapshot_id)
        self.snapshots[snapshot_id]= {
            "node": self.NODE_ID,
            "node_state": snapshot_data,
            "in_transit": []
        }
        self.in_transit_messages[snapshot_id] = []
        print(f"[{self.NODE_ID}] Recorded local snapshot {snapshot_id}: {self.snapshots[snapshot_id]}")

    def start_snapshot(self):
        snapshot_id = str(uuid.uuid4())
        self.record_local_snapshot(snapshot_id)
        self.send_markers(snapshot_id)
        return {"status": "started_snapshot", "snapshot_id": snapshot_id}

    def receive_marker(self,data: dict):
        snapshot_id = data["snapshot_id"]
        origin_node = data["origin_node"]

        if snapshot_id not in self.recorded_snapshot:
            self.record_local_snapshot(snapshot_id)
            self.send_markers(snapshot_id)
        else:
            pass

        return {"status": "OK", "snapshot_id": snapshot_id}

    def send_data(self, data: dict, request: Request):
        message = {
            "from": request.client.host,
            "data": data
        }

        for snap_id in self.recorded_snapshot:
            self.in_transit_messages[snap_id].append(message)
        
        return {"status": "OK", "received": data}

    def get_snapshots(self):
        """Return all recorded snapshots from this node's perspective."""
        return {
            "snapshots": self.snapshots,
            "in_transit": self.in_transit_messages
        }
    #end of snapshotting functions

    def read_root(self):
        return {"I am node %s".format(self.NODE_ID)}


    def query_put(self, oshpd_id: int, payload: str, query: str):
        conn = self.get_db_connection()
        cur = conn.cursor()

        try:
            # Update query
            update_query = query
            cur.execute(update_query, (payload, oshpd_id))
            conn.commit()

            if cur.rowcount == 0:
                # If rowcount = 0, no rows were updated (the OSHPD_ID might not exist)
                raise HTTPException(status_code=404, detail="Record not found or OSHPD_ID does not exist")

            return {"message": "Record updated successfully"}
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cur.close()
            conn.close()

    def query_get(self, oshpd_id: int, query: str):
        """
        Get a single facility's record by OSHPD_ID.
        """
        conn = self.get_db_connection()
        cur = conn.cursor()
        try:
            select_query = query 
            cur.execute(select_query, (oshpd_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Facility not found")

            return {
                row
            }

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cur.close()
            conn.close()

    #election algorithm functions
    def start_election(self):
        election_id = str(uuid.uuid4())
        self.master = None

        larger_nodes = [node for node in self.OTHER_NODES if node["node_id"] > self.NODE_ID]
        if not larger_nodes:
            #no node has a higher ID, elect itself as master node
            self.master = self.NODE_ID
            self.send_master_message()
            print(f"[Node {self.NODE_ID}] is the elected master.")
            return {"message" : "node has been elected as master", "master": self.NODE_ID}
        else:
            for node in larger_nodes:
                try:
                    requests.post(f"{node['url']}/recieve_election", json={
                        "election_id": election_id,
                        "sender_id": self.NODE_ID,
                        "sender_url": self.own_url
                    })
                except Exception as e:
                    print(f"Failed to send election message to {node['url']}: {e}")
            print(f"[Node {self.NODE_ID}] Election started, waiting for votes.")
            return {"message": "Election started, waiting for votes", "election_id": election_id}
    
    def receive_election(self, data:dict):
        sender_id = data["sender_id"]
        election_id = data["election_id"]
        sender_url = data.get("sender_url")

        print(f"[Node {self.NODE_ID}] Got election start message from Node {sender_id}")
        
        if self.NODE_ID > sender_id:
            if sender_url:
                try:
                    response = requests.post(
                        f"{sender_url}/receive_vote",
                        json = {
                            "election_id": election_id,
                            "voter_id": self.NODE_ID
                        }
                    )
                    print(f"[Node {self.NODE_ID}] Sent vote to Node {sender_id}, response: {response.text}")
                except Exception as e:
                    print(f"[Node {self.NODE_ID}] Failed to send vote to {sender_url}: {e}")
            else:
                print(f"[Node {self.NODE_ID}] No sender URL; can't send vote to Node {sender_id}")
            
            self.start_election() #start election since self.NODE_ID is larger
        else:
            print(f"[Node {self.NODE_ID}] Not sending vote as my node id is lower than {sender_id}")
        return {"message": "Election message received", "election_id": election_id}
    

    def receive_vote(self, data:dict):
        voter_id = data['voter_id']
        election_id = data['election_id']
        print(f"[Node {self.NODE_ID}] Recieved vote from Node: {voter_id} for election id: {election_id}")
        return {"message": "Vote recieved", "election_id": election_id}
    
    
    def master_connfirmation(self):
        for node in self.OTHER_NODES:
            try:
                requests.post(
                    f"{node['url']}/recieve_master", 
                    json = {
                        "master_id": self.master
                    }
                )
            except Exception as e:
                print(f"Failed to send master annoucnemennt to {node['url']}: {e}")
    
    def receive_master(self, data:dict):
        self.master = data["master_id"]
        print(f"[Node {self.NODE_ID}] Recieved master announcement: master is Node {self.master}")
        return {"message": "Master announcement received", "master": self.master}

"""
    def send_votes(self, snapshot_id: str):
        for node_url in self.OTHER_NODES:
            try:
                requests.post(f"{node_url}/receive_marker", json={
                    "snapshot_id": snapshot_id,
                    "origin_node": self.NODE_ID
                })

            except Exception as e:
                print(f"Failed to send marker to {node_url}: {e}")
"""