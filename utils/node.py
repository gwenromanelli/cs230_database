import os
import psycopg2
import requests
import uuid
import json
from fastapi import FastAPI, HTTPException, Request, APIRouter
from pydantic import BaseModel

class Node():
    def __init__(self, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, NODE_ID, local_state, OTHER_NODES = []):
        self.DB_HOST = DB_HOST
        self.DB_NAME = DB_NAME
        self.DB_USER = DB_USER
        self.DB_PASSWORD = DB_PASSWORD
        self.DB_PORT = DB_PORT
        self.OTHER_NODES = OTHER_NODES
        self.NODE_ID = NODE_ID
        self.local_state = local_state
        self.snapshots = {}
        self.recorded_snapshot = set()
        self.in_transit_messages = {}
        self.master = None

        self.router = APIRouter()
        self.router.add_api_route("/", self.read_root, methods=["GET"])
        self.router.add_api_route("/start_snapshot", self.start_snapshot, methods=["POST"])
        self.router.add_api_route("/receive_marker", self.receive_marker, methods=["POST"])
        self.router.add_api_route("/send_data", self.send_data, methods=["POST"])
        self.router.add_api_route("/snapshots", self.get_snapshots, methods=["GET"])
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
        self.recorded_snapshot.add(snapshot_id)
        self.snapshots[snapshot_id]= {
            "node": self.NODE_ID,
            "node_state": self.local_state.copy(),
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

    def start_election(self):
        election_id = str(uuid.uuid4())
        self.master = None
        

    def send_votes(self, snapshot_id: str):
    for node_url in self.OTHER_NODES:
        try:
            requests.post(f"{node_url}/receive_marker", json={
                "snapshot_id": snapshot_id,
                "origin_node": self.NODE_ID
            })

        except Exception as e:
            print(f"Failed to send marker to {node_url}: {e}")

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