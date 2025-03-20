import os
import psycopg2
import requests
import uuid
import json
from fastapi import FastAPI, HTTPException, Request, APIRouter
from pydantic import BaseModel
import time

class Node():
    def __init__(self, config, OTHER_NODES = []):
        #From config
        self.DB_HOST = config["DB_HOST"]
        self.DB_NAME = config["DB_NAME"]
        self.DB_USER = config["DB_USER"]
        self.DB_PASSWORD = config["DB_PASSWORD"]
        self.DB_PORT = config["DB_PORT"]
        self.NODE_ID = config["NODE_ID"]
        self.own_url = config["own_url"]

        #Defaults
        self.OTHER_NODES = OTHER_NODES
        self.local_state = {}
        self.snapshots = {}
        self.recorded_snapshot = set()
        self.in_transit_messages = {}
        self.master = None
        self.changes = {}

        self.router = APIRouter()


        #snapshotting endpoints
        self.router.add_api_route("/", self.read_root, methods=["GET"])
        self.router.add_api_route("/start_snapshot", self.start_snapshot, methods=["POST"])
        self.router.add_api_route("/receive_marker", self.receive_marker, methods=["POST"])
        self.router.add_api_route("/send_data", self.send_data, methods=["POST"])
        self.router.add_api_route("/snapshots", self.get_snapshots, methods=["GET"])

        #database changes endpoints
        self.router.add_api_route("/hc_facilities/{oshpd_id}/", self.query_put, methods=["PUT"])
        self.router.add_api_route("/hc_facilities/{oshpd_id}/", self.query_get, methods=["GET"])
        self.router.add_api_route("/trigger_send_changes", self.send_changes_to_master, methods=["POST"])
        self.router.add_api_route("/update_changes", self.update_changes, methods=["POST"])
        self.router.add_api_route("/local_changes", self.get_local_changes, methods=["GET"])

        #election endpoints 
        self.router.add_api_route("/start_election", self.start_election, methods=["POST"])
        self.router.add_api_route("/receive_election", self.receive_election, methods=["POST"])
        self.router.add_api_route("/receive_vote", self.receive_vote, methods=["POST"])
        self.router.add_api_route("/receive_master", self.receive_master, methods=["POST"])
        self.router.add_api_route("/master", self.master_node, methods=["GET"])

        #snapshot heuristic
        #TODO: change back to 30 seconds
        self.refresh_rates = [10*i for i in range(1,11)] # 30 seconds to 5 minutes
    
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
                requests.post(f"{node_url['url']}/receive_marker", json={
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


    #election algorithm functions
    def start_election(self):
        election_id = str(uuid.uuid4())
        self.master = None

        larger_nodes = [node for node in self.OTHER_NODES if node["node_id"] > self.NODE_ID]
        if not larger_nodes:
            #no node has a higher ID, elect itself as master node
            self.master = self.NODE_ID
            self.master_confirmation()
            print(f"[Node {self.NODE_ID}] is the elected master.")
            return {"message" : "node has been elected as master", "master": self.NODE_ID}
        else:
            for node in larger_nodes:
                try:
                    requests.post(f"{node['url']}/receive_election", json={
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
        print(f"[Node {self.NODE_ID}] Received vote from Node: {voter_id} for election id: {election_id}")
        return {"message": "Vote received", "election_id": election_id}
    
    
    def master_confirmation(self):
        for node in self.OTHER_NODES:
            try:
                requests.post(
                    f"{node['url']}/receive_master", 
                    json = {
                        "master_id": self.master
                    }
                )
            except Exception as e:
                print(f"Failed to send master annoucnemennt to {node['url']}: {e}")
    
    def receive_master(self, data:dict):
        self.master = data["master_id"]
        print(f"[Node {self.NODE_ID}] Received master announcement: master is Node {self.master}")
        return {"message": "Master announcement received", "master": self.master}
    
    def master_node(self):
        return {"message": "The master node is", "master": self.master}
    
    def is_master(self):
        return self.master == self.NODE_ID

    #end of election algorithms 

    def snapshot_listen(self):
        #Checking the refresh rate for listening
        self.refresh_rate = self.refresh_rates[self.snapshot_heuristic()]

        if self.master == None:
            print("No master node, starting election")
            self.start_election()

        if self.is_master():
            print("I am the master node {self.node.NODE_ID} \nSnapshotting...")
            self.start_snapshot()

        else:
            print("I am not the master node \nlistening...")
    

    def snapshot_heuristic(self):
        #TODO: Implement a heuristic to determine the snapshot frequency from 30 to 300 seconds
        return 0



    #functions for changing values 

    def query_put(self, oshpd_id: int, payload: str):
        """
        This function will update the facility records and track changes to the bed occupancy
        Each time it is called, it will either increment or decrement the occupancy count
        for the hospital within self.changes
        """
        query = "UPDATE hc_facilities SET total_number_beds = total_number_beds + %s WHERE oshpd_id = %s"
        conn = self.get_db_connection()
        cur = conn.cursor()

        try:
            cur.execute(query, (payload, oshpd_id))
            conn.commit()

            if cur.rowcount == 0:
                # If rowcount = 0, no rows were updated (the OSHPD_ID might not exist)
                raise HTTPException(status_code=404, detail="Record not found or OSHPD_ID does not exist")
            

            if oshpd_id not in self.changes:
                self.changes[oshpd_id] = 0
            self.changes[oshpd_id] += int(payload)    

            return {"message": "Record updated successfully"}
        
        except Exception as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cur.close()
            conn.close()
        

    def query_get(self, oshpd_id: int) -> dict:
        """
        Get a single facility's record by OSHPD_ID.
        """
        query = "SELECT total_number_beds FROM hc_facilities WHERE oshpd_id = %s"
        conn = self.get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(query, (oshpd_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Facility not found")

            return {"data" : row}

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cur.close()
            conn.close()
    
    def send_changes_to_master(self):
        if self.master is None:
            print("There is no master elected yet. No changes sent.")
            return {"message" : "No master."}
        if not self.changes:
            print("No new changes made to send.")
            return {"message" : "No new changes."}
        
        master_url = None
        for node in self.OTHER_NODES:
            if node["node_id"] == self.master:
                master_url = node["url"]
                break
                
        if master_url is None:
            print("Master URL not found.")
            return {"message" : "No Master URL found."}
        
        try:
            response = requests.post(
                f"{master_url}/update_changes",
                json = {
                    "sender_id": self.NODE_ID,
                    "changes": self.changes
                }
            )
            print(f"Sent changes to master: {self.changes}, response: {response.text}")
            #clear local changes
            self.changes.clear()
            return {"message" : "local changes sent to master and cleared."}
        except Exception as e:
            print(f"Failed to send changes to master: {e}")
            return {"message": f"Failed to send changes: {e}"}
    
    def update_changes(self, data:dict):
        """
        This function on the master node will receive the changes from 
        the other nodes. The master node can then update its global state and/or
        database. 
        """
        sender_id = data["sender_id"]
        changes = data["changes"]
        print(f"Master Node received changes from Node {sender_id}: {changes}")

        try:
            conn = self.get_db_connection()
            cur = conn.cursor()

            for hospital_id, change in changes.items():
                update_query = """
                    UPDATE hc_facilities 
                    SET total_number_beds = total_number_beds + %s
                    WHERE oshpd_id = %s
                """
                cur.execute(update_query, (change, hospital_id))
            conn.commit()
            return {"message": "Changes received and applied", "changes":changes}
        except Exception as e:
            conn.rollback()
            return {"message" : "Failed to update changes", "error":str(e)}
        finally:
            cur.close()
            conn.close()
    
    def get_local_changes(self):
        return {"local changes in dictionary": self.changes}

    #end of updating functions

 
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