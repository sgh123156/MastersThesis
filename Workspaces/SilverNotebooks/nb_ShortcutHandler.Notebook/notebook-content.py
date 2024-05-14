# CELL ********************

!pip install semantic-link --q 

# CELL ********************

import json
import time
import datetime

import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException, WorkspaceNotFoundException

# CELL ********************

class ShorcutHandler:

    def __init__(self, source_itemid, dest_itemid, workspaceid, dest_name, source_name, dest_path, source_path):
        self.source_itemid = source_itemid
        self.dest_itemid = dest_itemid
        self.workspaceid = workspaceid
        self.dest_name = dest_name
        self.source_name = source_name
        self.dest_path = dest_path
        self.source_path = source_path
        self.target_watermark = datetime.now()

        self.client = fabric.FabricRestClient()

    def analyze(self):
        try:
            #  Call Fabric REST API to check if a desired shortcut already exists 
            response = self.client.get(f'/v1/workspaces/{self.workspaceid}/items/{self.dest_itemid}/shortcuts/{self.dest_path}/{self.dest_name}')

            if response.status_code == 200:
                print('Shortcut with provided details already exists.')
                print('Checking if it is compatible with the request...')
                response = json.loads(response.content)

                # Inspect properties of an exisitng shortcut with desired ones
                test = (response['target']['oneLake']['path'], response['target']['oneLake']['itemId'])  \
                    == (self.source_path + "/" + self.source_name, self.source_itemid )

                if not test:
                    print('Current shortcut is not valid. It must be re-created.')
                    self.drop_shortcut()

                    # Pause the processing for 30 seconds to ensure the shortcut is deleted before re-creating it
                    time.sleep(30)
                    self.create_shortcut()

                else: 
                    print('Current shortcut is valid.')
                    
        except FabricHTTPException as e: 
            if f"Shortcut {self.dest_name} is not found" in str(e):
                print('No matching shortcut detected.')

                # Create one if not found
                self.create_shortcut()

            else: 
                # Raise errors if unexpected ones occur
                raise e

    def create_shortcut(self):
        
            body = {
                "path": self.dest_path,
                "name": self.dest_name,
                "target": {
                    "oneLake": {
                    "workspaceId": self.workspaceid,
                    "itemId": self.source_itemid,
                    "path": f"{self.source_path}/{self.source_name}"
                    }
                }
            }
            print('Initializing creation of a new shortcut...')

            # Call Fabric REST API to create a new shortcut, if it exits - throw an error
            response = self.client.post(f"/v1/workspaces/{self.workspaceid}/items/{self.dest_itemid}/shortcuts?ShortcutConflictPolicy=Abort", json=body)
            
            print(f'Creation of a new shortcut {self.dest_name} finished.')

    def drop_shortcut(self):

            print('Initializing deletion of an old shortcut...')

            # Call Fabric REST API to drop an existing shortcut if no longer needed or obsolete
            response = self.client.delete(f'/v1/workspaces/{self.workspaceid}/items/{self.dest_itemid}/shortcuts/{self.dest_path}/{self.dest_name}')
            
            print(f'Deletion of an old shortcut {self.dest_name} finished.')    
