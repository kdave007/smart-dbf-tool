import os
import json

class TableListManager:
    def __init__(self, setup_path, data_tables_path) -> None:
        # Get the directory where this file is located (utils directory)
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.setup_path = os.path.join(utils_dir, "setup.json")
        self.data_tables_path = os.path.join(utils_dir, "data_tables_schemas.json")
    
    def get_list(self):
        """
        Gets the list of tables for the current execute action
        """
        # Get the current action to execute
        action = self._fetch_action()
        if not action:
            return []
        
        # Get the table list for that action
        return self._fetch_list(action)
    
    def _fetch_list(self, action):
        """
        Gets the list of tables for the specified action from setup.json
        """
        try:
            with open(self.setup_path, 'r') as file:
                setup_data = json.load(file)
            
            # Get the actions section
            actions = setup_data.get('actions', {})
            
            # Return the table list for the specified action
            return actions.get(action, [])
            
        except FileNotFoundError:
            print(f"Setup json file not found: {self.setup_path}")
            return []
        except json.JSONDecodeError:
            print(f"Invalid JSON in setup file: {self.setup_path}")
            return []

    def _fetch_action(self):
        try:
            with open(self.setup_path, 'r') as file:
                setup_data = json.load(file)
            
            # Get the execute action from actions
            actions = setup_data.get('actions', {})
            execute_action = actions.get('execute')
            
            return execute_action
            
        except FileNotFoundError:
            print(f"Setup json file not found: {self.setup_path}")
            return None
        except json.JSONDecodeError:
            print(f"Invalid JSON in setup file: {self.setup_path}")
            return None

    def get_db_params(self):
        """
        Gets the database parameters from setup.json
        """
        try:
            with open(self.setup_path, 'r') as file:
                setup_data = json.load(file)
            
            # Get the db section
            return setup_data.get('db', {})
            
        except FileNotFoundError:
            print(f"Setup json file not found: {self.setup_path}")
            return {}
        except json.JSONDecodeError:
            print(f"Invalid JSON in setup file: {self.setup_path}")
            return {}