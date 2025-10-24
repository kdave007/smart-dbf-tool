import os
import json

class TableSpecManager:
    def __init__(self, setup_path, data_tables_path) -> None:
        # Get the directory where this file is located (utils directory)
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.setup_path = os.path.join(utils_dir, "setup.json")
        self.data_tables_path = os.path.join(utils_dir, "data_tables_schemas.json")
        self.helper_tables_path = os.path.join(utils_dir, "helper_tables_schemas.json")

    def get_spec(self, targets):
        """
            loops array of table names and returns their specs object
        """
        # Fetch schemas once for all tables
        data_schemas = self._fetch_data_schemas()
        helper_schemas = self._fetch_helper_schemas()
        
        specs_by_table_name = []

        for target in targets:
            # First try to find in helper tables
            current = self._fetch_helper_specs(target, helper_schemas)
            if not current:
                # If not found in helper tables, try data tables
                current = self._fetch_specs(target, data_schemas)
            specs_by_table_name.append(current)
        
        return specs_by_table_name

    def _get_setup(self, table_name):
        """
        Reads setup.json and returns the table configuration object matching the given table name
        """
        try:
            with open(self.setup_path, 'r') as file:
                setup_data = json.load(file)
            
            # Search for the table in the tables array
            tables = setup_data.get('tables', [])
            for table in tables:
                if table.get('name') == table_name:
                    return table
            
            # Return None if table not found
            return None
            
        except FileNotFoundError:
            print(f"Setup json file not found: {self.setup_path}")
            return None
        except json.JSONDecodeError:
            print(f"Invalid JSON in setup file: {self.setup_path}")
            return None
    
    def _fetch_data_schemas(self):
        """
        Reads data_tables_schemas.json and returns combined schemas (common + specific schema types)
        """
        try:
            with open(self.data_tables_path, 'r') as file:
                data_schema = json.load(file)
            
            schemas = data_schema.get('schemas', {})
            common_columns = schemas.get('common', {}).get('columns', [])
            options = schemas.get('options', [])
            
            # Create combined schemas for each option
            combined_schemas = {}
            for schema_option in options:
                if schema_option in schemas:
                    specific_columns = schemas[schema_option].get('columns', [])
                    # Combine common columns with specific schema columns
                    combined_schemas[schema_option] = {
                        'columns': common_columns + specific_columns
                    }
            
            return combined_schemas
            
        except FileNotFoundError:
            print(f"data tables schema json file not found: {self.data_tables_path}")
            return None
        except json.JSONDecodeError:
            print(f"Invalid data tables JSON in setup file: {self.data_tables_path}")
            return None
    
    def _fetch_helper_schemas(self):
        """
        Reads helper_tables_schemas.json and returns the helper table schemas
        """
        try:
            with open(self.helper_tables_path, 'r') as file:
                helper_schema = json.load(file)
            
            return helper_schema.get('schemas', {})
            
        except FileNotFoundError:
            print(f"Helper tables schema json file not found: {self.helper_tables_path}")
            return {}
        except json.JSONDecodeError:
            print(f"Invalid helper tables JSON in file: {self.helper_tables_path}")
            return {}


    def _fetch_specs(self, table_name, schemas):
        """
            this will fetch and assemble the specs object of the table requested
            it will return :
            {
                name: target_table,
                schema: schema,
                table_params: table_params,
                table_columns: columns

            }
        """
        # Get table configuration from setup.json
        table_config = self._get_setup(table_name)
        if not table_config:
            return None
        
        # Get the schema type for this table
        schema_type = table_config.get('schema')
        if schema_type not in schemas:
            print(f"Schema type '{schema_type}' not found in data schemas")
            return None
        
        # Get the columns for this schema type
        schema_columns = schemas[schema_type].get('columns', [])
        
        # Add additional columns if specified
        additional_columns = table_config.get('additional_columns', [])
        all_columns = schema_columns + additional_columns
        
        # Filter out skip_columns if specified
        skip_columns = table_config.get('skip_columns', [])
        if skip_columns:
            all_columns = [col for col in all_columns if col.get('name') not in skip_columns]
        
        # Return the merged specification
        return {
            'name': table_config.get('name'),
            'schema': schema_type,
            'id_fields': table_config.get('id_fields', []),
            'table_columns': all_columns,
            'additional_columns': additional_columns,
            'skip_columns': skip_columns
        }
    
    def _fetch_helper_specs(self, table_name, helper_schemas):
        """
        Fetch specifications for helper tables that don't follow the data table schema pattern
        Returns the same format as _fetch_specs for consistency
        """
        if not helper_schemas or 'tables' not in helper_schemas:
            return None
        
        helper_tables = helper_schemas.get('tables', {})
        if table_name not in helper_tables:
            return None
        
        table_def = helper_tables[table_name]
        columns = table_def.get('columns', [])
        values = table_def.get('values', [])  # Get values if they exist
        
        # Helper tables don't use the schema system, so we return a simplified spec
        return {
            'name': table_name,
            'schema': 'helper',  # Mark as helper table
            'id_fields': [],
            'table_columns': columns,
            'additional_columns': [],
            'skip_columns': [],
            'values': values  # Include values for insertion
        }
        