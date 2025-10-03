import os
import json

class TableSpecManager:
    def __init__(self, setup_path, data_tables_path) -> None:
        # Get the directory where this file is located (utils directory)
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.setup_path = os.path.join(utils_dir, "setup.json")
        self.data_tables_path = os.path.join(utils_dir, "data_tables_schemas.json")

    def get_spec(self, targets):
        """
            loops array of table names and returns their specs object
        """
        # Fetch schemas once for all tables
        schemas = self._fetch_data_schemas()
        if not schemas:
            return []
        
        specs_by_table_name = []

        for target in targets:
            current = self._fetch_specs(target, schemas)
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
        