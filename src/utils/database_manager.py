import os
import sqlite3
import json
from .table_list_manager import TableListManager
from .table_spec_manager import TableSpecManager

class DatabaseManager:
    def __init__(self, setup_path=None, data_tables_path=None):
        # Get the directory where this file is located (utils directory)
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.setup_path = os.path.join(utils_dir, "setup.json")
        self.data_tables_path = os.path.join(utils_dir, "data_tables_schemas.json")
        
        # Initialize managers
        self.list_manager = TableListManager(setup_path, data_tables_path)
        self.spec_manager = TableSpecManager(setup_path, data_tables_path)
        
        # Database connection
        self.connection = None
        self.db_path = None
        
    def _get_db_config(self):
        """Get database configuration from setup.json"""
        db_params = self.list_manager.get_db_params()
        return db_params
    
    def _create_db_path(self, db_name):
        """Create database path, defaulting to utils directory if path is null"""
        db_config = self._get_db_config()
        db_path = db_config.get('path')
        
        if db_path is None:
            # Default to utils directory
            utils_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = utils_dir
        
        return os.path.join(db_path, f"{db_name}.db")
    
    def connect_or_create_db(self):
        """Connect to existing database or create new one"""
        try:
            db_config = self._get_db_config()
            db_name = db_config.get('name', 'default_db')
            
            self.db_path = self._create_db_path(db_name)
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Connect to database (creates if doesn't exist)
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row  # Enable column access by name
            
            print(f"Connected to database: {self.db_path}")
            return True
            
        except Exception as e:
            print(f"Error connecting to database: {e}")
            return False
    
    def _map_column_type(self, json_type):
        """Map JSON column types to SQLite types"""
        type_mapping = {
            'TEXT': 'TEXT',
            'INTEGER': 'INTEGER',
            'REAL': 'REAL',
            'BLOB': 'BLOB',
            'NUMERIC': 'NUMERIC'
        }
        return type_mapping.get(json_type.upper(), 'TEXT')
    
    def _build_column_definition(self, column, is_primary_key=False):
        """Build SQL column definition from JSON column spec"""
        name = column.get('name')
        col_type = self._map_column_type(column.get('type', 'TEXT'))
        
        definition = f"{name} {col_type}"
        
        # Add constraints
        if column.get('not_null', False):
            definition += " NOT NULL"
        
        # Only add PRIMARY KEY if this is the designated primary key column
        if is_primary_key:
            definition += " PRIMARY KEY"
        
        if 'default' in column:
            default_value = column['default']
            if default_value == "CURRENT_TIMESTAMP":
                definition += " DEFAULT CURRENT_TIMESTAMP"
            elif isinstance(default_value, str):
                definition += f" DEFAULT '{default_value}'"
            else:
                definition += f" DEFAULT {default_value}"
        
        return definition
    
    def create_table(self, table_spec):
        """Create a table based on table specification"""
        if not self.connection:
            print("No database connection")
            return False
        
        try:
            table_name = table_spec['name']
            columns = table_spec['table_columns']
            
            # Find the primary key column based on schema type
            primary_key_column = self._get_primary_key_column(table_spec)
            
            # Build column definitions
            column_defs = []
            for column in columns:
                is_pk = (column.get('name') == primary_key_column)
                col_def = self._build_column_definition(column, is_pk)
                column_defs.append(col_def)
            
            # Create table SQL
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            sql += ",\n".join(f"    {col_def}" for col_def in column_defs)
            sql += "\n)"
            
            print(f"Creating table {table_name}...")
            print(f"SQL: {sql}")
            
            cursor = self.connection.cursor()
            cursor.execute(sql)
            self.connection.commit()
            
            print(f"Table {table_name} created successfully")
            return True
            
        except Exception as e:
            print(f"Error creating table {table_spec.get('name', 'unknown')}: {e}")
            return False
    
    def _get_primary_key_column(self, table_spec):
        """Determine the primary key column based on schema type from JSON"""
        schema_type = table_spec.get('schema')
        columns = table_spec.get('table_columns', [])
        
        # Get the schema definitions to find the correct primary key column
        schemas = self.spec_manager._fetch_data_schemas()
        if not schemas or schema_type not in schemas:
            # Fallback: find any column marked as pk in the table columns
            for column in columns:
                if column.get('pk', False):
                    return column.get('name')
            return 'id_corte'  # Ultimate fallback
        
        # Get the schema-specific columns and find the primary key
        schema_columns = schemas[schema_type].get('columns', [])
        for column in schema_columns:
            if column.get('pk', False):
                return column.get('name')
        
        # If no primary key found in schema, look in table columns
        for column in columns:
            if column.get('pk', False):
                return column.get('name')
        
        return 'id_corte'  # Ultimate fallback
    
    def drop_table(self, table_name):
        """Drop a table"""
        if not self.connection:
            print("No database connection")
            return False
        
        try:
            sql = f"DROP TABLE IF EXISTS {table_name}"
            
            print(f"Dropping table {table_name}...")
            
            cursor = self.connection.cursor()
            cursor.execute(sql)
            self.connection.commit()
            
            print(f"Table {table_name} dropped successfully")
            return True
            
        except Exception as e:
            print(f"Error dropping table {table_name}: {e}")
            return False
    
    def execute_action(self):
        """Execute the configured action (create or delete tables)"""
        if not self.connect_or_create_db():
            return False
        
        # Get the action and table list
        action = self.list_manager._fetch_action()
        table_names = self.list_manager.get_list()
        
        print(f"Executing action: {action}")
        print(f"Tables: {table_names}")
        
        if action == "create":
            return self._create_tables(table_names)
        elif action == "delete":
            return self._delete_tables(table_names)
        else:
            print(f"Unknown action: {action}")
            return False
    
    def _create_tables(self, table_names):
        """Create all tables in the list"""
        # Get specifications for all tables
        specs = self.spec_manager.get_spec(table_names)
        
        success_count = 0
        for spec in specs:
            if spec and self.create_table(spec):
                success_count += 1
        
        print(f"Created {success_count}/{len(table_names)} tables successfully")
        return success_count == len(table_names)
    
    def _delete_tables(self, table_names):
        """Delete all tables in the list"""
        success_count = 0
        for table_name in table_names:
            if self.drop_table(table_name):
                success_count += 1
        
        print(f"Deleted {success_count}/{len(table_names)} tables successfully")
        return success_count == len(table_names)
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
            print("Database connection closed")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
