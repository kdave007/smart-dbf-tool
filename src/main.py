from utils.table_spec_manager import TableSpecManager
from utils.table_list_manager import TableListManager
from utils.database_manager import DatabaseManager
import json

def main():
    # Test database operations
    print("=== Database Manager Test ===")
    
    with DatabaseManager() as db_manager:
        # Execute the configured action (create or delete tables)
        success = db_manager.execute_action()
        
        if success:
            print("Database operations completed successfully!")
        else:
            print("Some database operations failed.")
    
    print("\n" + "=" * 50)
    
    # Original table specification test
    print("=== Table Specifications Test ===")
    
    # Create instances of managers
    spec_manager = TableSpecManager(None, None)
    list_manager = TableListManager(None, None)
    
    # Get table names from setup.json based on execute action
    table_names = list_manager.get_list()

    print(f"tables {table_names}")
    
    # Get specifications for the tables
    specs = spec_manager.get_spec(table_names)
    
    # Print the results
    print("Table Specifications:")
    print("=" * 50)
    
    for spec in specs:
        if spec:
            print(f"\nTable: {spec['name']}")
            print("Complete JSON object:")
            print(json.dumps(spec, indent=2, ensure_ascii=False))
        else:
            print(f"Failed to get spec for table")
        print("-" * 50)

if __name__ == "__main__":
    main()