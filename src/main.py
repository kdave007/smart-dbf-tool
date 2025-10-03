from utils.table_spec_manager import TableSpecManager
import json

def main():
    # Create an instance of TableSpecManager
    spec_manager = TableSpecManager(None, None)
    
    # Test with some table names from setup.json
    table_names = ["VENTA", "PARTVTA", "CANOTA", "CUNOTA"]
    
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