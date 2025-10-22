from utils.table_spec_manager import TableSpecManager
import json

spec_manager = TableSpecManager(None, None)
specs = spec_manager.get_spec(['CANOTA'])

for spec in specs:
    if spec:
        print(f"Table: {spec['name']}")
        print(f"Schema: {spec['schema']}")
        print("\nColumns with pk=true:")
        for col in spec['table_columns']:
            if col.get('pk', False):
                print(f"  - {col['name']}: pk={col.get('pk')}")
        
        # Simulate what create_table does
        columns = spec['table_columns']
        pk_columns = [col.get('name') for col in columns if col.get('pk', False)]
        print(f"\nPK columns found: {pk_columns}")
        print(f"Number of PK columns: {len(pk_columns)}")
