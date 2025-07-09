import json

# File path
file_path = r"C:\Users\NEELAM\Downloads\BEL_quote-derivative.json"

# Load the JSON file
with open(file_path, 'r') as file:
    data = json.load(file)

# Extract unique keys to represent columns
def extract_keys(obj, parent_key=''):
    """
    Recursively extract all unique keys from the nested dictionary.
    """
    keys = set()
    for key, value in obj.items():
        new_key = f"{parent_key}.{key}" if parent_key else key
        keys.add(new_key)
        if isinstance(value, dict):
            keys.update(extract_keys(value, new_key))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    keys.update(extract_keys(item, new_key))
    return keys

# Extract columns and count them
columns = extract_keys(data)
columns_list = sorted(columns)
column_count = len(columns_list)

# Print results
print(f"Total columns: {column_count}")
for column in columns_list:
    print(column)

# Save columns to a file
output_file = r"C:\Users\NEELAM\Downloads\extracted_columns.txt"
with open(output_file, 'w') as file:
    file.write(f"Total columns: {column_count}\n")
    file.write("\n".join(columns_list))

print(f"\nExtracted columns saved to: {output_file}")
