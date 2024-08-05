import json
import csv
import pandas as pd

months = [4,5,6,7]

def main(month):
    my_json_file = f'hashicorp_vault_0{month}-2024_yearly_activity_report_prod-0{month + 1}-01-2024.json'
    output_csv_file = f'yearly_output_0{month}.csv'
    
    print(my_json_file)
    print('hello!')
    
    # Load the JSON file
    with open(my_json_file) as json_file:
        data = json.load(json_file)

        csv_data = []
        headers = ['namespace_id', 'namespace_path', 'mount_path', 'distinct_entities', 'entity_clients', 'non_entity_tokens', 'non_entity_clients', 'clients']
        csv_data.append(headers)

        for namespace in data['data']['by_namespace']:
            namespace_id = namespace['namespace_id']
            namespace_path = namespace['namespace_path']
            for mount in namespace['mounts']:
                mount_path = mount['mount_path']
                counts = mount['counts']
                row = [
                    namespace_id, 
                    namespace_path, 
                    mount_path, 
                    counts['distinct_entities'], 
                    counts['entity_clients'], 
                    counts['non_entity_tokens'], 
                    counts['non_entity_clients'], 
                    counts['clients']
                ]
                csv_data.append(row)
    
    with open(output_csv_file, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(csv_data)

    print(f"CSV file created: {output_csv_file}")
    
    # Load the CSV file
    data = pd.read_csv(output_csv_file)

    # Filter the dataframe to keep only rows where 'namespace_path' contains 'managed-vault'
    filtered_data = data[data['namespace_path'].str.contains('managed-vault', na=False)]

    # Group by 'mount_path' and calculate the totals for each distinct value
    totals = filtered_data.groupby('mount_path')[['clients']].sum().reset_index()

    # Print the result as a simple table
    print(totals.to_string(index=True))

if __name__ == '__main__':
    for i in months:
        main(i)
