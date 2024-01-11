import os
import pandas as pd
from publicsuffix2 import PublicSuffixList

# Initialize Public Suffix List
psl = PublicSuffixList()

# Function to process a CSV file
def process_csv(file_path, result_folder):
    df = pd.read_csv(file_path, header=None)

    # Create separate DataFrames for each TLD
    tld_dataframes = {}
    
    processed_rows = 0
    
    for index, row in df.iterrows():
        dns_name = str(row.iloc[0])  # Access the first (and only) column directly
        first_seen = row.iloc[1]  # Access the second column for "first seen"
        last_seen = row.iloc[2]   # Access the third column for "last seen"

        # Extract TLD using manual method
        dot_index = dns_name.rfind('.')
        tld = dns_name[dot_index + 1:]

        # Check if TLD is in Public Suffix List
        if psl.get_public_suffix(tld) == tld:
            tld_key = f"dot_{tld}"
        else:
            tld_key = "others"

        # Add the DNS name, first seen, and last seen to the corresponding DataFrame
        if tld_key not in tld_dataframes:
            tld_dataframes[tld_key] = pd.DataFrame(columns=['dns-name', 'first-seen', 'last-seen'])

        # Remove TLD from the domain
        domain_without_tld = dns_name[:dot_index] if dot_index != -1 else dns_name
        tld_dataframes[tld_key] = pd.concat([tld_dataframes[tld_key], pd.DataFrame({'dns-name': [domain_without_tld], 'first-seen': [first_seen], 'last-seen': [last_seen]})], ignore_index=True)
        
        # Increment the processed rows counter
        processed_rows += 1

        # Append results after every 10 rows
        if processed_rows % 10 == 0:
            for tld_key, tld_df in tld_dataframes.items():
                output_file_path = os.path.join(result_folder, f"{tld_key}.csv")
                mode = 'a' if os.path.exists(output_file_path) else 'w'  # Use 'w' if file doesn't exist, 'a' if it does
                tld_df.to_csv(output_file_path, mode=mode, header=False, index=False)
                tld_dataframes[tld_key] = pd.DataFrame(columns=['dns-name', 'first-seen', 'last-seen'])  # Reset DataFrame after appending
                print(f"I am running all fine ;)")

    # Append any remaining results
    for tld_key, tld_df in tld_dataframes.items():
        output_file_path = os.path.join(result_folder, f"{tld_key}.csv")
        mode = 'a' if os.path.exists(output_file_path) else 'w'  # Use 'w' if file doesn't exist, 'a' if it does
        tld_df.to_csv(output_file_path, mode=mode, header=False, index=False)
        
    print(f"Processed {processed_rows} rows.")

# Get user input for input and output directories
input_directory = input("Enter the input directory path: ")
output_directory = input("Enter the output directory path: ")

# Process all CSV files in the specified input directory
for filename in os.listdir(input_directory):
    if filename.endswith(".csv"):
        file_path = os.path.join(input_directory, filename)
        process_csv(file_path, output_directory)
