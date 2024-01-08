import os
import pandas as pd
import tldextract
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

        # Extract TLD using tldextract library
        extracted_info = tldextract.extract(dns_name)
        tld = extracted_info.suffix

        # Check if TLD is in Public Suffix List
        if psl.get_public_suffix(tld) == tld:
            tld_key = f"dot_{tld}"
        else:
            tld_key = "others"

        # Add the DNS name to the corresponding DataFrame
        if tld_key not in tld_dataframes:
            tld_dataframes[tld_key] = pd.DataFrame(columns=['dns-name'])

        # Remove TLD from the domain
        domain_without_tld = f"{extracted_info.subdomain}.{extracted_info.domain}"
        tld_dataframes[tld_key] = pd.concat([tld_dataframes[tld_key], pd.DataFrame({'dns-name': [domain_without_tld]})], ignore_index=True)
        
        # Increment the processed rows counter
        processed_rows += 1

        # Append results after every 10 rows
        if processed_rows % 10 == 0:
            for tld_key, tld_df in tld_dataframes.items():
                output_file_path = os.path.join(result_folder, f"{tld_key}.csv")
                mode = 'a' if os.path.exists(output_file_path) else 'w'  # Use 'w' if file doesn't exist, 'a' if it does
                tld_df.to_csv(output_file_path, mode=mode, header=False, index=False)
                tld_dataframes[tld_key] = pd.DataFrame(columns=['dns-name'])  # Reset DataFrame after appending

    # Append any remaining results
    for tld_key, tld_df in tld_dataframes.items():
        output_file_path = os.path.join(result_folder, f"{tld_key}.csv")
        mode = 'a' if os.path.exists(output_file_path) else 'w'  # Use 'w' if file doesn't exist, 'a' if it does
        tld_df.to_csv(output_file_path, mode=mode, header=False, index=False)
        
    print(f"Processed {processed_rows} rows.")

# Process all CSV files in the specified directory
directory_path = "test-data"
result_folder = "tokens"

# Create result folder if it doesn't exist
os.makedirs(result_folder, exist_ok=True)

for filename in os.listdir(directory_path):
    if filename.endswith(".csv"):
        file_path = os.path.join(directory_path, filename)
        process_csv(file_path, result_folder)
