import os
import pandas as pd
import logging
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(filename='process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_group(group):
    min_time = min(group['leafTime'])
    max_time = max(group['leafTime'])
    return pd.Series({'firstseen': min_time, 'lastseen': max_time})

def process_file(file_path, tld_output_dir, checkpoint_file):
    try:
        df = pd.read_csv(file_path, header=None, names=['cname', 'leafTime'])
        df['leafTime'] = pd.to_datetime(df['leafTime'], errors='coerce')
        df['cname'] = df['cname'].astype('category')
        
        grouped = df.groupby('cname').apply(process_group).reset_index()
        grouped.columns = ['cname', 'firstseen', 'lastseen']
        merged_df = pd.merge(df, grouped, on='cname')
        merged_df.sort_values(by=['cname', 'leafTime'], inplace=True)
        
        cname_entries = {}
        for index, row in merged_df.iterrows():
            cname = row['cname']
            if cname not in cname_entries:
                cname_entries[cname] = (row['firstseen'], row['lastseen'])

        output_file = os.path.join(tld_output_dir, os.path.basename(file_path))
        with open(output_file, 'w') as f:
            for cname, (firstseen, lastseen) in cname_entries.items():
                f.write(f"{cname};{firstseen};{lastseen}\n")

        with open(checkpoint_file, 'a') as cf:
            cf.write(file_path + '\n')
        logging.info(f"Processed file {file_path} successfully.")
        
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

def compare_timestamps_and_write_sorted(input_dir, output_dir, checkpoint_file, max_entries_per_file=100000):
    os.makedirs(output_dir, exist_ok=True)
    # Read checkpoint file to find already processed files
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as cf:
            processed_files = set(cf.read().splitlines())
    else:
        processed_files = set()

    tasks = []
    with ProcessPoolExecutor() as executor:
        for tld_dir in os.listdir(input_dir):
            tld_path = os.path.join(input_dir, tld_dir)
            if os.path.isdir(tld_path):
                tld_output_dir = os.path.join(output_dir, tld_dir)
                os.makedirs(tld_output_dir, exist_ok=True)
                
                for filename in os.listdir(tld_path):
                    if filename.endswith(".csv"):
                        file_path = os.path.join(tld_path, filename)
                        if file_path not in processed_files:
                            tasks.append(executor.submit(process_file, file_path, tld_output_dir, checkpoint_file))

        for task in tasks:
            task.result()

if __name__ == "__main__":
    input_dir = ""
    output_dir = ""
    checkpoint_file = "checkpoint.txt"
    compare_timestamps_and_write_sorted(input_dir, output_dir, checkpoint_file)

