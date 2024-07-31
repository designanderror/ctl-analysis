import os
import pandas as pd
import logging
import gc
import fcntl

# Set up logging
logging.basicConfig(filename='process.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def process_group(group):
    min_time = group['leafTime'].dropna().min()
    max_time = group['leafTime'].dropna().max()
    return pd.Series({'firstseen': min_time, 'lastseen': max_time})

def process_file(file_path, tld_output_dir, checkpoint_file, lock_file, chunk_size=100000):
    try:
        print(f"Current: {file_path}")
        logging.info(f"Starting to process file: {file_path}")   
        cname_entries = {}
        for chunk in pd.read_csv(file_path, header=None, names=['cname', 'leafTime'], chunksize=chunk_size):
            chunk['leafTime'] = pd.to_datetime(chunk['leafTime'], unit='s', errors='coerce')
            chunk['cname'] = chunk['cname'].astype('category')
            grouped = chunk.groupby('cname').apply(process_group).reset_index()
            grouped.columns = ['cname', 'firstseen', 'lastseen']
            merged_chunk = pd.merge(chunk, grouped, on='cname')
            merged_chunk.sort_values(by=['cname', 'leafTime'], inplace=True)
            for index, row in merged_chunk.iterrows():
                cname = row['cname']
                if cname not in cname_entries:
                    cname_entries[cname] = (row['firstseen'], row['lastseen'])
            del chunk, grouped, merged_chunk
            gc.collect()

        output_file = os.path.join(tld_output_dir, os.path.basename(file_path))
        with open(output_file, 'w') as f:
            for cname, (firstseen, lastseen) in cname_entries.items():
                f.write(f"{cname};{firstseen};{lastseen}\n")
              
        # Create or acquire the lock
        with open(lock_file, 'w') as lockf:
            fcntl.flock(lockf, fcntl.LOCK_EX)
            # Update checkpoint file
            with open(checkpoint_file, 'a') as cf:
                cf.write(file_path + '\n')
            fcntl.flock(lockf, fcntl.LOCK_UN)
          
        logging.info(f"Processed file {file_path} successfully.")
        
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}", exc_info=True)

def compare_timestamps_and_write_sorted(input_dir, output_dir, checkpoint_file, lock_file, max_entries_per_file=100000):
    os.makedirs(output_dir, exist_ok=True)
    processed_files = set()
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as cf:
            processed_files = set(cf.read().splitlines())
    
    #To avoid processing the currently active file
    active_files = set()
    if os.path.exists('process.log'):
        with open('process.log', 'r') as logf:
            lines = logf.readlines()
            for line in lines:
                if 'Starting to process file:' in line:
                    parts = line.split('Starting to process file:')
                    if len(parts) > 1:
                        active_files.add(parts[1].strip())

    for tld_dir in os.listdir(input_dir):
        tld_path = os.path.join(input_dir, tld_dir)
        if os.path.isdir(tld_path):
            tld_output_dir = os.path.join(output_dir, tld_dir)
            os.makedirs(tld_output_dir, exist_ok=True)
            
            for filename in os.listdir(tld_path):
                if filename.endswith(".csv"):
                    file_path = os.path.join(tld_path, filename)
                    if file_path not in processed_files and file_path not in active_files:
                        process_file(file_path, tld_output_dir, checkpoint_file, lock_file)

if __name__ == "__main__":
    input_dir = ""
    output_dir = ""
    checkpoint_file = "checkpoint.txt"
    lock_file = "lockfile.lck"
    compare_timestamps_and_write_sorted(input_dir, output_dir, checkpoint_file, lock_file)
