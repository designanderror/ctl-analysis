import os
import csv
from datetime import datetime

def process_file(input_path, output_path, error_log_path):
    try:
        with open(input_path, 'r') as input_file:
            lines = input_file.readlines()

        output_filename = f"{os.path.splitext(os.path.basename(input_path))[0].split('_')[1]}_names.csv"
        output_file_path = os.path.join(output_path, output_filename)

        with open(output_file_path, 'a', newline='') as output_file:
            csv_writer = csv.writer(output_file)
            for line in lines:
                parts = line.strip().split(',')
                domain_parts = parts[0].split('.')
                if domain_parts[0] == "*":
                    csv_writer.writerow([domain_parts[1], parts[1], parts[2]])
                elif domain_parts[0] == 'www':
                    csv_writer.writerow([domain_parts[1], parts[1], parts[2]])
                else:
                    csv_writer.writerow([domain_parts[0], parts[1], parts[2]])

    except Exception as e:
        log_error(error_log_path, f"Error processing file {input_path}: {str(e)}")

def log_error(log_path, error_message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, 'a') as log_file:
        log_file.write(f"{timestamp} - {error_message}\n")

def process_directory(input_directory, output_directory, error_log_path):
    try:
        os.makedirs(output_directory, exist_ok=True)
        for filename in os.listdir(input_directory):
            if filename.endswith(".csv"):
                input_path = os.path.join(input_directory, filename)
                process_file(input_path, output_directory, error_log_path)
    except Exception as e:
        log_error(error_log_path, f"Error processing directory {input_directory}: {str(e)}")

if __name__ == "__main__":
    input_directory = "psl"
    output_directory = "ones"
    error_log_path = "error_log.txt"

    process_directory(input_directory, output_directory, error_log_path)
