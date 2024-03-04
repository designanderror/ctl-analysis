import os
import csv

def count_name_frequency(file_path):
    name_frequency = {}
    
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            name = row[0]
            if name in name_frequency:
                name_frequency[name] += 1
            else:
                name_frequency[name] = 1
    
    return name_frequency

def process_files(input_dir, output_dir):
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Iterate over each file in the input directory
    for filename in os.listdir(input_dir):
        if filename.endswith(".csv"):
            input_file_path = os.path.join(input_dir, filename)
            output_filename = f"{os.path.splitext(os.path.basename(input_file_path))[0].split('_')[0]}_names_freq.csv"
            output_file_path = os.path.join(output_dir, output_filename)
            
            name_frequency = count_name_frequency(input_file_path)
            
            # Write results to output CSV file
            with open(output_file_path, 'w', newline='') as output_file:
                writer = csv.writer(output_file)
                for name, frequency in name_frequency.items():
                    writer.writerow([name, frequency])

def main():
    input_dir = 'ones'  # Update with your input directory
    output_dir = 'frequency'  # Update with your output directory
    process_files(input_dir, output_dir)

if __name__ == "__main__":
    main()
