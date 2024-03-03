import os
import csv

# Function to search for specific words in a CSV file and save them to a new file along with their frequencies
def search_and_save_words(input_file_path, output_file_path, target_words):
    filtered_words = {}
    with open(input_file_path, 'r', encoding='latin-1') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                if len(row) >= 2:
                    word = row[0].strip()
                    freq = int(row[1])
                    if word in target_words:
                        if word in filtered_words:
                            filtered_words[word] += freq
                        else:
                            filtered_words[word] = freq

    if filtered_words:
        with open(output_file_path, 'w') as file:
            writer = csv.writer(file)
            for word, freq in filtered_words.items():
                writer.writerow([word, freq])

# Main function
def main():
    input_dir = 'filtered_sorted/filtered'  # Directory containing input CSV files
    output_dir = 'wordfiltered'  # Directory to save word filter files
    target_words = {"redpill", "incel", "mrp", "pua"}  # Words to search for
    os.makedirs(output_dir, exist_ok=True)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, filename)
            search_and_save_words(input_file_path, output_file_path, target_words)

if __name__ == "__main__":
    main()
