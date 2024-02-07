import os
import csv
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# Function to read CSV file and extract DNS first names
def read_csv(file_path):
    firstname = []
    with open(file_path, 'r', encoding='latin-1') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                firstname.append(row[0])
    return firstname

# Function to generate Word Cloud from DNS first names
def generate_wordcloud(firstname, output_path):
    if not firstname:  # Check if there are no DNS names
        print(f"Skipping {output_path} as it has zero DNS names.")
        return

    wordcloud = WordCloud(width=800, height=400, background_color='white')
    wordcloud.generate(' '.join(firstname))

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(output_path)
    plt.close()


# Function to process each CSV file and generate Word Cloud
# Function to process each CSV file and generate Word Cloud
def process_csv_files(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, f'{os.path.splitext(filename)[0]}.png')
            firstname = read_csv(input_file_path)
            if not firstname:  # If no DNS names extracted, skip the file
                print(f"No DNS names extracted from {filename}. Skipping...")
                continue
            print(f"Generating word cloud for {filename}...")
            generate_wordcloud(firstname, output_file_path)
            print(f"Word cloud generated for {filename}.")


# Main function
def main():
    input_dir = 'frequency_names'  # Directory containing CSV files
    output_dir = 'wordclouds'  # Directory to save Word Cloud images
    process_csv_files(input_dir, output_dir)

if __name__ == "__main__":
    main()