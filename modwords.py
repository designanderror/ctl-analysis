import os
import csv, re
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt

forbidden_words = {"blog", "mail", "webmail", "nic", "dev", "test", "prod", "staging", "jenkins", "help",
                       "admin", "cdn", "dashboard", "lab", "beta", "api", "nginx", "sql", "mysql", "db",
                       "smtp", "pop", "imap", "cpanel", "domain", "frontend", "demo", "node", "gitlab",
                       "web", "system", "site", "shop", "www", "app", "stage"}


def read_csv(file_path):
    firstname = []
    with open(file_path, 'r', encoding='latin-1') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                # Use a pattern to match non-space characters instead of word boundaries
                names = re.findall(r'\S+', row[0].lower())
                for name in names:
                    # Check if the length is greater than 2
                    if len(name) > 2:
                        # Check if the name contains any forbidden words
                        contains_forbidden = any(word in name for word in forbidden_words)
                        if not contains_forbidden:
                            firstname.append(name)
                            #print(firstname)
    return firstname

def generate_wordcloud(firstname, output_path):
    wordcloud = WordCloud(width=800, height=400, background_color='white', stopwords=forbidden_words, regexp=r'\S+')
    wordcloud.generate(' '.join(firstname))
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.savefig(output_path)
    plt.close()

def process_csv_files(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, f'{os.path.splitext(filename)[0]}.png')
            firstname = read_csv(input_file_path)
            if not firstname:
                print(f"No DNS names extracted from {filename}. Skipping...")
                continue
            print(f"Generating word cloud for {filename}...")
            generate_wordcloud(firstname, output_file_path)
            print(f"Word cloud generated for {filename}.")


def main():
    input_dir = 'frequency_names'  
    output_dir = 'modwords'  
    process_csv_files(input_dir, output_dir)

if __name__ == "__main__":
    main()