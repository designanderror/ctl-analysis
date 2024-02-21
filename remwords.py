import os
import csv
import re
from collections import defaultdict

# Function to read CSV file, filter out forbidden words, sort based on frequency, and save the filtered data to a new CSV file
def filter_and_save_csv(input_file_path, output_file_path, forbidden_words):
    filtered_data = defaultdict(int)
    with open(input_file_path, 'r', encoding='latin-1') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                if len(row) >= 2:  # Check if there are at least two columns
                    name = row[0].strip().lower()
                    freq = int(row[1])
                    names = re.findall(r'\S+', name)  # Extract non-space sequences
                    filtered_names = []
                    for name in names:
                        # Check if the length is greater than 2
                        if len(name) > 2:
                            # Check if the name contains any forbidden words
                            contains_forbidden = any(word in name for word in forbidden_words)
                            if not contains_forbidden:
                                filtered_names.append(name)
                    if filtered_names:
                        for filtered_name in filtered_names:
                            filtered_data[filtered_name] += freq  # Increment frequency

    # Sort filtered data based on frequency (in descending order)
    sorted_data = sorted(filtered_data.items(), key=lambda x: x[1], reverse=True)

    # Write sorted data to output CSV file
    with open(output_file_path, 'w', newline='', encoding='latin-1') as file:
        writer = csv.writer(file)
        for name, freq in sorted_data:
            writer.writerow([name, freq])

# Main function
def main():
    input_dir = 'frequency_names'  # Directory containing input CSV files
    output_dir = 'filtered_sorted'  # Directory to save filtered and sorted CSV files
    forbidden_words = {
        "blog", "mail", "webmail", "nic", "dev", "test", "prod", "staging", "jenkins", "help",
        "admin", "cdn", "dashboard", "lab", "beta", "api", "nginx", "sql", "mysql", "db",
        "smtp", "pop", "imap", "cpanel", "domain", "frontend", "demo", "node", "gitlab",
        "web", "system", "site", "shop", "www", "app", "stage",
        "aaa", "root", "int", "deploy", "grafana", "log", "temp", "job",
        "lms", "myftp", "ftp", "download", "link", "inbox", "cms", "data", "epp",
        "whois", "pdf", "login", "auth", "microsoft365", "m365", "office",
        "git", "github", "gitlab", "jenkins", "terraform", "ldap", "ssl", "update",
        "archive", "img", "image", "registration", "faq", "autodiscover", "plugin",
        "outlook", "myaccount"
    }
    os.makedirs(output_dir, exist_ok=True)
    for filename in os.listdir(input_dir):
        if filename.endswith('.csv'):
            input_file_path = os.path.join(input_dir, filename)
            output_file_path = os.path.join(output_dir, filename)
            filter_and_save_csv(input_file_path, output_file_path, forbidden_words)

if __name__ == "__main__":
    main()
