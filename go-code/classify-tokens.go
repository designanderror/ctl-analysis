package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/net/publicsuffix"
)

// Process a CSV file
func processCSV(filePath, resultFolder string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	df := make(map[string][]string)
	processedRows := 0

	reader := csv.NewReader(file)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error reading CSV row:", err)
			return
		}

		dnsName := row[0]
		firstSeen := row[1]
		lastSeen := row[2]

		// Extract TLD
		dotIndex := strings.LastIndex(dnsName, ".")
		if dotIndex == -1 {
			continue // Invalid DNS name, skip
		}
		tld := dnsName[dotIndex+1:]

		// Check if TLD is in Public Suffix List
		pslTLD, _ := publicsuffix.PublicSuffix(tld)
		var tldKey string
		if tld == pslTLD {
			tldKey = fmt.Sprintf("dot_%s", tld)
		} else {
			tldKey = "others"
		}

		// Add the DNS name, first seen, and last seen to the corresponding slice
		df[tldKey] = append(df[tldKey], fmt.Sprintf("%s,%s,%s", dnsName, firstSeen, lastSeen))

		// Increment the processed rows counter
		processedRows++

		// Append results after every 10 rows
		if processedRows%10 == 0 {
			for key, values := range df {
				outputFilePath := filepath.Join(resultFolder, fmt.Sprintf("%s.csv", key))
				appendToFile(outputFilePath, values)
				df[key] = nil // Reset slice after appending
			}
		}
	}

	// Append any remaining results
	for key, values := range df {
		outputFilePath := filepath.Join(resultFolder, fmt.Sprintf("%s.csv", key))
		appendToFile(outputFilePath, values)
	}

	fmt.Printf("Processed %d rows.\n", processedRows)
}

// Append data to a file
func appendToFile(filePath string, data []string) {
	// Ensure the directory structure exists
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			fmt.Println("Error creating directory:", err)
			return
		}
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, row := range data {
		writer.Write(strings.Split(row, ","))
	}
}

func main() {
	// Take input and output directories from the user
	inputDir := getUserInput("Enter the input directory: ")
	outputDir := getUserInput("Enter the output directory: ")

	// Process all CSV files in the specified input directory
	filePaths, err := filepath.Glob(filepath.Join(inputDir, "*.csv"))
	if err != nil {
		fmt.Println("Error reading input files:", err)
		return
	}

	var wg sync.WaitGroup

	for _, filePath := range filePaths {
		wg.Add(1)
		go func(filePath string) {
			defer wg.Done()

			// Process each input file
			processCSV(filePath, outputDir)
		}(filePath)
	}

	wg.Wait()
	fmt.Println("Processing complete.")
}

// Get user input
func getUserInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}
