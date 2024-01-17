package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/weppos/publicsuffix-go/publicsuffix"
)

func main() {
	// Input and output directories
	inputDir := "test-data"
	outputDir := "tokens"

	// Process files in batches of 10
	batchSize := 10

	// Read input files from the input directory
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
			processFile(filePath, outputDir, batchSize)
		}(filePath)
	}

	wg.Wait()
	fmt.Println("Processing complete.")
}

func processFile(filePath, outputDir string, batchSize int) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Create output directory if it doesn't exist
	outputPath := outputDir
	err = os.MkdirAll(outputPath, 0755)
	if err != nil {
		fmt.Println("Error creating output directory:", err)
		return
	}

	// Create a map to store records for each public suffix
	outputFiles := make(map[string]*os.File)

	// Read CSV data
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 3

	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error reading CSV records:", err)
		return
	}

	// Process records
	for _, record := range records {
		dnsName := record[0]

		// Extract public suffix using the publicsuffix library
		domain, err := publicsuffix.Parse(dnsName)
		if err != nil {
			fmt.Println("Error extracting public suffix:", err)
			continue
		}

		// Create or open the output file for the public suffix
		outputFilePath := filepath.Join(outputPath, fmt.Sprintf("dot_%s.csv", domain.TLD))
		outputFile, ok := outputFiles[domain.TLD]
		if !ok {
			outputFile, err = os.OpenFile(outputFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				fmt.Println("Error creating/opening output file:", err)
				return
			}
			defer outputFile.Close()
			outputFiles[domain.TLD] = outputFile
		}

		// Write the record to the output file
		outputFile.WriteString(strings.Join(record, ",") + "\n")
	}

	// Close all output files
	for _, file := range outputFiles {
		file.Close()
	}
}
