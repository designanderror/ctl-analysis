package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/publicsuffix"
)

const batchSize = 100000

// Process a CSV file
func processCSV(filePath, resultFolder string, wg *sync.WaitGroup, ch chan<- map[string][]string) {
	defer wg.Done()

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

		dnsName := strings.TrimSpace(row[0])
		firstSeen, err := strconv.Atoi(strings.TrimSpace(row[1]))
		if err != nil {
			fmt.Printf("Error converting firstSeen to integer: %v\n", err)
			continue
		}
		lastSeen, err := strconv.Atoi(strings.TrimSpace(row[2]))
		if err != nil {
			fmt.Printf("Error converting lastSeen to integer: %v\n", err)
			continue
		}

		// Extract TLD
		dotIndex := strings.LastIndex(dnsName, ".")
		if dotIndex == -1 {
			continue // Invalid DNS name, skip
		}
		tld := strings.TrimSpace(dnsName[dotIndex+1:])

		// Check if TLD is in Public Suffix List
		pslTLD, pslValid := publicsuffix.PublicSuffix(tld)
		var tldKey string
		if pslValid && tld == pslTLD {
			tldKey = fmt.Sprintf("dot_%s", tld)
			df[tldKey] = append(df[tldKey], fmt.Sprintf("%s,%d,%d", dnsName, firstSeen, lastSeen))
		} else {
			tldKey = "others"
			df[tldKey] = append(df[tldKey], fmt.Sprintf("%s,%d,%d", dnsName, firstSeen, lastSeen))
		}

		processedRows++

		// Write in batches to reduce I/O operations
		if processedRows%batchSize == 0 {
			ch <- df
			df = make(map[string][]string)
		}
	}

	// Send the remaining result to the channel
	ch <- df
}

// Append data to a file
func appendToFile(filePath string, data map[string][]string) {
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

	for _, values := range data {
		for _, row := range values {
			writer.Write(strings.Split(row, ","))
		}
	}
}

func main() {
	// Set the maximum number of CPUs that can be executing simultaneously
	runtime.GOMAXPROCS(runtime.NumCPU())

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
	ch := make(chan map[string][]string, len(filePaths))

	// Start goroutines to process each file concurrently
	for _, filePath := range filePaths {
		wg.Add(1)
		go processCSV(filePath, outputDir, &wg, ch)
	}

	// Start a goroutine to close the channel when all processing is complete
	go func() {
		wg.Wait()
		close(ch)
	}()

	// Collect results from goroutines
	finalResult := make(map[string][]string)
	for result := range ch {
		for key, values := range result {
			finalResult[key] = append(finalResult[key], values...)
		}
		// Print a message after processing each batch
		fmt.Println("Processed a batch. Everything is running fine!")
	}

	// Write the final result to files
	for key, values := range finalResult {
		outputFilePath := filepath.Join(outputDir, fmt.Sprintf("%s.csv", key))
		appendToFile(outputFilePath, map[string][]string{key: values})
	}

	fmt.Println("Processing complete.")
}

// Get user input
func getUserInput(prompt string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(prompt)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}
