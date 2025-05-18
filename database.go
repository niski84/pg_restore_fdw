package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
}

// ProgressMonitor tracks progress of database operations
type ProgressMonitor struct {
	Operation   string
	StartTime   time.Time
	LastUpdate  time.Time
	UpdateEvery time.Duration
}

func NewProgressMonitor(operation string) *ProgressMonitor {
	return &ProgressMonitor{
		Operation:   operation,
		StartTime:   time.Now(),
		LastUpdate:  time.Now(),
		UpdateEvery: 5 * time.Second,
	}
}

func (pm *ProgressMonitor) Update(status string) {
	now := time.Now()
	if now.Sub(pm.LastUpdate) >= pm.UpdateEvery {
		elapsed := now.Sub(pm.StartTime).Round(time.Second)
		log.Printf("[%s] %s (elapsed: %v)", pm.Operation, status, elapsed)
		pm.LastUpdate = now
	}
}

// RetryWithBackoff retries a function with exponential backoff
func RetryWithBackoff(operation string, maxAttempts int, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
			if attempt < maxAttempts {
				backoff := time.Duration(attempt*attempt) * time.Second
				log.Printf("Attempt %d/%d for %s failed: %v. Retrying in %v...",
					attempt, maxAttempts, operation, err, backoff)
				time.Sleep(backoff)
			}
		}
	}
	return fmt.Errorf("operation %s failed after %d attempts: %w",
		operation, maxAttempts, lastErr)
}

// CreateDatabase creates a new PostgreSQL database
func CreateDatabase(config DBConfig) error {
	log.Printf("Creating database: %s", config.DBName)

	cmd := exec.Command(
		"psql",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"-c", fmt.Sprintf("CREATE DATABASE %s;", config.DBName),
		"postgres", // Connect to default postgres database
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error creating database: %s", output)
		return fmt.Errorf("failed to create database: %w", err)
	}

	log.Printf("Database %s created successfully", config.DBName)
	return nil
}

// DumpWorkflow performs a complete dump of both moodys and tenant databases
func DumpWorkflow(moodysConfig, tenantConfig DBConfig, outputDir string) error {
	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Dump databases in sections with appropriate formats
	databases := []struct {
		config     DBConfig
		namePrefix string
	}{
		{moodysConfig, "moodys"},
		{tenantConfig, "tenant"},
	}

	sections := []string{"pre-data", "data", "post-data"}
	for _, db := range databases {
		for _, section := range sections {
			outFile := filepath.Join(outputDir, fmt.Sprintf("%s_%s", db.namePrefix, section))
			if err := dumpDatabaseSection(db.config, outFile, section); err != nil {
				return fmt.Errorf("failed to dump %s %s: %w", db.namePrefix, section, err)
			}
		}
	}

	return nil
}

// dumpDatabaseSection dumps a specific section of a database
func dumpDatabaseSection(config DBConfig, outputFile, section string) error {
	log.Printf("Dumping %s section of database %s to %s", section, config.DBName, outputFile)

	// Configure format based on section
	// Pre-data needs to be text format for FDW modification
	// Data and post-data can use custom format for parallel restore
	var format string
	fileExt := ".sql" // Default for text format
	if section == "pre-data" {
		format = "p" // plain text format
	} else {
		format = "c" // custom format for parallel restore
		fileExt = ".dump"
	}

	outputFile = outputFile + fileExt

	cmd := exec.Command(
		"pg_dump",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"--no-owner",
		"--no-privileges",
		fmt.Sprintf("-F%s", format), // Format type
		fmt.Sprintf("--section=%s", section),
		"-f", outputFile,
		config.DBName,
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error dumping database section: %s", output)
		return fmt.Errorf("failed to dump database section: %w", err)
	}

	log.Printf("Successfully dumped %s section of %s to %s", section, config.DBName, outputFile)
	return nil
}

// modifyPreDataFile modifies the tenant pre-data SQL file to update FDW configuration
func modifyPreDataFile(inputFile string, srcMoodysConfig, destMoodysConfig DBConfig) error {
	// Read the current content
	content, err := os.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("failed to read pre-data file: %w", err)
	}

	// Log original content
	log.Printf("Original pre-data file content:\n%s", string(content))

	// Replace the FDW configuration
	modified := string(content)

	// Update host, port, and dbname in SERVER options
	modified = strings.Replace(
		modified,
		fmt.Sprintf("dbname '%s'", srcMoodysConfig.DBName),
		fmt.Sprintf("dbname '%s'", destMoodysConfig.DBName),
		-1,
	)
	modified = strings.Replace(
		modified,
		fmt.Sprintf("host '%s'", srcMoodysConfig.Host),
		fmt.Sprintf("host '%s'", destMoodysConfig.Host),
		-1,
	)
	modified = strings.Replace(
		modified,
		fmt.Sprintf("port '%s'", srcMoodysConfig.Port),
		fmt.Sprintf("port '%s'", destMoodysConfig.Port),
		-1,
	)

	// Update user mapping options
	modified = strings.Replace(
		modified,
		fmt.Sprintf("user '%s'", srcMoodysConfig.User),
		fmt.Sprintf("user '%s'", destMoodysConfig.User),
		-1,
	)
	modified = strings.Replace(
		modified,
		fmt.Sprintf("password '%s'", srcMoodysConfig.Password),
		fmt.Sprintf("password '%s'", destMoodysConfig.Password),
		-1,
	)

	// Log modified content
	log.Printf("Modified pre-data file content:\n%s", modified)

	// Write the modified content back to the file
	if err := os.WriteFile(inputFile, []byte(modified), 0644); err != nil {
		return fmt.Errorf("failed to write modified pre-data file: %w", err)
	}

	return nil
}

// restoreDatabaseSection restores a specific section of a database with parallel processing
func restoreDatabaseSection(config DBConfig, inputFile string, section string) error {
	monitor := NewProgressMonitor(fmt.Sprintf("Restore %s", filepath.Base(inputFile)))
	monitor.Update("Starting restore...")
	startTime := time.Now()

	result := RetryWithBackoff(fmt.Sprintf("restore %s", inputFile), 3, func() error {
		var cmd *exec.Cmd

		// Use psql for pre-data (plain text) and pg_restore for data/post-data (custom format)
		if section == "pre-data" {
			cmd = exec.Command(
				"psql",
				"-h", config.Host,
				"-p", config.Port,
				"-U", config.User,
				"-d", config.DBName,
				"-f", inputFile,
			)
		} else {
			numCPUs := getNumCPUs()
			monitor.Update(fmt.Sprintf("Using %d parallel workers", numCPUs))
			cmd = exec.Command(
				"pg_restore",
				"-h", config.Host,
				"-p", config.Port,
				"-U", config.User,
				"-d", config.DBName,
				"--no-owner",
				"--no-privileges",
				"-j", fmt.Sprintf("%d", numCPUs),
				inputFile,
			)
		}

		cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

		// Log the command being executed (with password redacted)
		cmdStr := strings.Join(cmd.Args, " ")
		log.Printf("Executing: %s", cmdStr)

		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("failed to restore database section: %w\nOutput: %s", err, output)
		}

		monitor.Update("Restore completed successfully")
		return nil
	})

	duration := time.Since(startTime)

	// If this is a data section, get the record count
	if section == "data" {
		countCmd := exec.Command(
			"psql",
			"-h", config.Host,
			"-p", config.Port,
			"-U", config.User,
			"-d", config.DBName,
			"-t", // tuple only
			"-c", "SELECT COUNT(*) FROM customer_transactions;",
		)
		countCmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

		if output, err := countCmd.CombinedOutput(); err == nil {
			count := strings.TrimSpace(string(output))
			log.Printf("Restore completed in %v. Records restored: %s", duration, count)
		} else {
			log.Printf("Restore completed in %v. Could not get record count: %v", duration, err)
		}
	} else {
		log.Printf("Restore completed in %v", duration)
	}

	return result
}

// RestoreWorkflow restores both databases with proper FDW configuration
func RestoreWorkflow(srcMoodysConfig, srcTenantConfig, destMoodysConfig, destTenantConfig DBConfig, inputDir string) error {
	// Create destination databases
	if err := CreateDatabase(destMoodysConfig); err != nil {
		return fmt.Errorf("failed to create moodys database: %w", err)
	}
	if err := CreateDatabase(destTenantConfig); err != nil {
		return fmt.Errorf("failed to create tenant database: %w", err)
	}

	// Restore Moodys database first (it's the source for FDW)
	sections := []string{"pre-data", "data", "post-data"}
	for _, section := range sections {
		fileExt := ".dump"
		if section == "pre-data" {
			fileExt = ".sql"
		}
		inFile := filepath.Join(inputDir, fmt.Sprintf("moodys_%s%s", section, fileExt))
		if err := restoreDatabaseSection(destMoodysConfig, inFile, section); err != nil {
			return fmt.Errorf("failed to restore moodys %s: %w", section, err)
		}
	}

	// Modify tenant pre-data file to update FDW configuration
	tenantPreDataFile := filepath.Join(inputDir, "tenant_pre-data.sql")
	if err := modifyPreDataFile(tenantPreDataFile, srcMoodysConfig, destMoodysConfig); err != nil {
		return fmt.Errorf("failed to modify tenant pre-data file: %w", err)
	}

	// Restore Tenant pre-data first
	if err := restoreDatabaseSection(destTenantConfig, tenantPreDataFile, "pre-data"); err != nil {
		return fmt.Errorf("failed to restore tenant pre-data: %w", err)
	}

	// Restore remaining tenant sections
	for _, section := range []string{"data", "post-data"} {
		inFile := filepath.Join(inputDir, fmt.Sprintf("tenant_%s.dump", section))
		if err := restoreDatabaseSection(destTenantConfig, inFile, section); err != nil {
			return fmt.Errorf("failed to restore tenant %s: %w", section, err)
		}
	}

	return nil
}

// getNumCPUs returns the number of CPU cores available for parallel processing
func getNumCPUs() int {
	return 1 //runtime.NumCPU()
}

// DeleteDatabases ensures the databases are deleted if they exist
func DeleteDatabases(configs ...DBConfig) error {
	for _, config := range configs {
		if err := dropDatabase(config); err != nil {
			return fmt.Errorf("failed to drop database %s: %w", config.DBName, err)
		}
	}
	return nil
}

// dropDatabase drops a PostgreSQL database
func dropDatabase(config DBConfig) error {
	cmd := exec.Command(
		"psql",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"-c", "DROP DATABASE IF EXISTS "+config.DBName,
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to drop database %s: %v, output: %s", config.DBName, err, string(output))
	}
	return nil
}

// SetupSourceDatabases creates and populates the source databases
func SetupSourceDatabases(moodysConfig, tenantConfig DBConfig, numTestRecords int) error {
	// Create and populate source databases
	if err := CreateDatabase(moodysConfig); err != nil {
		return fmt.Errorf("failed to create source moodys database: %w", err)
	}
	if err := CreateSampleTable(moodysConfig); err != nil {
		return fmt.Errorf("failed to create sample table in moodys: %w", err)
	}
	if err := CreateDatabase(tenantConfig); err != nil {
		return fmt.Errorf("failed to create source tenant database: %w", err)
	}
	if err := SetupFDW(tenantConfig, moodysConfig); err != nil {
		return fmt.Errorf("failed to setup FDW: %w", err)
	}

	// Populate tenant database with test data
	if err := populateTestData(tenantConfig, numTestRecords); err != nil {
		return fmt.Errorf("failed to populate test data: %w", err)
	}

	return nil
}

// populateTestData fills the tenant database with test data
func populateTestData(config DBConfig, numRecords int) error {
	startTime := time.Now()
	log.Printf("Populating database %s with %d test records", config.DBName, numRecords)

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS customer_transactions (
			id SERIAL PRIMARY KEY,
			customer_id INTEGER,
			transaction_date TIMESTAMP,
			amount DECIMAL(10,2),
			description TEXT
		);
	`

	cmd := exec.Command(
		"psql",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"-d", config.DBName,
		"-c", createTableSQL,
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

	// Log the command being executed (with password redacted)
	cmdStr := strings.Join(cmd.Args, " ")
	log.Printf("Executing: %s", cmdStr)

	if output, err := cmd.CombinedOutput(); err != nil {
		log.Printf("Error creating table: %s", output)
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Generate and insert test data in batches to show progress
	batchSize := 1000000 // 1 million records per batch
	remainingRecords := numRecords
	insertedRecords := 0

	for remainingRecords > 0 {
		currentBatch := batchSize
		if remainingRecords < batchSize {
			currentBatch = remainingRecords
		}

		log.Printf("Inserting batch of %d records (%.1f%% complete)",
			currentBatch,
			float64(insertedRecords)/float64(numRecords)*100,
		)

		insertSQL := fmt.Sprintf(`
			INSERT INTO customer_transactions (customer_id, transaction_date, amount, description)
			SELECT 
				floor(random() * 10000000)::int, -- Increased customer ID range
				now() - (random() * interval '3650 days'), -- Increased date range to 10 years
				round((random() * 1000000)::numeric, 2), -- Increased amount range
				'Transaction ' || generate_series || ' - ' || 
				CASE floor(random() * 5)::int
					WHEN 0 THEN 'Purchase'
					WHEN 1 THEN 'Payment'
					WHEN 2 THEN 'Refund'
					WHEN 3 THEN 'Subscription'
					WHEN 4 THEN 'Service'
				END
			FROM generate_series(1, %d);
		`, currentBatch)

		cmd = exec.Command(
			"psql",
			"-h", config.Host,
			"-p", config.Port,
			"-U", config.User,
			"-d", config.DBName,
			"-c", insertSQL,
		)
		cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

		cmdStr := strings.Join(cmd.Args, " ")
		log.Printf("Executing: %s", cmdStr)

		if output, err := cmd.CombinedOutput(); err != nil {
			log.Printf("Error inserting test data: %s", output)
			return fmt.Errorf("failed to insert test data: %w", err)
		}

		insertedRecords += currentBatch
		remainingRecords -= currentBatch

		elapsed := time.Since(startTime)
		rate := float64(insertedRecords) / elapsed.Seconds()
		log.Printf("Progress: %d/%d records (%.1f%%). Rate: %.0f records/sec. Elapsed: %v",
			insertedRecords, numRecords,
			float64(insertedRecords)/float64(numRecords)*100,
			rate,
			elapsed.Round(time.Second),
		)
	}

	indexSQL := `
		CREATE INDEX IF NOT EXISTS idx_customer_transactions_customer_id ON customer_transactions(customer_id);
		CREATE INDEX IF NOT EXISTS idx_customer_transactions_transaction_date ON customer_transactions(transaction_date);
		CREATE INDEX IF NOT EXISTS idx_customer_transactions_amount ON customer_transactions(amount);
	`

	cmd = exec.Command(
		"psql",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"-d", config.DBName,
		"-c", indexSQL,
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

	if output, err := cmd.CombinedOutput(); err != nil {
		log.Printf("Error creating indexes: %s", output)
		return fmt.Errorf("failed to create indexes: %w", err)
	}

	duration := time.Since(startTime)
	rate := float64(numRecords) / duration.Seconds()
	log.Printf("Successfully populated %s with %d records in %v (%.0f records/sec)",
		config.DBName, numRecords,
		duration.Round(time.Second),
		rate,
	)

	return nil
}

// ValidateDatabaseContent verifies that the source and destination databases have matching content
func ValidateDatabaseContent(srcConfig, destConfig DBConfig) error {
	validateSQL := `SELECT COUNT(*) FROM customer_transactions;`

	// Get source count
	srcCmd := exec.Command(
		"psql",
		"-h", srcConfig.Host,
		"-p", srcConfig.Port,
		"-U", srcConfig.User,
		"-d", srcConfig.DBName,
		"-t", // tuple only
		"-c", validateSQL,
	)
	srcCmd.Env = append(os.Environ(), "PGPASSWORD="+srcConfig.Password)
	srcOutput, err := srcCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get source record count: %w", err)
	}

	// Get destination count
	destCmd := exec.Command(
		"psql",
		"-h", destConfig.Host,
		"-p", destConfig.Port,
		"-U", destConfig.User,
		"-d", destConfig.DBName,
		"-t", // tuple only
		"-c", validateSQL,
	)
	destCmd.Env = append(os.Environ(), "PGPASSWORD="+destConfig.Password)
	destOutput, err := destCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to get destination record count: %w", err)
	}

	// Compare counts
	if string(srcOutput) != string(destOutput) {
		return fmt.Errorf("record count mismatch: source has %s records, destination has %s records",
			strings.TrimSpace(string(srcOutput)), strings.TrimSpace(string(destOutput)))
	}
	return nil
}

// CreateSampleTable creates a sample table in the specified database
func CreateSampleTable(config DBConfig) error {
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS companies (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100),
			rating VARCHAR(10),
			last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		INSERT INTO companies (name, rating) VALUES 
		('Apple Inc.', 'AAA'),
		('Microsoft', 'AA+'),
		('Google', 'AA');
	`

	cmd := exec.Command(
		"psql",
		"-h", config.Host,
		"-p", config.Port,
		"-U", config.User,
		"-d", config.DBName,
		"-c", createTableSQL,
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+config.Password)

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error creating sample table: %s", output)
		return fmt.Errorf("failed to create sample table: %w", err)
	}

	log.Printf("Sample table created successfully in %s", config.DBName)
	return nil
}

// SetupFDW sets up Foreign Data Wrapper between tenant and moodys databases
func SetupFDW(tenantConfig, moodysConfig DBConfig) error {
	setupSQL := fmt.Sprintf(`
		CREATE EXTENSION IF NOT EXISTS postgres_fdw;
		
		CREATE SERVER IF NOT EXISTS moodys_server
		FOREIGN DATA WRAPPER postgres_fdw
		OPTIONS (host '%s', port '%s', dbname '%s');
		
		CREATE USER MAPPING IF NOT EXISTS FOR %s
		SERVER moodys_server
		OPTIONS (user '%s', password '%s');
		
		CREATE FOREIGN TABLE companies_foreign (
			id INTEGER,
			name VARCHAR(100),
			rating VARCHAR(10),
			last_updated TIMESTAMP
		)
		SERVER moodys_server
		OPTIONS (schema_name 'public', table_name 'companies');
	`, moodysConfig.Host, moodysConfig.Port, moodysConfig.DBName,
		tenantConfig.User, moodysConfig.User, moodysConfig.Password)

	cmd := exec.Command(
		"psql",
		"-h", tenantConfig.Host,
		"-p", tenantConfig.Port,
		"-U", tenantConfig.User,
		"-d", tenantConfig.DBName,
		"-c", setupSQL,
	)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+tenantConfig.Password)

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Error setting up FDW: %s", output)
		return fmt.Errorf("failed to setup FDW: %w", err)
	}

	log.Printf("FDW setup completed successfully between %s and %s", tenantConfig.DBName, moodysConfig.DBName)
	return nil
}
