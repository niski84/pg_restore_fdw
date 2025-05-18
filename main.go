package main

import (
	"log"
	"time"
)

func main() {
	startTime := time.Now()

	// Source configurations
	moodysConfig := DBConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "your_password", // Replace with actual password
		DBName:   "moodys",
	}

	tenantConfig := DBConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "your_password", // Replace with actual password
		DBName:   "tenant",
	}

	// Destination configurations (for testing restore)
	destMoodysConfig := moodysConfig
	destMoodysConfig.DBName = "moodys_dest"
	destTenantConfig := tenantConfig
	destTenantConfig.DBName = "tenant_dest"

	// Clean up any existing databases
	log.Println("Cleaning up existing databases...")
	if err := DeleteDatabases(moodysConfig, tenantConfig, destMoodysConfig, destTenantConfig); err != nil {
		log.Fatalf("Failed to cleanup existing databases: %v", err)
	}

	// Setup source databases with a large number of records
	// 50 million records should take ~10-15 minutes to generate
	const numTestRecords = 50000000
	log.Printf("Setting up source databases with %d records...", numTestRecords)

	if err := SetupSourceDatabases(moodysConfig, tenantConfig, numTestRecords); err != nil {
		log.Fatalf("Failed to setup source databases: %v", err)
	}

	// Perform dump workflow
	log.Println("Starting database dump workflow...")
	if err := DumpWorkflow(moodysConfig, tenantConfig, "dump_test"); err != nil {
		log.Fatalf("Failed to dump databases: %v", err)
	}

	// Perform restore workflow
	log.Println("Starting database restore workflow...")
	if err := RestoreWorkflow(moodysConfig, tenantConfig, destMoodysConfig, destTenantConfig, "dump_test"); err != nil {
		log.Fatalf("Failed to restore databases: %v", err)
	}

	// Validate the restoration
	log.Println("Validating restored data...")
	if err := ValidateDatabaseContent(tenantConfig, destTenantConfig); err != nil {
		log.Fatalf("Data validation failed: %v", err)
	}

	duration := time.Since(startTime)
	log.Printf("Complete database backup/restore workflow completed successfully in %v", duration.Round(time.Second))
}
