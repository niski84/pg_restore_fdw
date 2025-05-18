package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseWorkflow(t *testing.T) {
	const (
		dumpDir     = "dump_test"
		testRecords = 100000 // Reduced for faster testing, increase for thorough testing
	)

	// Source configurations
	moodysConfig := DBConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "your_new_password",
		DBName:   "moodys",
	}

	tenantConfig := DBConfig{
		Host:     "localhost",
		Port:     "5432",
		User:     "postgres",
		Password: "your_new_password",
		DBName:   "tenant",
	}

	// Destination configurations
	destMoodysConfig := moodysConfig
	destMoodysConfig.DBName = "moodys_restore_test"
	destTenantConfig := tenantConfig
	destTenantConfig.DBName = "tenant_restore_test"

	// Create dump directory
	if err := os.MkdirAll(dumpDir, 0755); err != nil {
		t.Fatalf("Failed to create dump directory: %v", err)
	}

	// Clean up any existing databases first
	t.Run("Initial Cleanup", func(t *testing.T) {
		if err := DeleteDatabases(moodysConfig, tenantConfig, destMoodysConfig, destTenantConfig); err != nil {
			t.Fatalf("Failed to cleanup existing databases: %v", err)
		}
	})

	// Setup source databases
	t.Run("Setup Source Databases", func(t *testing.T) {
		if err := SetupSourceDatabases(moodysConfig, tenantConfig, testRecords); err != nil {
			t.Fatalf("Failed to setup source databases: %v", err)
		}
	})

	// Test database dump workflow
	t.Run("Dump Workflow", func(t *testing.T) {
		if err := DumpWorkflow(moodysConfig, tenantConfig, dumpDir); err != nil {
			t.Fatalf("Failed to dump databases: %v", err)
		}

		// Verify dump files exist with correct extensions
		sections := []string{"pre-data", "data", "post-data"}
		databases := []string{"moodys", "tenant"}

		for _, db := range databases {
			for _, section := range sections {
				var expectedExt string
				if section == "pre-data" {
					expectedExt = ".sql"
				} else {
					expectedExt = ".dump"
				}

				dumpFile := filepath.Join(dumpDir, db+"_"+section+expectedExt)
				if _, err := os.Stat(dumpFile); os.IsNotExist(err) {
					t.Errorf("Expected dump file not found: %s", dumpFile)
				}
			}
		}
	})

	// Test tenant pre-data modification
	t.Run("Modify Tenant Pre-data", func(t *testing.T) {
		preDataFile := filepath.Join(dumpDir, "tenant_pre-data.sql")
		if err := modifyPreDataFile(preDataFile, moodysConfig, destMoodysConfig); err != nil {
			t.Fatalf("Failed to modify tenant pre-data file: %v", err)
		}
	})

	// Test restore workflow
	t.Run("Restore Workflow", func(t *testing.T) {
		if err := RestoreWorkflow(moodysConfig, tenantConfig, destMoodysConfig, destTenantConfig, dumpDir); err != nil {
			t.Fatalf("Failed to restore databases: %v", err)
		}
	})

	// Validate database content
	t.Run("Validate Database Content", func(t *testing.T) {
		if err := ValidateDatabaseContent(tenantConfig, destTenantConfig); err != nil {
			t.Fatalf("Database content validation failed: %v", err)
		}
	})

	// Final cleanup
	t.Run("Final Cleanup", func(t *testing.T) {
		if err := DeleteDatabases(moodysConfig, tenantConfig, destMoodysConfig, destTenantConfig); err != nil {
			t.Fatalf("Failed to cleanup databases: %v", err)
		}

		// Clean up dump directory
		if err := os.RemoveAll(dumpDir); err != nil {
			t.Errorf("Failed to remove dump directory: %v", err)
		}
	})
}
