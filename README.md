# PostgreSQL FDW-Aware Backup and Restore Tool

This tool provides an efficient way to backup and restore PostgreSQL databases that use Foreign Data Wrappers (FDW), with support for parallel processing during restore operations.

## Key Features

- Split-section backup/restore handling (pre-data, data, post-data)
- Smart handling of Foreign Data Wrapper configurations
- Parallel restore capabilities for large datasets
- Progress monitoring and performance metrics

## How It Works

### Specialized Backup Format Strategy

The tool uses different dump formats for different sections of the database:

1. **Pre-data Section**: Dumped as plain text (`.sql`)
   - Contains schema definitions, FDW configurations, and user mappings
   - Plain text format allows modification of FDW credentials during restore
   - Essential for updating connection details between environments

2. **Data and Post-data Sections**: Dumped in custom/compressed format (`.dump`)
   - Enables parallel restore capabilities
   - Significantly faster for large datasets
   - Reduces storage requirements through compression

### Foreign Data Wrapper (FDW) Handling

Foreign Data Wrappers in PostgreSQL allow a database to query external data sources as if they were local tables. This tool specifically handles:

- Automatic detection of FDW configurations
- Credential management between environments
- Safe update of connection details during restore

### Performance Optimizations

- Parallel restore operations using multiple CPU cores
- Batched data processing for large datasets
- Progress monitoring with real-time metrics
- Custom-format compression for efficient storage

## Usage

```go
// Configure source and destination databases
sourceConfig := DBConfig{
    Host:     "source.host",
    Port:     "5432",
    User:     "postgres",
    Password: "password",
    DBName:   "source_db",
}

destConfig := DBConfig{
    Host:     "dest.host",
    Port:     "5432",
    User:     "postgres",
    Password: "password",
    DBName:   "dest_db",
}

// Perform backup
if err := DumpWorkflow(sourceConfig, destConfig, "dump_dir"); err != nil {
    log.Fatal(err)
}

// Restore with FDW credential updates
if err := RestoreWorkflow(sourceConfig, destConfig, "dump_dir"); err != nil {
    log.Fatal(err)
}
```

## Performance

The tool is designed to handle large datasets efficiently:

- Parallel restore operations using available CPU cores
- Progress monitoring with insertion rates
- Real-time metrics during backup and restore
- Support for multi-million record datasets

## Requirements

- PostgreSQL 12 or later
- Go 1.18 or later
- `pg_dump` and `pg_restore` utilities
- Sufficient disk space for dump files

## Installation

```bash
go get github.com/yourusername/pg_restore_fdw
```

## Build

```bash
go build
```
