package migration

import (
	"fmt"
	"io"
	"time"
)

// Config holds the configuration for data migration
type Config struct {
	// Source S3-compatible configuration
	SourceEndpoint       string
	SourceRegion         string
	SourceAccessKey      string
	SourceSecretKey      string
	SourceBucket         string
	SourcePrefix         string
	SourceUseSSL         bool
	SourceForcePathStyle bool

	// Destination WalruS3 configuration
	DestEndpoint  string
	DestAccessKey string
	DestSecretKey string
	DestBucket    string
	DestUseSSL    bool

	// Migration options
	Workers      int
	BatchSize    int
	DryRun       bool
	MaxRetries   int
	SkipExisting bool
	Verbose      bool
}

// ObjectInfo represents an object to be migrated
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	Metadata     map[string]string
}

// ListObjectsResult represents the result of listing objects
type ListObjectsResult struct {
	Objects               []*ObjectInfo
	NextContinuationToken string
	IsTruncated           bool
}

// ObjectData represents an object's data and metadata
type ObjectData struct {
	Info    *ObjectInfo
	Content io.ReadCloser
}

// MigrationStatus represents the status of a migration operation
type MigrationStatus struct {
	ObjectKey  string
	Status     string // "pending", "success", "failed", "skipped"
	Error      string
	RetryCount int
	StartTime  time.Time
	EndTime    time.Time
	BytesCount int64
}

// ProgressStats holds migration progress statistics
type ProgressStats struct {
	TotalObjects     int64
	ProcessedObjects int64
	SuccessObjects   int64
	FailedObjects    int64
	SkippedObjects   int64
	TotalBytes       int64
	ProcessedBytes   int64
	StartTime        time.Time
	ElapsedTime      time.Duration
}

// GetRate returns the migration rate in objects per second
func (p *ProgressStats) GetRate() float64 {
	if p.ElapsedTime.Seconds() == 0 {
		return 0
	}
	return float64(p.ProcessedObjects) / p.ElapsedTime.Seconds()
}

// GetThroughput returns the migration throughput in bytes per second
func (p *ProgressStats) GetThroughput() float64 {
	if p.ElapsedTime.Seconds() == 0 {
		return 0
	}
	return float64(p.ProcessedBytes) / p.ElapsedTime.Seconds()
}

// GetProgress returns the migration progress as a percentage
func (p *ProgressStats) GetProgress() float64 {
	if p.TotalObjects == 0 {
		return 0
	}
	return float64(p.ProcessedObjects) / float64(p.TotalObjects) * 100
}

// GetETA returns the estimated time to completion
func (p *ProgressStats) GetETA() time.Duration {
	if p.ProcessedObjects == 0 {
		return 0
	}
	remaining := p.TotalObjects - p.ProcessedObjects
	rate := p.GetRate()
	if rate == 0 {
		return 0
	}
	return time.Duration(float64(remaining)/rate) * time.Second
}

// String returns a formatted string representation of the progress
func (p *ProgressStats) String() string {
	return fmt.Sprintf(
		"Progress: %.1f%% (%d/%d objects) | Rate: %.1f obj/s | Throughput: %.2f MB/s | ETA: %v | Success: %d | Failed: %d | Skipped: %d",
		p.GetProgress(),
		p.ProcessedObjects,
		p.TotalObjects,
		p.GetRate(),
		p.GetThroughput()/1024/1024,
		p.GetETA().Round(time.Second),
		p.SuccessObjects,
		p.FailedObjects,
		p.SkippedObjects,
	)
}
