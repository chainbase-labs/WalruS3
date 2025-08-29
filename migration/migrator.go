package migration

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Migrator handles the migration process
type Migrator struct {
	config        *Config
	sourceStorage *S3Client
	destStorage   *S3Client

	// Progress tracking
	stats    *ProgressStats
	statsMux sync.RWMutex

	// Worker management
	objectChan chan *ObjectInfo
	resultChan chan *MigrationStatus
	workerWg   sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewMigrator creates a new migrator instance
func NewMigrator(config *Config) (*Migrator, error) {
	// Create source storage (S3-compatible)
	sourceConfig := &S3Config{
		Endpoint:        config.SourceEndpoint,
		Region:          config.SourceRegion,
		AccessKeyID:     config.SourceAccessKey,
		SecretAccessKey: config.SourceSecretKey,
		UseSSL:          config.SourceUseSSL,
		ForcePathStyle:  config.SourceForcePathStyle,
	}

	sourceStorage, err := NewS3Client(sourceConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create source storage: %w", err)
	}

	// Create destination storage (WalruS3 S3 API)
	destConfig := &S3Config{
		Endpoint:        config.DestEndpoint,
		Region:          "us-east-1",
		AccessKeyID:     config.DestAccessKey,
		SecretAccessKey: config.DestSecretKey,
		UseSSL:          config.DestUseSSL,
		ForcePathStyle:  true, // WalruS3 typically uses path-style
	}

	destStorage, err := NewS3Client(destConfig)
	if err != nil {
		sourceStorage.Close()
		return nil, fmt.Errorf("failed to create destination storage: %w", err)
	}

	// Ensure destination bucket exists
	ctx := context.Background()
	exists, err := destStorage.BucketExists(ctx, config.DestBucket)
	if err != nil {
		sourceStorage.Close()
		destStorage.Close()
		return nil, fmt.Errorf("failed to check destination bucket: %w", err)
	}
	if !exists {
		if err := destStorage.CreateBucket(ctx, config.DestBucket); err != nil {
			sourceStorage.Close()
			destStorage.Close()
			return nil, fmt.Errorf("failed to create destination bucket: %w", err)
		}
		log.Printf("Created destination bucket: %s", config.DestBucket)
	}

	ctx, cancel := context.WithCancel(context.Background())

	migrator := &Migrator{
		config:        config,
		sourceStorage: sourceStorage,
		destStorage:   destStorage,
		stats: &ProgressStats{
			StartTime: time.Now(),
		},
		objectChan: make(chan *ObjectInfo, config.Workers*2),
		resultChan: make(chan *MigrationStatus, config.Workers*2),
		ctx:        ctx,
		cancel:     cancel,
	}

	return migrator, nil
}

// Run starts the migration process
func (m *Migrator) Run() error {
	// Start workers
	for i := 0; i < m.config.Workers; i++ {
		m.workerWg.Add(1)
		go m.worker(i)
	}

	// Start result processor
	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		m.processResults()
	}()

	// Start progress reporter
	if m.config.Verbose {
		go m.reportProgress()
	}

	// List and queue objects
	if err := m.listAndQueueObjects(); err != nil {
		m.cancel()
		return err
	}

	// Close object channel to signal workers to finish
	close(m.objectChan)

	// Wait for all workers to finish
	m.workerWg.Wait()

	// Close result channel to signal result processor to finish
	close(m.resultChan)

	// Wait for result processor to finish
	resultWg.Wait()

	// Final progress report
	m.printFinalStats()

	return nil
}

// listAndQueueObjects lists objects from source and queues them for migration
func (m *Migrator) listAndQueueObjects() error {
	var continuationToken string
	totalListed := int64(0)

	for {
		select {
		case <-m.ctx.Done():
			return m.ctx.Err()
		default:
		}

		result, err := m.sourceStorage.ListObjects(
			m.ctx,
			m.config.SourceBucket,
			m.config.SourcePrefix,
			m.config.BatchSize,
			continuationToken,
		)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range result.Objects {
			select {
			case m.objectChan <- obj:
				totalListed++
			case <-m.ctx.Done():
				return m.ctx.Err()
			}
		}

		// Update total count
		atomic.StoreInt64(&m.stats.TotalObjects, totalListed)

		if !result.IsTruncated {
			break
		}
		continuationToken = result.NextContinuationToken
	}

	log.Printf("Listed %d objects for migration", totalListed)
	return nil
}

// worker processes objects from the queue
func (m *Migrator) worker(_ int) {
	defer m.workerWg.Done()

	for obj := range m.objectChan {
		status := m.migrateObject(obj)

		select {
		case m.resultChan <- status:
		case <-m.ctx.Done():
			return
		}
	}
}

// migrateObject migrates a single object
func (m *Migrator) migrateObject(obj *ObjectInfo) *MigrationStatus {
	status := &MigrationStatus{
		ObjectKey:  obj.Key,
		StartTime:  time.Now(),
		BytesCount: obj.Size,
	}

	// Check if object already exists and skip if configured
	if m.config.SkipExisting {
		exists, err := m.objectExists(obj.Key)
		if err != nil {
			status.Status = "failed"
			status.Error = fmt.Sprintf("failed to check if object exists: %v", err)
			status.EndTime = time.Now()
			return status
		}
		if exists {
			status.Status = "skipped"
			status.EndTime = time.Now()
			return status
		}
	}

	if m.config.DryRun {
		status.Status = "success"
		status.EndTime = time.Now()
		return status
	}

	// Retry logic
	var lastErr error
	for attempt := 0; attempt <= m.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry with exponential backoff
			waitTime := time.Duration(attempt*attempt) * time.Second
			time.Sleep(waitTime)
		}

		err := m.transferObject(obj)
		if err == nil {
			status.Status = "success"
			status.EndTime = time.Now()
			return status
		}

		lastErr = err
		status.RetryCount = attempt

		if m.config.Verbose {
			log.Printf("Attempt %d failed for %s: %v", attempt+1, obj.Key, err)
		}
	}

	status.Status = "failed"
	status.Error = lastErr.Error()
	status.EndTime = time.Now()
	return status
}

// transferObject transfers a single object from source to destination
func (m *Migrator) transferObject(obj *ObjectInfo) error {
	// Get object from source
	objectData, err := m.sourceStorage.GetObject(m.ctx, m.config.SourceBucket, obj.Key)
	if err != nil {
		return fmt.Errorf("failed to get object from source: %w", err)
	}
	defer objectData.Content.Close()

	// Store object in destination (WalruS3)
	err = m.destStorage.PutObject(
		m.ctx,
		m.config.DestBucket,
		obj.Key,
		objectData.Content,
		objectData.Info.Size,
		objectData.Info.Metadata,
	)
	if err != nil {
		return fmt.Errorf("failed to store object in destination: %w", err)
	}

	return nil
}

// objectExists checks if an object already exists in the destination
func (m *Migrator) objectExists(key string) (bool, error) {
	_, err := m.destStorage.GetObjectMetadata(m.ctx, m.config.DestBucket, key)
	if err != nil {
		// If the error contains "NotFound" or "NoSuchKey", the object doesn't exist
		errStr := err.Error()
		if strings.Contains(errStr, "NotFound") || strings.Contains(errStr, "NoSuchKey") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// processResults processes migration results and updates statistics
func (m *Migrator) processResults() {
	for result := range m.resultChan {
		m.statsMux.Lock()

		atomic.AddInt64(&m.stats.ProcessedObjects, 1)
		atomic.AddInt64(&m.stats.ProcessedBytes, result.BytesCount)

		switch result.Status {
		case "success":
			atomic.AddInt64(&m.stats.SuccessObjects, 1)
		case "failed":
			atomic.AddInt64(&m.stats.FailedObjects, 1)
		case "skipped":
			atomic.AddInt64(&m.stats.SkippedObjects, 1)
		}

		m.statsMux.Unlock()
	}
}

// reportProgress reports migration progress periodically
func (m *Migrator) reportProgress() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.statsMux.RLock()
			m.stats.ElapsedTime = time.Since(m.stats.StartTime)
			log.Println(m.stats.String())
			m.statsMux.RUnlock()
		case <-m.ctx.Done():
			return
		}
	}
}

// printFinalStats prints final migration statistics
func (m *Migrator) printFinalStats() {
	m.statsMux.RLock()
	m.stats.ElapsedTime = time.Since(m.stats.StartTime)

	log.Println("Migration completed!")
	log.Printf("Total objects: %d", m.stats.TotalObjects)
	log.Printf("Processed: %d", m.stats.ProcessedObjects)
	log.Printf("Successful: %d", m.stats.SuccessObjects)
	log.Printf("Failed: %d", m.stats.FailedObjects)
	log.Printf("Skipped: %d", m.stats.SkippedObjects)
	log.Printf("Total bytes: %d (%.2f MB)", m.stats.ProcessedBytes, float64(m.stats.ProcessedBytes)/1024/1024)
	log.Printf("Elapsed time: %v", m.stats.ElapsedTime.Round(time.Second))
	log.Printf("Average rate: %.1f objects/second", m.stats.GetRate())
	log.Printf("Average throughput: %.2f MB/second", m.stats.GetThroughput()/1024/1024)

	m.statsMux.RUnlock()
}

// Close closes the migrator and its resources
func (m *Migrator) Close() error {
	if m.cancel != nil {
		m.cancel()
	}

	var errs []error

	if m.sourceStorage != nil {
		if err := m.sourceStorage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close source storage: %w", err))
		}
	}

	if m.destStorage != nil {
		if err := m.destStorage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close destination storage: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors during close: %v", errs)
	}

	return nil
}
