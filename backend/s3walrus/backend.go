package s3walrus

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/namihq/walrus-go"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/internal/s3io"
)

type Option func(b *Backend)

func WithTimeSource(timeSource gofakes3.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func WithEpochs(epochs int) Option {
	return func(b *Backend) { b.epochs = epochs }
}

func WithPublisherURL(url string) Option {
	return func(b *Backend) { b.publisherURL = url }
}

func WithAggregatorURL(url string) Option {
	return func(b *Backend) { b.aggregatorURL = url }
}

// Backend implements the gofakes3.Backend interface using Walrus for storage and Postgres for metadata
type Backend struct {
	db            *DB
	timeSource    gofakes3.TimeSource
	walrus        *walrus_go.Client
	epochs        int // Number of epochs to store objects for
	publisherURL  string
	aggregatorURL string
}

func New(dsn string, opts ...Option) (*Backend, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to to connect database: %w", err)
	}
	if err := db.AutoMigrate(&Bucket{}, &Object{}); err != nil {
		return nil, fmt.Errorf("failed to auto migrate database: %w", err)
	}

	b := &Backend{
		db: NewDB(db),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = gofakes3.DefaultTimeSource()
	}
	if b.epochs == 0 {
		b.epochs = 32
	}

	var walrusOpts []walrus_go.ClientOption
	if b.publisherURL != "" {
		walrusOpts = append(walrusOpts, walrus_go.WithPublisherURLs([]string{b.publisherURL}))
	}
	if b.aggregatorURL != "" {
		walrusOpts = append(walrusOpts, walrus_go.WithAggregatorURLs([]string{b.aggregatorURL}))
	}

	b.walrus = walrus_go.NewClient(walrusOpts...)

	return b, nil
}

// CreateBucket implements gofakes3.Backend
func (b *Backend) CreateBucket(name string) error {
	if err := b.db.CreateBucket(name); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return gofakes3.ResourceError(gofakes3.ErrBucketAlreadyExists, name)
		}
		return err
	}
	return nil
}

// BucketExists implements gofakes3.Backend
func (b *Backend) BucketExists(name string) (bool, error) {
	return b.db.BucketExists(name)
}

// ListBuckets implements gofakes3.Backend
func (b *Backend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	buckets, err := b.db.ListBuckets()
	if err != nil {
		return nil, err
	}

	result := make([]gofakes3.BucketInfo, len(buckets))
	for i, bucket := range buckets {
		result[i] = gofakes3.BucketInfo{
			Name:         bucket.Name,
			CreationDate: gofakes3.NewContentTime(bucket.CreatedAt),
		}
	}
	return result, nil
}

// DeleteBucket implements gofakes3.Backend
func (b *Backend) DeleteBucket(name string) error {
	return b.db.DeleteBucket(name)
}

// GetObject implements gofakes3.Backend
// rangeRequest is not implemented
func (b *Backend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (*gofakes3.Object, error) {
	obj, err := b.db.GetObject(bucketName, objectName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, gofakes3.KeyNotFound(objectName)
	}
	if err != nil {
		return nil, err
	}

	// Get object content from Walrus
	reader, err := b.walrus.ReadToReader(obj.BlobID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read from walrus: %w", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data from reader: %w", err)
	}

	rnge, err := rangeRequest.Range(obj.Size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		data = data[rnge.Start : rnge.Start+rnge.Length]
	}

	metadata, err := obj.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}

	return &gofakes3.Object{
		Name:           objectName,
		Metadata:       metadata,
		Size:           obj.Size,
		Contents:       s3io.ReaderWithDummyCloser{Reader: bytes.NewReader(data)},
		Hash:           []byte(obj.ETag),
		Range:          nil,
		VersionID:      gofakes3.VersionID(obj.VersionID),
		IsDeleteMarker: false,
	}, nil
}

// PutObject implements gofakes3.Backend
func (b *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (gofakes3.PutObjectResult, error) {
	// First check if bucket exists
	exists, err := b.db.BucketExists(bucketName)
	if err != nil {
		return gofakes3.PutObjectResult{}, err
	}
	if !exists {
		return gofakes3.PutObjectResult{}, gofakes3.BucketNotFound(bucketName)
	}

	// Store object in Walrus
	resp, err := b.walrus.StoreFromReader(input, &walrus_go.StoreOptions{
		Epochs: b.epochs,
	})
	if err != nil {
		return gofakes3.PutObjectResult{}, fmt.Errorf("failed to store in walrus: %w", err)
	}

	err = gofakes3.MergeMetadata(b, bucketName, objectName, meta)
	if err != nil {
		return gofakes3.PutObjectResult{}, err
	}

	// Create object record
	obj := &Object{
		BucketName: bucketName,
		ObjectName: objectName,
		BlobID:     resp.Blob.BlobID,
		Size:       size,
		ETag:       resp.Blob.BlobID,
		VersionID:  uuid.New().String(),
	}

	if err := obj.SetMetadata(meta); err != nil {
		return gofakes3.PutObjectResult{}, fmt.Errorf("failed to set metadata: %w", err)
	}

	if err := b.db.CreateObject(obj); err != nil {
		return gofakes3.PutObjectResult{}, fmt.Errorf("failed to store metadata: %w", err)
	}

	return gofakes3.PutObjectResult{
		VersionID: gofakes3.VersionID(obj.VersionID),
	}, nil
}

// DeleteObject implements gofakes3.Backend
func (b *Backend) DeleteObject(bucketName, objectName string) (gofakes3.ObjectDeleteResult, error) {
	// First check if bucket exists
	exists, err := b.db.BucketExists(bucketName)
	if err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}
	if !exists {
		return gofakes3.ObjectDeleteResult{}, gofakes3.BucketNotFound(bucketName)
	}

	err = b.db.DeleteObject(bucketName, objectName)
	if err != nil {
		return gofakes3.ObjectDeleteResult{}, err
	}

	// Note: We don't delete from Walrus as it handles its own lifecycle
	return gofakes3.ObjectDeleteResult{
		IsDeleteMarker: true,
	}, nil
}

// HeadObject implements gofakes3.Backend
func (b *Backend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	obj, err := b.db.GetObject(bucketName, objectName)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, gofakes3.KeyNotFound(objectName)
	}
	if err != nil {
		return nil, err
	}
	metadata, err := obj.GetMetadata()
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata: %w", err)
	}
	return &gofakes3.Object{
		Name:           objectName,
		Metadata:       metadata,
		Size:           obj.Size,
		Contents:       s3io.NoOpReadCloser{},
		Hash:           []byte(obj.ETag),
		Range:          nil,
		VersionID:      gofakes3.VersionID(obj.VersionID),
		IsDeleteMarker: false,
	}, nil
}

// ListBucket implements gofakes3.Backend
func (b *Backend) ListBucket(name string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {
	if prefix == nil {
		prefix = &gofakes3.Prefix{}
	}

	// Check if bucket exists
	exists, err := b.db.BucketExists(name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, gofakes3.BucketNotFound(name)
	}

	marker := ""
	if page.HasMarker {
		marker = page.Marker
	}

	maxKeys := 100
	if page.MaxKeys > 0 {
		maxKeys = int(page.MaxKeys)
	}

	objects, err := b.db.ListObjects(name, prefix.Prefix, marker, maxKeys)
	if err != nil {
		return nil, err
	}

	result := gofakes3.NewObjectList()
	var lastMatchedPart string
	var match gofakes3.PrefixMatch

	for _, obj := range objects {
		if !prefix.Match(obj.ObjectName, &match) {
			continue
		}

		if match.CommonPrefix {
			if match.MatchedPart == lastMatchedPart {
				continue
			}
			result.AddPrefix(match.MatchedPart)
			lastMatchedPart = match.MatchedPart
		} else {
			result.Add(&gofakes3.Content{
				Key:          obj.ObjectName,
				LastModified: gofakes3.NewContentTime(obj.CreatedAt),
				ETag:         `"` + obj.ETag + `"`,
				Size:         obj.Size,
				StorageClass: "STANDARD",
			})
		}
	}

	// Set next marker if results were truncated
	if len(objects) > 0 && len(objects) == maxKeys {
		result.IsTruncated = true
		result.NextMarker = objects[len(objects)-1].ObjectName
	}

	return result, nil
}

// DeleteMulti implements gofakes3.Backend
func (b *Backend) DeleteMulti(bucketName string, objects ...string) (gofakes3.MultiDeleteResult, error) {
	result := gofakes3.MultiDeleteResult{
		Deleted: make([]gofakes3.ObjectID, 0, len(objects)),
		Error:   make([]gofakes3.ErrorResult, 0),
	}

	for _, obj := range objects {
		_, err := b.DeleteObject(bucketName, obj)
		if err != nil {
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Key:     obj,
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
			})
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: obj,
			})
		}
	}

	return result, nil
}

// CopyObject implements gofakes3.Backend
func (b *Backend) CopyObject(srcBucket, srcKey, dstBucket, dstKey string, meta map[string]string) (gofakes3.CopyObjectResult, error) {
	result, err := gofakes3.CopyObject(b, srcBucket, srcKey, dstBucket, dstKey, meta)
	if err != nil {
		return result, err
	}

	obj, err := b.db.GetObject(dstBucket, dstKey)
	if err != nil {
		return gofakes3.CopyObjectResult{}, err
	}

	return gofakes3.CopyObjectResult{
		ETag:         `"` + obj.ETag + `"`,
		LastModified: gofakes3.NewContentTime(obj.CreatedAt),
	}, nil
}

// ForceDeleteBucket implements gofakes3.Backend
func (b *Backend) ForceDeleteBucket(name string) error {
	return b.db.ForceDeleteBucket(name)
}
