package migration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Client is a generic S3-compatible client that can connect to any S3-compatible service
// It implements the SourceStorage interface
type S3Client struct {
	client *s3.S3
	config *S3Config
}

// S3Config holds S3 client configuration
type S3Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
	ForcePathStyle  bool
}

// NewS3Client creates a new S3-compatible client
func NewS3Client(config *S3Config) (*S3Client, error) {
	awsConfig := &aws.Config{
		Region:           aws.String(config.Region),
		DisableSSL:       aws.Bool(!config.UseSSL),
		S3ForcePathStyle: aws.Bool(config.ForcePathStyle),
	}

	// Only set credentials if they are provided (non-empty)
	if config.AccessKeyID != "" && config.SecretAccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(
			config.AccessKeyID,
			config.SecretAccessKey,
			"",
		)
	} else {
		// Use anonymous credentials for WalruS3 when no credentials are provided
		awsConfig.Credentials = credentials.AnonymousCredentials
	}

	if config.Endpoint != "" {
		awsConfig.Endpoint = aws.String(config.Endpoint)
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS session: %w", err)
	}

	return &S3Client{
		client: s3.New(sess),
		config: config,
	}, nil
}

// ListObjects lists objects in the bucket
func (c *S3Client) ListObjects(ctx context.Context, bucket, prefix string, pageSize int, continuationToken string) (*ListObjectsResult, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int64(int64(pageSize)),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}

	output, err := c.client.ListObjectsV2WithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	result := &ListObjectsResult{
		Objects:     make([]*ObjectInfo, 0, len(output.Contents)),
		IsTruncated: aws.BoolValue(output.IsTruncated),
	}

	if output.NextContinuationToken != nil {
		result.NextContinuationToken = aws.StringValue(output.NextContinuationToken)
	}

	for _, obj := range output.Contents {
		objInfo := &ObjectInfo{
			Key:          aws.StringValue(obj.Key),
			Size:         aws.Int64Value(obj.Size),
			LastModified: aws.TimeValue(obj.LastModified),
			ETag:         aws.StringValue(obj.ETag),
		}
		result.Objects = append(result.Objects, objInfo)
	}

	return result, nil
}

// GetObject downloads an object
func (c *S3Client) GetObject(ctx context.Context, bucket, key string) (*ObjectData, error) {
	// First get metadata
	metadata, err := c.GetObjectMetadata(ctx, bucket, key)
	if err != nil {
		return nil, err
	}

	// Get object content
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := c.client.GetObjectWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}

	// Merge metadata from GetObject response
	if output.Metadata != nil {
		if metadata.Metadata == nil {
			metadata.Metadata = make(map[string]string)
		}
		for k, v := range output.Metadata {
			if v != nil {
				metadata.Metadata[k] = *v
			}
		}
	}

	return &ObjectData{
		Info:    metadata,
		Content: output.Body,
	}, nil
}

// GetObjectMetadata gets object metadata
func (c *S3Client) GetObjectMetadata(ctx context.Context, bucket, key string) (*ObjectInfo, error) {
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	output, err := c.client.HeadObjectWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to get object metadata for %s: %w", key, err)
	}

	metadata := make(map[string]string)
	if output.Metadata != nil {
		for k, v := range output.Metadata {
			if v != nil {
				metadata[k] = *v
			}
		}
	}

	// Add standard metadata
	if output.ContentType != nil {
		metadata["Content-Type"] = *output.ContentType
	}
	if output.ContentEncoding != nil {
		metadata["Content-Encoding"] = *output.ContentEncoding
	}
	if output.ContentLanguage != nil {
		metadata["Content-Language"] = *output.ContentLanguage
	}
	if output.ContentDisposition != nil {
		metadata["Content-Disposition"] = *output.ContentDisposition
	}
	if output.CacheControl != nil {
		metadata["Cache-Control"] = *output.CacheControl
	}

	return &ObjectInfo{
		Key:          key,
		Size:         aws.Int64Value(output.ContentLength),
		LastModified: aws.TimeValue(output.LastModified),
		ETag:         aws.StringValue(output.ETag),
		Metadata:     metadata,
	}, nil
}

// PutObject uploads an object
func (c *S3Client) PutObject(ctx context.Context, bucket, key string, body io.Reader, size int64, metadata map[string]string) error {
	// Convert io.Reader to []byte and then to ReadSeeker
	data, err := io.ReadAll(body)
	if err != nil {
		return fmt.Errorf("failed to read body: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(size),
	}

	if metadata != nil {
		awsMetadata := make(map[string]*string)
		for k, v := range metadata {
			// Handle standard metadata
			switch strings.ToLower(k) {
			case "content-type":
				input.ContentType = aws.String(v)
			case "content-encoding":
				input.ContentEncoding = aws.String(v)
			case "content-language":
				input.ContentLanguage = aws.String(v)
			case "content-disposition":
				input.ContentDisposition = aws.String(v)
			case "cache-control":
				input.CacheControl = aws.String(v)
			default:
				// Custom metadata
				awsMetadata[k] = aws.String(v)
			}
		}
		if len(awsMetadata) > 0 {
			input.Metadata = awsMetadata
		}
	}

	_, err = c.client.PutObjectWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", key, err)
	}

	return nil
}

// BucketExists checks if a bucket exists
func (c *S3Client) BucketExists(ctx context.Context, bucket string) (bool, error) {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err := c.client.HeadBucketWithContext(ctx, input)
	if err != nil {
		// Check if the error is "NotFound"
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "NoSuchBucket") {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// CreateBucket creates a bucket
func (c *S3Client) CreateBucket(ctx context.Context, bucket string) error {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	}

	// For regions other than us-east-1, we need to specify the location constraint
	if c.config.Region != "us-east-1" && c.config.Region != "" {
		input.CreateBucketConfiguration = &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(c.config.Region),
		}
	}

	_, err := c.client.CreateBucketWithContext(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create bucket %s: %w", bucket, err)
	}

	return nil
}

// Close closes the S3 client connection
func (c *S3Client) Close() error {
	// AWS SDK doesn't require explicit closing
	return nil
}
