package s3walrus

import (
	"encoding/json"
	"time"

	"gorm.io/gorm"

	"github.com/johannesboyne/gofakes3"
)

// Bucket represents a S3 bucket in the database
type Bucket struct {
	Name      string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName specifies the table name for Bucket
func (b *Bucket) TableName() string {
	return "buckets"
}

// Object represents a S3 object in the database
type Object struct {
	BucketName string         `gorm:"primaryKey;type:varchar(255)"`
	ObjectName string         `gorm:"primaryKey;type:varchar(1024)"`
	BlobID     string         `gorm:"not null"`
	Size       int64          `gorm:"not null"`
	Metadata   []byte         `gorm:"type:jsonb"`
	ETag       string         `gorm:"type:varchar(255)"`
	VersionID  string         `gorm:"primaryKey;type:varchar(255)"`
	CreatedAt  time.Time      `gorm:"autoCreateTime"`
	DeletedAt  gorm.DeletedAt `gorm:"index"`
}

// TableName specifies the table name for Object
func (obj *Object) TableName() string {
	return "objects"
}

// GetMetadata gets object metadata as a map
func (obj *Object) GetMetadata() (map[string]string, error) {
	if len(obj.Metadata) == 0 {
		return nil, nil
	}

	var metadata map[string]string
	err := json.Unmarshal(obj.Metadata, &metadata)
	return metadata, err
}

// SetMetadata sets object metadata from a map
func (obj *Object) SetMetadata(metadata map[string]string) error {
	if metadata == nil {
		obj.Metadata = nil
		return nil
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	obj.Metadata = data
	return nil
}

// DB represents the database connection
type DB struct {
	db *gorm.DB
}

// NewDB creates a new database connection
func NewDB(db *gorm.DB) *DB {
	return &DB{db: db}
}

// ListBuckets returns all buckets
func (d *DB) ListBuckets() ([]Bucket, error) {
	var buckets []Bucket
	err := d.db.Order("name").Find(&buckets).Error
	return buckets, err
}

// CreateBucket creates a new bucket
func (d *DB) CreateBucket(name string) error {
	return d.db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(&Bucket{Name: name}).Error
	})
}

// BucketExists checks if a bucket exists
func (d *DB) BucketExists(name string) (bool, error) {
	var count int64
	err := d.db.Model(&Bucket{}).Where("name = ?", name).Count(&count).Error
	return count > 0, err
}

// DeleteBucket deletes a bucket if it's empty
func (d *DB) DeleteBucket(name string) error {
	return d.db.Transaction(func(tx *gorm.DB) error {
		// Check if bucket has objects
		var count int64
		if err := tx.Model(&Object{}).Where("bucket_name = ?", name).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return gofakes3.ResourceError(gofakes3.ErrBucketNotEmpty, name)
		}

		return tx.Delete(&Bucket{Name: name}).Error
	})
}

// GetObject gets an object's metadata
func (d *DB) GetObject(bucketName, objectName string) (*Object, error) {
	var obj Object
	err := d.db.Where("bucket_name = ? AND object_name = ?", bucketName, objectName).
		Order("last_modified DESC").
		First(&obj).Error
	return &obj, err
}

// CreateObject creates a new object
func (d *DB) CreateObject(obj *Object) error {
	return d.db.Transaction(func(tx *gorm.DB) error {
		return tx.Create(obj).Error
	})
}

// DeleteObject deletes an object
func (d *DB) DeleteObject(bucketName, objectName string) (*Object, error) {
	var obj Object
	err := d.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Where("bucket_name = ? AND object_name = ?", bucketName, objectName).
			Order("last_modified DESC").
			First(&obj).Error
		if err != nil {
			return err
		}

		return tx.Delete(&obj).Error
	})

	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// ListObjects lists objects in a bucket with optional prefix and pagination
func (d *DB) ListObjects(bucketName string, prefix string, marker string, maxKeys int) ([]Object, error) {
	query := d.db.Where("bucket_name = ?", bucketName)

	if prefix != "" {
		query = query.Where("object_name LIKE ?", prefix+"%")
	}

	if marker != "" {
		query = query.Where("object_name > ?", marker)
	}

	query = query.Order("object_name")

	if maxKeys > 0 {
		query = query.Limit(maxKeys)
	}

	var objects []Object
	err := query.Find(&objects).Error
	return objects, err
}

// ForceDeleteBucket deletes a bucket and all its objects
func (d *DB) ForceDeleteBucket(name string) error {
	return d.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("bucket_name = ?", name).Delete(&Object{}).Error; err != nil {
			return err
		}
		return tx.Delete(&Bucket{Name: name}).Error
	})
}
