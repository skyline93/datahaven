package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var Cfg Config

type MongoDBConfig struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
}

type S3Config struct {
	Region    string `mapstructure:"region"`
	Endpoint  string `mapstructure:"endpoint"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
}

type Config struct {
	S3      S3Config      `mapstructure:"s3"`
	MongoDB MongoDBConfig `mapstructure:"mongodb"`
}

func InitConfig(cfgFile string) error {
	if cfgFile == "" {
		viper.SetConfigName("datahaven")
		viper.SetConfigType("toml")
		viper.AddConfigPath("$HOME/.datahaven")
		viper.AddConfigPath("/etc")
	} else {
		viper.SetConfigFile(cfgFile)
	}

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	if err := viper.Unmarshal(&Cfg); err != nil {
		return err
	}

	return nil
}

type FileMetadata struct {
	Ctime int64
	Mtime int64
	Atime int64
	Name  string
	Path  string
	Size  int64
	Uid   int
	Gid   int
	Hash  string
}

// MongoDBClient represents the interface for MongoDB operations.
type MongoDBClient interface {
	InsertOne(collectionName string, document interface{}) error
	Close()
}

// MongoClient implements the MongoDBClient interface.
type MongoClient struct {
	client *mongo.Client
}

// NewMongoClient creates a new instance of MongoClient.
func NewMongoClient(cfg *MongoDBConfig) (*MongoClient, error) {
	uri := fmt.Sprintf("mongodb://%s:%s@%s:%d", cfg.User, cfg.Password, cfg.Host, cfg.Port)

	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}
	return &MongoClient{client: client}, nil
}

// InsertOne inserts a document into the specified collection.
func (mc *MongoClient) InsertOne(collectionName string, document interface{}) error {
	collection := mc.client.Database("datahaven").Collection(collectionName)
	_, err := collection.InsertOne(context.Background(), document)
	return err
}

// Close closes the MongoDB client connection.
func (mc *MongoClient) Close() {
	if mc.client != nil {
		mc.client.Disconnect(context.Background())
	}
}

func calculateSHA256Hash(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	hashBytes := hash.Sum(nil)
	return "sha256:" + hex.EncodeToString(hashBytes), nil
}

func scanDir(dir string, metadataChan chan FileMetadata) {
	filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		hash, err := calculateSHA256Hash(path)
		if err != nil {
			return nil
		}

		metadata := FileMetadata{
			Ctime: info.Sys().(*syscall.Stat_t).Mtim.Nano(),
			Mtime: info.Sys().(*syscall.Stat_t).Mtim.Nano(),
			Atime: info.Sys().(*syscall.Stat_t).Atim.Nano(),
			Name:  info.Name(),
			Path:  path,
			Size:  info.Size(),
			Uid:   int(info.Sys().(*syscall.Stat_t).Uid),
			Gid:   int(info.Sys().(*syscall.Stat_t).Gid),
			Hash:  hash,
		}

		metadataChan <- metadata
		return nil
	})

	close(metadataChan)
	log.Println("scan dir completed")
}

type S3Client struct {
	svc *s3.S3
}

func NewS3Client(cfg *S3Config) *S3Client {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:           aws.String(cfg.Region),
		Endpoint:         aws.String(cfg.Endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
	}))

	return &S3Client{svc: s3.New(sess)}
}

func (c *S3Client) UploadLargeFile(bucketName, key, filePath string) error {
	log.Printf("upload large file [%s] to s3", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	uploader := s3manager.NewUploaderWithClient(c.svc)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   file,
	})
	if err != nil {
		log.Println("Error uploading file to S3:", err)
		return err
	}

	log.Printf("Large file uploaded to S3: s3://%s/%s\n", bucketName, key)
	return nil
}

func main() {
	if err := InitConfig("datahaven.toml"); err != nil {
		panic(err)
	}

	client, err := NewMongoClient(&Cfg.MongoDB)
	if err != nil {
		fmt.Println("Error creating MongoDB client:", err)
		return
	}
	defer client.Close()

	s3Client := NewS3Client(&Cfg.S3)

	metadataChan := make(chan FileMetadata, 1)

	go scanDir("/home/skyline93/workspace/datahaven/testdata", metadataChan)

	for metadata := range metadataChan {
		log.Printf("save metadata to mongodb, file: [%s]", metadata.Name)
		collectionName := "1"
		err = client.InsertOne(collectionName, metadata)
		if err != nil {
			fmt.Println("Error inserting metadata:", err)
			continue
		}

		go s3Client.UploadLargeFile("datahaven", metadata.Hash, metadata.Path)
	}

	fmt.Println("Metadata inserted successfully.")
}
