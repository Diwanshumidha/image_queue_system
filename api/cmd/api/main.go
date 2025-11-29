package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-contrib/cors"
	"github.com/google/uuid"

	envutil "diwanshuMidha/common/lib/env"
	s3_internal "diwanshuMidha/common/lib/s3"
	"diwanshuMidha/queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gin-gonic/gin"
)

var uploader *manager.Uploader

func main() {
	s3Client, err := s3_internal.NewS3Client()
	if err != nil {
		log.Fatalf("s3 client failed: %v", err)
	}

	uploader = manager.NewUploader(s3Client)

	r := gin.Default()
	r.Use(cors.New(cors.Config{
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"POST"},
		AllowHeaders: []string{"Content-Type"},
	}))

	r.POST("/upload", uploadHandler)
	r.POST("/upload/complete", completeHandler)

	r.Run(":8080")
}

func isAllowedImage(file multipart.File) (bool, error) {
	// read first 512 bytes (safe, standard)
	header := make([]byte, 512)
	n, err := file.Read(header)
	if err != nil && err != io.EOF {
		return false, err
	}

	// reset pointer so upload continues normally
	_, _ = file.Seek(0, io.SeekStart)

	mimeType := http.DetectContentType(header[:n])

	switch mimeType {
	case "image/jpeg",
		"image/png",
		"image/webp",
		"image/gif":
		return true, nil
	}

	return false, nil
}

type KeyResponse struct {
	Key string `json:"key"`
	Id  string `json:"id"`
	Ext string `json:"ext"`
}

func generateKey(filename string) (KeyResponse, error) {
	filename = strings.TrimSpace(filename)
	if filename == "" {
		return KeyResponse{}, errors.New("filename is required")
	}

	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(filename), "."))
	if ext == "" {
		return KeyResponse{}, errors.New("filename must contain an extension")
	}

	id := strings.ToLower(uuid.New().String())

	key := fmt.Sprintf("uploads/raw/%s.%s", id, ext)

	return KeyResponse{
		Key: key,
		Id:  id,
		Ext: ext,
	}, nil
}

func uploadHandler(c *gin.Context) {
	// This is just for demo purposes, in real application there will be actual authentication
	userId := c.GetHeader("X-User-Id")
	if userId == "" {
		userId = "guest"
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "file required"})
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "open failed"})
		return
	}

	isAllowed, err := isAllowedImage(file)
	if err != nil || !isAllowed {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "isAllowedImage failed"})
		return
	}

	defer file.Close()

	keyResponse, err := generateKey(fileHeader.Filename)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "generateKey failed", "details": err.Error()})
		return
	}

	_, err = uploader.Upload(c, &s3.PutObjectInput{
		Bucket: aws.String(s3_internal.Bucket),
		Key:    aws.String(keyResponse.Key),
		Body:   file,
		Metadata: map[string]string{
			"ContentType":   fileHeader.Header.Get("Content-Type"),
			"FileName":      fileHeader.Filename,
			"FileExtension": keyResponse.Ext,
			"UserId":        userId,
			"id":            keyResponse.Id,
		},
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "upload failed", "details": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "uploaded",
		"key":     keyResponse.Key,
		"id":      keyResponse.Id,
	})
}

type CompleteRequest struct {
	Key string `json:"key" binding:"required"`
}

// The Complete handler can be replaced by s3 webhooks
func completeHandler(c *gin.Context) {
	var req CompleteRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"error": "key is required"})
		return
	}

	// This is just for demo purposes, in real application there will be actual authentication
	userId := c.GetHeader("X-User-Id")
	if userId == "" {
		userId = "guest"
	}

	s3Client, err := s3_internal.NewS3Client()
	if err != nil {
		log.Fatalf("s3 client failed: %v", err)
	}

	// Make sure file is in s3
	_, err = s3Client.ListObjectsV2(c, &s3.ListObjectsV2Input{
		Bucket: &[]string{s3_internal.Bucket}[0],
		Prefix: &req.Key,
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "s3 failed", "details": err.Error()})
		return
	}

	rabbitMqUri := envutil.Get("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/")
	q, err := queue.NewRabbitClient(rabbitMqUri)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rabbitmq failed", "details": err.Error()})
		return
	}

	defer q.Close()

	q.PublishEvent(queue.ImageEvent{
		Bucket: s3_internal.Bucket,
		Key:    req.Key,
	})

	c.JSON(http.StatusOK, gin.H{"message": "completed"})
}
