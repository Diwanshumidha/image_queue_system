package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	envutil "diwanshuMidha/common/lib/env"
	s3_internal "diwanshuMidha/common/lib/s3"
	queue "diwanshuMidha/queue"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/davidbyttow/govips/v2/vips"
)

const (
	s3Timeout        = 30 * time.Second
	imageProcessTime = 50 * time.Second
	concurrency      = 4
)

func main() {
	rabbitMqUri := envutil.Get("RABBITMQ_URL", "amqp://admin:admin123@localhost:5672/")
	log.Println("Starting Transcode Worker...")

	rabbit, err := queue.NewRabbitClient(rabbitMqUri)
	if err != nil {
		log.Fatal("Failed to connect to RabbitMQ:", err)
	}
	defer rabbit.Close()

	msgs, err := rabbit.Consume(queue.TranscodeQueue)
	if err != nil {
		log.Fatal("Failed to register consumer:", err)
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf(" [*] Transcode Worker is listening on queue: %s", queue.TranscodeQueue)

	var wg sync.WaitGroup
	jobChan := make(chan queue.Delivery, concurrency)

	// Worker pool
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobChan {
				handleDelivery(d, rabbit)
			}
		}()
	}

	go func() {
		for d := range msgs {
			jobChan <- d
		}
		close(jobChan)
	}()

	<-stopChan
	log.Println(" [*] Worker shutting down gracefully...")
	wg.Wait()
}

func handleDelivery(d queue.Delivery, rabbit *queue.RabbitClient) {
	var event queue.ImageEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Printf("Invalid event, rejecting: %v", err)
		d.Reject(false)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), imageProcessTime)
	defer cancel()

	err := safeHandleImageEvent(ctx, event)
	if err != nil {

		log.Printf("Failed to process image %s: %v", event.Key, err)
		rabbit.Retry(&d)
		return
	}

	d.Ack(false)
	log.Printf("Processed image successfully: %s", event.Key)
}

func safeHandleImageEvent(ctx context.Context, event queue.ImageEvent) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[!] Panic caught in govips: %v", r)
			err = errors.New("govips panic: image too large or corrupted")
		}
	}()
	return handleImageEvent(ctx, event)
}

func handleImageEvent(ctx context.Context, event queue.ImageEvent) error {
	client, err := s3_internal.NewS3Client()
	if err != nil {
		return errors.New("cannot connect to S3 client")
	}

	uploader := manager.NewUploader(client)
	if uploader == nil {
		return errors.New("cannot create S3 uploader")
	}

	// Get object from S3 with timeout
	getCtx, cancel := context.WithTimeout(ctx, s3Timeout)
	defer cancel()

	obj, err := client.GetObject(getCtx, &s3.GetObjectInput{
		Bucket: aws.String(event.Bucket),
		Key:    aws.String(event.Key),
	})

	if err != nil {
		return fmt.Errorf("object not found: %w", err)
	}
	defer obj.Body.Close()

	// Create temp file safely
	tmpFile, err := os.CreateTemp("", "image-*")
	if err != nil {
		return err
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()

	if _, err := io.Copy(tmpFile, obj.Body); err != nil {
		return err
	}

	// Process image
	img, err := vips.NewImageFromFile(tmpFile.Name())
	if err != nil {
		return err
	}
	defer img.Close()

	const targetMaxDim = 4000

	w, h := img.Width(), img.Height()
	scale := 1.0
	if w > targetMaxDim || h > targetMaxDim {
		if w > h {
			scale = float64(targetMaxDim) / float64(w)
		} else {
			scale = float64(targetMaxDim) / float64(h)
		}
		err := img.Resize(scale, vips.KernelAuto)
		if err != nil {
			return err
		}
	}

	buf, _, err := img.ExportWebp(&vips.WebpExportParams{Quality: 70, Lossless: false, ReductionEffort: 1})

	if err != nil {
		return err
	}

	fmt.Println("== HTTP Headers ==")
	for k, v := range obj.Metadata {
		fmt.Printf("%s: %v\n", k, v)
	}

	// Upload processed image with timeout
	uploadCtx, cancel := context.WithTimeout(ctx, s3Timeout)
	defer cancel()

	id, ok := obj.Metadata["id"]
	if !ok {
		return errors.New("missing required metadata: id")
	}

	processedKey := fmt.Sprintf(
		"processed/%s.webp",
		id,
	)

	_, err = uploader.Upload(uploadCtx, &s3.PutObjectInput{
		Bucket:      aws.String(event.Bucket),
		Key:         aws.String(processedKey),
		Body:        bytes.NewReader(buf),
		Metadata:    obj.Metadata,
		ContentType: aws.String("image/webp"),
	})

	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	_, err = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(event.Bucket),
		Key:    aws.String(event.Key),
	})

	if err != nil {
		log.Printf("Warning: failed to delete original object %s: %v", event.Key, err)
	}

	return nil
}
