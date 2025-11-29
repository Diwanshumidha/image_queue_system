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

	// root context for the whole process
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs, err := rabbit.Consume(queue.TranscodeQueue)
	if err != nil {
		log.Fatal("Failed to register consumer:", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	jobChan := make(chan queue.Delivery, concurrency*2)
	var wg sync.WaitGroup
	var closeOnce sync.Once

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go worker(ctx, i, &wg, jobChan, rabbit)
	}

	go func() {
		defer func() {
			close(jobChan)
			log.Println("consumer: exiting and closed jobChan")
		}()

		for {
			select {
			case <-ctx.Done():
				log.Println("consumer: context cancelled, closing rabbit connection")
				closeOnce.Do(func() { rabbit.Close() })
				return
			case d, ok := <-msgs:
				if !ok {
					log.Println("consumer: msgs channel closed by rabbit")
					return
				}
				select {
				case jobChan <- d:
				case <-ctx.Done():
					_ = d.Nack(false, true)
					return
				}
			}
		}
	}()

	<-sigCh
	log.Println("main: shutdown signal received — cancelling root context")
	cancel()

	closeOnce.Do(func() { rabbit.Close() })

	wg.Wait()
	log.Println("main: graceful shutdown complete")
}

func worker(rootCtx context.Context, id int, wg *sync.WaitGroup, jobChan <-chan queue.Delivery, rabbit *queue.RabbitClient) {
	defer wg.Done()
	log.Printf("worker-%d: started", id)

	for {
		select {
		case <-rootCtx.Done():
			log.Printf("worker-%d: root context cancelled — exiting", id)
			return
		case d, ok := <-jobChan:
			if !ok {
				log.Printf("worker-%d: job channel closed — exiting", id)
				return
			}

			jobCtx, jobCancel := context.WithTimeout(rootCtx, imageProcessTime)
			err := handleDeliveryWithCtx(jobCtx, &d, rabbit)
			jobCancel()

			if err != nil {
				log.Printf("worker-%d: job error: %v", id, err)
			}
		}
	}
}

func handleDeliveryWithCtx(ctx context.Context, d *queue.Delivery, rabbit *queue.RabbitClient) error {
	var event queue.ImageEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Printf("Invalid event, rejecting: %v", err)
		_ = d.Reject(false)
		return fmt.Errorf("invalid event: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- safeHandleImageEvent(ctx, event, rabbit, d)
	}()

	select {
	case <-ctx.Done():
		log.Printf("handleDeliveryWithCtx: ctx cancelled for key=%s, requeueing", event.Key)
		_ = d.Nack(false, true)
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("aborted by shutdown: %w", err)
			}
			return errors.New("aborted by shutdown")
		case <-time.After(2 * time.Second):
			return errors.New("aborted by shutdown")
		}
	case err := <-done:
		return err
	}
}

func safeHandleImageEvent(ctx context.Context, event queue.ImageEvent, rabbit *queue.RabbitClient, d *queue.Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[!] Panic caught in govips: %v", r)
			err = errors.New("govips panic: image too large or corrupted")
		}
	}()

	if err := handleImageEvent(ctx, event); err != nil {
		log.Printf("Failed to process image %s: %v", event.Key, err)
		rabbit.Retry(d)
		return err
	}

	if err := d.Ack(false); err != nil {
		log.Printf("Warning: failed to ack message %s: %v", event.Key, err)
		return fmt.Errorf("ack failed: %w", err)
	}

	log.Printf("Processed image successfully: %s", event.Key)
	return nil
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

	getCtx, getCancel := context.WithTimeout(ctx, s3Timeout)
	defer getCancel()

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
		_ = os.Remove(tmpFile.Name())
	}()

	// Copy with ctx awareness: we can't interrupt io.Copy directly, so copy in goroutine and select
	copyDone := make(chan error, 1)
	go func() {
		_, err := io.Copy(tmpFile, obj.Body)
		copyDone <- err
	}()

	select {
	case <-ctx.Done():
		return errors.New("aborted while downloading object")
	case err := <-copyDone:
		if err != nil {
			return fmt.Errorf("failed copying object body: %w", err)
		}
	}

	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	processDone := make(chan struct {
		buf []byte
		err error
	}, 1)

	go func() {
		// Create image from file
		img, ierr := vips.NewImageFromFile(tmpFile.Name())
		if ierr != nil {
			processDone <- struct {
				buf []byte
				err error
			}{nil, ierr}
			return
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
			if ierr = img.Resize(scale, vips.KernelAuto); ierr != nil {
				processDone <- struct {
					buf []byte
					err error
				}{nil, ierr}
				return
			}
		}

		// Export to webp
		buf, _, ierr := img.ExportWebp(&vips.WebpExportParams{Quality: 70, Lossless: false, ReductionEffort: 1})
		processDone <- struct {
			buf []byte
			err error
		}{buf, ierr}
	}()

	// Wait for processing or cancellation
	var buf []byte
	select {
	case <-ctx.Done():
		return errors.New("aborted during image processing")
	case res := <-processDone:
		if res.err != nil {
			return fmt.Errorf("image processing failed: %w", res.err)
		}
		buf = res.buf
	}

	// Prepare upload
	uploadCtx, uploadCancel := context.WithTimeout(ctx, s3Timeout)
	defer uploadCancel()

	id, ok := obj.Metadata["id"]
	if !ok {
		return errors.New("missing required metadata: id")
	}

	processedKey := fmt.Sprintf("processed/%s.webp", id)

	putInput := &s3.PutObjectInput{
		Bucket:      aws.String(event.Bucket),
		Key:         aws.String(processedKey),
		Body:        bytes.NewReader(buf),
		Metadata:    obj.Metadata,
		ContentType: aws.String("image/webp"),
	}

	// Upload is cancellable via ctx
	if _, err := uploader.Upload(uploadCtx, putInput); err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	// Delete original using ctx — cancellable
	delCtx, delCancel := context.WithTimeout(ctx, s3Timeout)
	defer delCancel()

	if _, err := client.DeleteObject(delCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(event.Bucket),
		Key:    aws.String(event.Key),
	}); err != nil {
		log.Printf("Warning: failed to delete original object %s: %v", event.Key, err)
		// don't fail the whole job if delete fails
	}

	return nil
}
