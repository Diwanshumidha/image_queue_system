package s3_internal

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewS3Client() (*s3.Client, error) {
	cr := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin123", ""))
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithBaseEndpoint("http://localhost:9000"),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(cr),
	)

	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return client, nil
}

const Bucket = "image-queue"
