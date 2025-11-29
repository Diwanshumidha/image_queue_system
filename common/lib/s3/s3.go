package s3_internal

import (
	"context"
	envutil "diwanshuMidha/common/lib/env"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func NewS3Client() (*s3.Client, error) {

	cr := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(envutil.Get("MINIO_ACCESS_KEY", ""), envutil.Get("MINIO_SECRET_KEY", ""), ""))
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithBaseEndpoint(envutil.Get("MINIO_URL", "http://localhost:9000")),
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
