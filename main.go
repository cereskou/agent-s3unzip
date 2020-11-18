package main

import (
	"context"
	"log"
	"strings"

	"ditto.co.jp/agent-s3unzip/svc"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

//Handler - package json
func Handler(ctx context.Context, req events.S3Event) error {
	bucket := req.Records[0].S3.Bucket.Name
	key := req.Records[0].S3.Object.Key

	log.Printf("Bucket: %v", bucket)
	log.Printf("Key   : %v", key)

	sv := svc.NewService()
	//read package file
	pkg, err := sv.ReadPackage(bucket, key)
	if err != nil {
		return err
	}

	//.gz file
	gz := strings.ReplaceAll(key, ".json", ".gz")
	err = sv.Unzip(pkg, bucket, gz)
	if err != nil {
		return err
	}

	// clear
	//delete .json
	deleteInput := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	resp, err := sv.Client().DeleteObject(deleteInput)
	if err != nil {
		return err
	}
	log.Printf("delete %v", resp.GoString())
	//delete .gz
	deleteInput = &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(gz),
	}
	resp, err = sv.Client().DeleteObject(deleteInput)
	if err != nil {
		return err
	}
	log.Printf("delete %v", resp.GoString())

	return nil
}

func main() {
	lambda.Start(Handler)
}
