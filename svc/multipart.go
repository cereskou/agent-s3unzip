package svc

import (
	"log"
	"sort"
	"strings"

	"ditto.co.jp/agent-s3unzip/cx"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

//AbortMultiPart -
func (s *Service) AbortMultiPart(fi *cx.File) error {
	abort := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(fi.Bucket),
		Key:      aws.String(fi.Key),
		UploadId: aws.String(fi.UploadID),
	}

	_, err := s._client.AbortMultipartUpload(abort)
	if err != nil {
		return err
	}
	return nil
}

//CompletePart -
func (s *Service) CompletePart(fi *cx.File) error {
	input := &s3.ListPartsInput{
		Bucket:   aws.String(fi.Bucket),
		Key:      aws.String(fi.Key),
		UploadId: aws.String(fi.UploadID),
	}
	resp, err := s._client.ListParts(input)
	if err != nil {
		return err
	}

	//パート数
	if int(fi.Total) == len(resp.Parts) {
		parts := make([]*s3.CompletedPart, 0)
		for _, p := range resp.Parts {
			parts = append(parts, &s3.CompletedPart{
				ETag:       p.ETag,
				PartNumber: p.PartNumber,
			})
		}
		//sort - Parts must be ordered by part number.
		sort.SliceStable(parts, func(i, j int) bool {
			return *parts[i].PartNumber < *parts[j].PartNumber
		})

		completeInput := &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(fi.Bucket),
			Key:      aws.String(fi.Key),
			UploadId: aws.String(fi.UploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: parts,
			},
		}

		resp, err := s._client.CompleteMultipartUpload(completeInput)
		if err != nil {
			//排他
			if strings.Index(err.Error(), "NoSuchUpload:") < 0 {
				s.AbortMultiPart(fi)

				return err
			}
		}
		log.Printf("CompleteMultipartUpload: %v", *resp.Location)
	}

	return nil
}
