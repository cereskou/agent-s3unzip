package svc

import (
	"ditto.co.jp/agent-s3unzip/cx"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

//ReadPackage -
func (s *Service) ReadPackage(bucket string, key string) (*cx.Package, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	obj, err := s.Client().GetObject(input)
	if err != nil {
		return nil, err
	}
	defer obj.Body.Close()

	var pkg cx.Package
	err = cx.JSON.NewDecoder(obj.Body).Decode(&pkg)
	if err != nil {
		return nil, err
	}

	return &pkg, nil
}
