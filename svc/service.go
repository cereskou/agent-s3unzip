package svc

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// bytes size
const (
	_       = iota
	KB uint = 1 << (10 * iota)
	MB
	GB
	TB
)

//Service -
type Service struct {
	_session  *session.Session
	_client   *s3.S3
	_uploader *s3manager.Uploader
}

//NewService -
func NewService() *Service {
	s := &Service{
		_session: session.New(),
	}

	s._client = s3.New(s._session)
	s._uploader = s3manager.NewUploader(s._session, func(u *s3manager.Uploader) {
		u.PartSize = int64(8 * MB)
		u.S3 = s._client
	})

	return s
}

//Client - s3 client
func (s *Service) Client() *s3.S3 {
	return s._client
}

//Uploader - s3manager uploader
func (s *Service) Uploader() *s3manager.Uploader {
	return s._uploader
}
