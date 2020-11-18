package cx

import "time"

//File -
type File struct {
	Err         error     `json:"-"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Local       string    `json:"local"`
	IsDir       bool      `jong:"isdir"`
	Size        int64     `json:"size"`
	Md5         string    `json:"md5"`
	ContentType string    `json:"contentType"`
	ModTime     time.Time `json:"modtime"`
	Num         uint32    `json:"num"`
	Offset      int64     `json:"offset"`
	Length      int64     `json:"length"`
	Total       int64     `json:"total"`
	UploadID    string    `json:"uploadId"`
}

//Package - put job/ get job
type Package struct {
	ID       int64   `json:"id"`       //Package id
	Tag      string  `json:"tag"`      //identity tag for client's job
	Dir      string  `json:"dir"`      //save to
	S3       string  `json:"s3"`       //s3 url
	Bucket   string  `json:"bucket"`   //upload to s3 bucket
	Prefix   string  `json:"prefix"`   //upload to s3 prefix
	Work     string  `json:"-"`        //workdirectory
	Md5      bool    `json:"md5"`      //md5 check flag
	Count    int64   `json:"count"`    //Data count
	PartSize int64   `json:"partsize"` //multi-part size
	Data     []*File `json:"data"`     //data array
}
