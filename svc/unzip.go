package svc

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"ditto.co.jp/agent-s3unzip/cx"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/klauspost/pgzip"
)

//Unzip -
func (s *Service) Unzip(pkg *cx.Package, bucket, key string) error {
	offset := len(pkg.Prefix)
	pkgmap := make(map[string]*cx.File)
	for _, fi := range pkg.Data {
		key := fi.Key[offset:]
		if !strings.HasPrefix(key, "/") {
			key = "/" + key
		}
		if fi.Num > 0 {
			key = fmt.Sprintf("%v.%v", key, fi.Num)
		}
		//フォルダー
		if fi.IsDir {
			input := &s3.PutObjectInput{
				Bucket: aws.String(fi.Bucket),
				Key:    aws.String(fi.Key),
			}
			s.Client().PutObject(input)

			continue
		}

		pkgmap[key] = fi
	}

	log.Printf("Bucket: %v", bucket)
	log.Printf("Key   : %v", key)
	//download .gz
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	obj, err := s.Client().GetObject(input)
	if err != nil {
		return err
	}
	defer obj.Body.Close()

	//zip reader
	r, err := pgzip.NewReader(obj.Body)
	if err != nil {
		return err
	}
	defer r.Close()

	//tar reader
	tr := tar.NewReader(r)
	for {
		th, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("tar.Next() failed. %v", err)
		}
		log.Printf("Name: %v", th.Name)
		switch th.Typeflag {
		case tar.TypeDir:
			log.Printf("directory")

		case tar.TypeReg:
			var fi *cx.File
			var ok bool
			if fi, ok = pkgmap[th.Name]; ok {
				rp, wp := io.Pipe()
				wg := new(sync.WaitGroup)
				wg.Add(2)

				go func() {
					defer wp.Close()
					if _, err = io.Copy(wp, tr); err != nil {
						log.Fatalf("copy() failed. %v", err)
					}

					wg.Done()
				}()
				go func() {
					defer rp.Close()

					//Multipart
					if fi.Num > 0 {
						buf := new(bytes.Buffer)
						l, err := io.Copy(buf, rp)
						if err != nil {
							log.Fatalf("%v copy() failed. %v", th.Name, err)
						}
						log.Printf("Part#%v : Readed bytes: %v (File size: %v)", fi.Num, l, fi.Length)
						input := &s3.UploadPartInput{
							Body:          bytes.NewReader(buf.Bytes()),
							Bucket:        aws.String(fi.Bucket),
							Key:           aws.String(fi.Key),
							PartNumber:    aws.Int64(int64(fi.Num)),
							UploadId:      aws.String(fi.UploadID),
							ContentLength: aws.Int64(l),
						}

						resp, err := s.Client().UploadPart(input)
						if err != nil {
							log.Fatal(err)
						} else {
							log.Printf("Part#%v uploaded: %v", fi.Num, *resp.ETag)
							// err = s.CompletePart(f, *resp.ETag)
							// if err != nil {
							// 	log.Fatal(err)
							// }
							err = s.CompletePart(fi)
							if err != nil {
								log.Print(err)
							}
						}

					} else {
						put := &s3manager.UploadInput{
							Body:   rp,
							Bucket: aws.String(fi.Bucket),
							Key:    aws.String(fi.Key),
						}
						resp, err := s.Uploader().Upload(put)
						if err != nil {
							log.Fatal(err)
						}
						log.Printf("successfully. %v", resp.Location)
					}
					wg.Done()
				}()

				wg.Wait()
				log.Println("end.")
			}
		default:
			log.Fatalf("unknown type: %v in %v", th.Typeflag, th.Name)
		}
	}

	return nil
}
