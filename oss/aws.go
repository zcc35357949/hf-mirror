package oss

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"
	"io"
	"net/url"
	"os"
)

type Config struct {
	Endpoint string `yaml:"endpoint"`
	Region   string `yaml:"region"`
	Ak       string `yaml:"ak"`
	Sk       string `yaml:"sk"`
	Bucket   string `yaml:"bucket"`
}

func newSession(cfg *Config) *session.Session {
	sess := session.Must(session.NewSession(&aws.Config{
		DisableSSL:                aws.Bool(true),
		Endpoint:                  aws.String(cfg.Endpoint),
		Region:                    aws.String(cfg.Region),
		DisableEndpointHostPrefix: aws.Bool(true),
		DisableComputeChecksums:   aws.Bool(true),
		S3ForcePathStyle:          aws.Bool(true),
		S3Disable100Continue:      aws.Bool(true),
		Credentials: credentials.NewStaticCredentials(
			cfg.Ak,
			cfg.Sk,
			"", // <yourToken>，可以不填。
		),
	}))
	return sess
}

func NewS3Client(cfg *Config) *S3 {
	sess := newSession(cfg)
	return &S3{
		client: s3.New(sess),
		cfg:    cfg,
	}
}

type S3 struct {
	client *s3.S3
	cfg    *Config
}

func fileMd5(localFile string) (string, error) {
	fp, err := os.Open(localFile)
	if err != nil {
		return "", fmt.Errorf("failed to open file %v, %v", localFile, err)
	}
	defer fp.Close()

	h := md5.New()
	_, err = io.Copy(h, fp)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)), nil
}

func (s *S3) UploadFile(localFile, remoteFile string) error {
	fp, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("failed to open file %v, %v", localFile, err)
	}
	defer fp.Close()

	md5Sum, err := fileMd5(localFile)
	if err != nil {
		return err
	}
	result, err := s.client.PutObject(&s3.PutObjectInput{
		Body:       aws.ReadSeekCloser(fp),
		Bucket:     aws.String(s.cfg.Bucket),
		Key:        aws.String(remoteFile),
		ContentMD5: &md5Sum,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file %v, %v", localFile, err)
	}

	log.Infof("file uploaded success, %s", result)
	return nil
}

func (s *S3) GetRequest(remoteFile string) (string, error) {
	return url.JoinPath(s.cfg.Endpoint, s.cfg.Bucket, remoteFile)
}

func (s *S3) StatFile(remoteFile string) error {
	_, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(remoteFile),
	})
	return err
}
