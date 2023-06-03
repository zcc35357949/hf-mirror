package oss

import (
	log "github.com/sirupsen/logrus"
	"path/filepath"
)

const (
	defaultBlobDir = "huggingface/blobs/"
)

type OssCacheConfig struct {
	CacheDir   string  `yaml:"cache_dir"`
	S3         *Config `yaml:"s3"`
	Concurrent int     `yaml:"concurrent"`
}

func NewOssCacheConfig() *OssCacheConfig {
	return &OssCacheConfig{
		CacheDir: defaultBlobDir,
		S3: &Config{
			Endpoint: "",
			Region:   "",
			Bucket:   "",
			Ak:       "",
			Sk:       "",
		},
		Concurrent: 3,
	}
}

type RemoteCache interface {
	UploadFile(file string)
	GetRequest(file string) (string, error)
	StatFile(file string) error
}

type remoteCache struct {
	s3         *S3
	file       chan string
	concurrent int
	blobdir    string
}

func NewRemoteCache(cfg *OssCacheConfig) RemoteCache {
	c := &remoteCache{
		s3:         NewS3Client(cfg.S3),
		file:       make(chan string, 1024),
		concurrent: cfg.Concurrent,
		blobdir:    cfg.CacheDir,
	}
	c.runUploadWorkers()
	return c
}

func (r *remoteCache) runUploadWorkers() {
	for i := 0; i < r.concurrent; i++ {
		go func() {
			for {
				select {
				case localFile := <-r.file:
					remoteFile := r.blobdir + filepath.Base(localFile)
					if err := r.s3.UploadFile(localFile, remoteFile); err != nil {
						log.WithFields(log.Fields{"local": localFile, "remote": remoteFile}).
							Errorf("upload file to s3 error:%v", err)
					}
				}
			}
		}()
	}
}

func (r *remoteCache) UploadFile(file string) {
	remoteFile := r.blobdir + filepath.Base(file)
	if err := r.s3.StatFile(remoteFile); err != nil {
		r.file <- file
	}
}

func (r *remoteCache) GetRequest(file string) (string, error) {
	remoteFile := r.blobdir + filepath.Base(file)
	return r.s3.GetRequest(remoteFile)
}

func (r *remoteCache) StatFile(file string) error {
	remoteFile := r.blobdir + filepath.Base(file)
	return r.s3.StatFile(remoteFile)
}
