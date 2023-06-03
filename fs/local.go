package fs

import (
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	defaultBlobDir = ""
	tmpfile_suffix = "_tmp"
)

type LocalCacheConfig struct {
	CacheDir string `yaml:"cache_dir"`
}

func NewConfig() *LocalCacheConfig {
	return &LocalCacheConfig{
		CacheDir: defaultBlobDir,
	}
}

type FileLocalCache interface {
	CreateBlobWriter(etag string, expectLen int64, onFinish func()) (io.WriteCloser, error)
	HasFile(etag string) bool
	GetFilePath(etag string) string
	FileHandler() http.Handler
}

type fileLocalCache struct {
	fsHandler http.Handler
	blobdir   string
}

func NewFileCache(cfg *LocalCacheConfig) FileLocalCache {
	os.MkdirAll(cfg.CacheDir, 0766)
	return &fileLocalCache{
		fsHandler: http.FileServer(http.Dir(cfg.CacheDir)),
		blobdir:   cfg.CacheDir,
	}
}

type fileDownloadWriter struct {
	fd        int
	tmpFile   string
	expectLen int64
	readLen   int64
	io.WriteCloser
	onFinish func()
}

func NewFileDownloadWriter(file string, expectLen int64, onFinish func()) (io.WriteCloser, error) {
	file = file + tmpfile_suffix
	fd, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.WithFields(log.Fields{"file": file}).Errorf("fail to open file, err:%v", err)
		return nil, err
	}
	fdInt := int(fd.Fd())
	if err := syscall.Flock(fdInt, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	return &fileDownloadWriter{
		tmpFile:     file,
		expectLen:   expectLen,
		WriteCloser: fd,
		fd:          fdInt,
		onFinish:    onFinish,
	}, nil
}

func (f *fileDownloadWriter) Close() error {
	var err error
	if f.readLen == f.expectLen {
		err = os.Rename(f.tmpFile, strings.TrimSuffix(f.tmpFile, tmpfile_suffix))
		if err == nil {
			f.onFinish()
		}
	} else {
		err = os.RemoveAll(f.tmpFile)
	}
	if err != nil {
		return err
	}
	if err = syscall.Flock(f.fd, syscall.LOCK_UN); err != nil {
		log.Warnf("unlock file failed, err:%v", err)
	}
	return f.WriteCloser.Close()
}

func (f *fileDownloadWriter) Write(p []byte) (n int, err error) {
	n, err = f.WriteCloser.Write(p)
	if n >= 0 {
		f.readLen += int64(n)
	}
	return n, err
}

func (f *fileLocalCache) CreateBlobWriter(etag string, expectLen int64, onFinish func()) (io.WriteCloser, error) {
	file := filepath.Join(f.blobdir, etag)
	return NewFileDownloadWriter(file, expectLen, onFinish)
}

func (f *fileLocalCache) HasFile(etag string) bool {
	file := filepath.Join(f.blobdir, etag)
	_, err := os.Stat(file)
	if err != nil {
		return false
	}
	return true
}

func (f *fileLocalCache) GetFilePath(etag string) string {
	return filepath.Join(f.blobdir, etag)
}

func (f *fileLocalCache) FileHandler() http.Handler {
	return f.fsHandler
}
