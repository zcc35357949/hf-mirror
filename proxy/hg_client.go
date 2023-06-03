package proxy

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hf-mirror/metacache"
	"net/http"
	"time"
)

type HGClient struct {
	cli *http.Client
}

func NewHGClient() *HGClient {
	return &HGClient{
		cli: &http.Client{
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Timeout: time.Second * 5,
		},
	}
}

func (h *HGClient) FileMeta(prj, file, rev string) metacache.FileMetadata {
	url := fmt.Sprintf("https://huggingface.co/%s/resolve/%s/%s", prj, rev, file)
	res, err := h.cli.Head(url)
	if err != nil {
		log.Errorf("head file meta failed, url:%v, err:%v", url, err)
		return metacache.FileMetadata{}
	}
	defer res.Body.Close()
	return HfFileMetadata(res)
}
