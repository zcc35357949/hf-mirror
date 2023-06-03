package proxy

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"hf-mirror/fs"
	"hf-mirror/metacache"
	"hf-mirror/oss"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strings"
)

const (
	HUGGINGFACE_HEADER_X_REPO_COMMIT = "X-Repo-Commit"
	HUGGINGFACE_HEADER_X_LINKED_ETAG = "X-Linked-Etag"
	HUGGINGFACE_HEADER_X_LINKED_SIZE = "X-Linked-Size"

	INJECT_ETAG = "x-etag"
)

var (
	HuggingfaceUrlReg    = regexp.MustCompile("huggingface.co/(.*)/resolve/(.*)")
	LfsHuggingfaceUrlReg = regexp.MustCompile("cdn-lfs.huggingface.co/(.*)/([0-9a-zA-Z]+)?.*")
)

type ProxyConfig struct {
	Addr     string   `yaml:"addr"`
	ProxyUrl string   `yaml:"proxy_url"`
	Targets  []string `yaml:"targets"`
}

func NewConfig() *ProxyConfig {
	return &ProxyConfig{
		Addr:     "0.0.0.0:8082",
		ProxyUrl: "http://127.0.0.1:8082/",
		Targets:  []string{},
	}
}

type hfProxy struct {
	proxyUrl     string
	targets      []string
	targetsProxy map[string]*httputil.ReverseProxy
	metaCache    metacache.MetaDataCache
	fileCache    fs.FileLocalCache
	remoteCache  oss.RemoteCache
	hgClient     *HGClient
}

func NewHFProxy(cfg *ProxyConfig, metaCache metacache.MetaDataCache, localCache fs.FileLocalCache, remoteCache oss.RemoteCache) http.Handler {
	proxies := make(map[string]*httputil.ReverseProxy)
	targets := cfg.Targets
	handler := &hfProxy{
		proxyUrl:     cfg.ProxyUrl,
		targets:      targets,
		targetsProxy: proxies,
		metaCache:    metaCache,
		fileCache:    localCache,
		remoteCache:  remoteCache,
		hgClient:     NewHGClient(),
	}
	for _, tg := range targets {
		tgUrl, _ := url.Parse(tg)
		py := httputil.NewSingleHostReverseProxy(tgUrl)
		d := py.Director
		py.Director = func(r *http.Request) {
			d(r)
			r.Host = tgUrl.Host
		}
		py.ModifyResponse = func(response *http.Response) error {
			code := response.StatusCode
			switch response.Request.Method {
			case http.MethodHead:
				project, file, revision := getFileInfoFromHGUri(response.Request.URL)
				meta := HfFileMetadata(response)
				loc := ModifyHfFileLocation(handler.proxyUrl, meta)
				meta.Tag = revision
				meta.Location = loc
				response.Header.Set("Location", loc)
				if project != "" && file != "" && meta.Etag != "" && meta.Location != "" {
					handler.metaCache.AppendMetadata(project, file, &meta)
				}
			case http.MethodGet:
				etag := handler.getEtagFromUri(response.Request)
				rangeHeader := response.Request.Header.Get("Range")
				if code == http.StatusOK && rangeHeader == "" && response.ContentLength > 0 && etag != "" {
					host := response.Request.URL.Host
					filePath := handler.fileCache.GetFilePath(etag)
					fd, err := handler.fileCache.CreateBlobWriter(etag, response.ContentLength, func() {
						if strings.Contains(host, "huggingface") {
							handler.remoteCache.UploadFile(filePath)
						}
					})
					if err == nil {
						response.Body = NewTeeReadCloser(response.Body, fd)
					} else {
						log.WithFields(log.Fields{"etag": etag}).Errorf("create local file writer failed, err:%v", err)
					}
				}
				redirectUrl := response.Header.Get("Location")
				if (code == http.StatusMovedPermanently || code == http.StatusFound) && redirectUrl != "" {
					response.Header.Set("Location", handler.proxyUrl+redirectUrl)
				}
			}
			return nil
		}
		proxies[tgUrl.Host] = py
	}
	return handler
}

func ModifyHfFileLocation(proxyUrl string, meta metacache.FileMetadata) string {
	if meta.Etag == "" || meta.Location == "" {
		return ""
	}
	replacedLocation := meta.Location
	if !strings.Contains(meta.Location, "cdn-lfs.huggingface.co") {
		uriSubseqs := strings.Split(meta.Location, "/")
		uriSubseqs[len(uriSubseqs)-2] = meta.CommitHash
		replacedLocation = strings.Join(uriSubseqs, "/")
	}
	proxyLocation := proxyUrl + replacedLocation
	locUrl, _ := url.Parse(proxyLocation)
	if locUrl != nil {
		vals := locUrl.Query()
		vals.Set(INJECT_ETAG, meta.Etag)
		locUrl.RawQuery = vals.Encode()
		proxyLocation = locUrl.String()
	}
	return proxyLocation
}

func getFileInfoFromHGUri(uri *url.URL) (project, file, revision string) {
	matches := HuggingfaceUrlReg.FindAllStringSubmatch(uri.String(), -1)
	if len(matches) > 0 && len(matches[0]) == 3 {
		project = matches[0][1]
		revFile := strings.SplitN(matches[0][2], "/", 2)
		if len(revFile) == 2 {
			revision = revFile[0]
			file = revFile[1]
		}
	}
	return project, file, revision
}

func getEtagFromLfsUri(uri *url.URL) string {
	matches := LfsHuggingfaceUrlReg.FindAllStringSubmatch(uri.String(), -1)
	if len(matches) > 0 && len(matches[0]) == 3 {
		return matches[0][2]
	}
	return ""
}

func (h *hfProxy) getEtagFromUri(req *http.Request) (etag string) {
	etag = req.URL.Query().Get(INJECT_ETAG)
	if etag == "" {
		etag = req.Header.Get(INJECT_ETAG)
	}
	return etag
}

func HfFileMetadata(res *http.Response) metacache.FileMetadata {
	if res == nil {
		return metacache.FileMetadata{}
	}
	resHeader := res.Header
	commitHash := resHeader.Get(HUGGINGFACE_HEADER_X_REPO_COMMIT)
	etag := resHeader.Get("ETag")
	if etag == "" {
		etag = resHeader.Get(HUGGINGFACE_HEADER_X_LINKED_ETAG)
	}
	etag = strings.Trim(etag, "\"")
	location := resHeader.Get("Location")
	if location == "" {
		location = res.Request.URL.String()
	}
	size := resHeader.Get(HUGGINGFACE_HEADER_X_LINKED_SIZE)
	if size == "" {
		size = resHeader.Get("Content-Length")
	}
	return metacache.FileMetadata{
		CommitHash: commitHash,
		Etag:       etag,
		Location:   location,
		Size:       size,
	}
}

func (h *hfProxy) ServeLocalFileMeta(rw http.ResponseWriter, req *http.Request) bool {
	project, file, revision := getFileInfoFromHGUri(req.URL)
	meta := h.metaCache.SearchMetaData(project, file, revision)
	if meta == nil {
		return false
	}
	rw.Header().Set("Content-Type", "text/plain; charset=utf-8")
	rw.Header().Set(HUGGINGFACE_HEADER_X_LINKED_SIZE, meta.Size)
	rw.Header().Set(HUGGINGFACE_HEADER_X_REPO_COMMIT, meta.CommitHash)
	rw.Header().Set("Location", meta.Location)
	rw.Header().Set("Accept-Ranges", "bytes")
	rw.Header().Set(HUGGINGFACE_HEADER_X_LINKED_ETAG, fmt.Sprintf("\"%s\"", meta.Etag))
	rw.Header().Set("Accept-Ranges", "bytes")
	rw.WriteHeader(http.StatusOK)
	log.WithFields(log.Fields{"file": file}).Infof("file meta hit cache:%s", req.URL.String())
	return true
}

func (h *hfProxy) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	originUrl := req.URL.String()
	originUrl = strings.TrimPrefix(originUrl, "/")
	realUrl, err := url.Parse(originUrl)
	if err != nil {
		return
	}
	querys := realUrl.Query()
	xetag := querys.Get(INJECT_ETAG)
	querys.Del(INJECT_ETAG)
	realUrl.RawQuery = querys.Encode()
	req.Header.Set(INJECT_ETAG, xetag)
	req.URL = realUrl
	if h.targetsProxy[realUrl.Host] == nil {
		rw.Write([]byte("403: Host forbidden " + originUrl))
		return
	}
	if req.Method == http.MethodHead {
		if h.ServeLocalFileMeta(rw, req) {
			return
		}
	}
	if req.Method == http.MethodGet {
		etag := h.getEtagFromUri(req)
		if etag == "" {
			project, file, rev := getFileInfoFromHGUri(realUrl)
			if project != "" && file != "" && rev != "" {
				// https://huggingface.co
				var meta *metacache.FileMetadata
				meta = h.metaCache.SearchMetaData(project, file, rev)
				if meta == nil {
					metaSource := h.hgClient.FileMeta(project, file, rev)
					loc := ModifyHfFileLocation(h.proxyUrl, metaSource)
					metaSource.Tag = rev
					metaSource.Location = loc
					if metaSource.Etag != "" && metaSource.Location != "" {
						h.metaCache.AppendMetadata(project, file, &metaSource)
					}
					meta = &metaSource
				}
				req.Header.Set(INJECT_ETAG, meta.Etag)
				etag = meta.Etag
			} else {
				// https://cdn-lfs.huggingface.co
				etag = getEtagFromLfsUri(realUrl)
				req.Header.Set(INJECT_ETAG, etag)
			}
		}
		if etag != "" {
			if h.fileCache.HasFile(etag) {
				log.WithFields(log.Fields{"etag": etag}).Infof("file download hit cache")
				req.URL.Path = "/" + etag
				req.URL.RawPath = "/" + etag
				fileServe := h.fileCache.FileHandler()
				fileServe.ServeHTTP(rw, req)
				return
			}
			filePath := h.fileCache.GetFilePath(etag)
			if err = h.remoteCache.StatFile(filePath); err == nil {
				remoteUrlStr, err := h.remoteCache.GetRequest(filePath)
				if err == nil && remoteUrlStr != "" {
					remoteUrl, err := url.Parse(remoteUrlStr)
					remoteStorageProxy := h.targetsProxy[remoteUrl.Host]
					if err == nil && remoteStorageProxy != nil {
						log.WithFields(log.Fields{"url": remoteUrlStr}).Infof("downloading from remote oss storage")
						req.URL = remoteUrl
						remoteStorageProxy.ServeHTTP(rw, req)
						return
					}
				}
			}
		}
	}
	proxy := h.targetsProxy[realUrl.Host]
	proxy.ServeHTTP(rw, req)
}
