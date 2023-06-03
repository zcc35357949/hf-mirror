package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"hf-mirror/fs"
	"hf-mirror/metacache"
	"hf-mirror/oss"
	"hf-mirror/proxy"
	"net/http"
	"os"
)

func init() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		panic(err)
	}
	metaCache := metacache.NewMetaDataCache(cfg.MetaCache)
	localCache := fs.NewFileCache(cfg.LocalCache)
	remoteCache := oss.NewRemoteCache(cfg.RemoteCache)
	h := proxy.NewHFProxy(cfg.Proxy, metaCache, localCache, remoteCache)
	http.Handle("/", h)

	server := &http.Server{
		Addr:    cfg.Proxy.Addr,
		Handler: h,
	}
	log.Fatal(server.ListenAndServe())
}

type Config struct {
	Proxy       *proxy.ProxyConfig    `yaml:"proxy"`
	MetaCache   *metacache.MetaConfig `yaml:"meta_cache"`
	LocalCache  *fs.LocalCacheConfig  `yaml:"local_cache"`
	RemoteCache *oss.OssCacheConfig   `yaml:"remote_cache"`
}

func NewConfig() *Config {
	return &Config{
		Proxy:       proxy.NewConfig(),
		MetaCache:   metacache.NewMetaConfig(),
		LocalCache:  fs.NewConfig(),
		RemoteCache: oss.NewOssCacheConfig(),
	}
}

func loadConfig() (*Config, error) {
	cfg := NewConfig()
	raw, err := os.ReadFile("config.yaml")
	if err != nil {
		return nil, err
	}
	if err = yaml.Unmarshal(raw, cfg); err != nil {
		return nil, err
	}
	out, err := yaml.Marshal(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return nil, err
	} else {
		fmt.Printf("%s\n", out)
	}
	return cfg, nil
}
