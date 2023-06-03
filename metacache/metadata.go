package metacache

import (
	"fmt"
	"github.com/allegro/bigcache"
	"strings"
	"sync"
	"time"
)

type FileMetadata struct {
	Tag        string `json:"tag,omitempty"`
	CommitHash string `json:"commit_hash,omitempty"`
	Etag       string `json:"etag,omitempty"`
	Location   string `json:"location,omitempty"`
	Size       string `json:"size,omitempty"`
}

type metadataCache struct {
	mux   sync.RWMutex
	cache *LocalCache[[]*FileMetadata]
}

type MetaDataCache interface {
	AppendMetadata(project, file string, meta *FileMetadata)
	SearchMetaData(project, file string, revision string) *FileMetadata
}

type MetaConfig struct {
	Shards             int           `yaml:"shards"`
	LifeWindow         time.Duration `yaml:"life_window"`
	CleanWindow        time.Duration `yaml:"clean_window"`
	MaxEntriesInWindow int           `yaml:"max_entries_in_window"`
	MaxEntrySize       int           `yaml:"max_entry_size"`
}

func NewMetaConfig() *MetaConfig {
	return &MetaConfig{
		Shards:             1024,
		LifeWindow:         time.Hour * 24,
		CleanWindow:        time.Second * 10,
		MaxEntriesInWindow: 1000,
		MaxEntrySize:       4096,
	}
}

func NewMetaDataCache(cfg *MetaConfig) MetaDataCache {
	c, err := NewLocalCache[[]*FileMetadata](&bigcache.Config{
		Shards:             cfg.Shards,
		LifeWindow:         cfg.LifeWindow,
		CleanWindow:        cfg.CleanWindow,
		MaxEntriesInWindow: cfg.MaxEntriesInWindow,
		MaxEntrySize:       cfg.MaxEntrySize,
	})
	if err != nil {
		panic(err)
	}
	return &metadataCache{
		cache: c,
	}
}

func getMetaKey(project, file string) string {
	return fmt.Sprintf("%v_%v", project, file)
}

func (m *metadataCache) AppendMetadata(project, file string, meta *FileMetadata) {
	m.mux.Lock()
	defer m.mux.Unlock()
	key := getMetaKey(project, file)
	metasPt := m.cache.Get(key)
	var metas []*FileMetadata
	if metasPt != nil {
		metas = *metasPt
	}
	var found bool
	for i, m := range metas {
		if m.Tag == meta.Tag || m.CommitHash == meta.CommitHash {
			metas[i] = meta
			found = true
			break
		}
	}
	if !found {
		metas = append(metas, meta)
	}
	m.cache.Set(key, &metas)
}

func (m *metadataCache) SearchMetaData(project, file string, revision string) *FileMetadata {
	key := getMetaKey(project, file)
	m.mux.RLock()
	metasPt := m.cache.Get(key)
	m.mux.RUnlock()

	var metas []*FileMetadata
	if metasPt != nil {
		metas = *metasPt
	}
	for _, meta := range metas {
		if meta.Tag == revision {
			return meta
		}
		if strings.HasPrefix(meta.CommitHash, revision) {
			return meta
		}
	}
	return nil
}
