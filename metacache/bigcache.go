package metacache

import (
	"encoding/json"
	"github.com/allegro/bigcache"
	"github.com/go-kratos/kratos/v2/log"
	"time"
)

var defaultConfig = bigcache.Config{
	Shards:             1024,
	LifeWindow:         time.Hour * 24,
	CleanWindow:        time.Second * 2,
	MaxEntriesInWindow: 5000,
	MaxEntrySize:       2048,
}

type LocalCache[T any] struct {
	cache *bigcache.BigCache
}

func NewLocalCache[T any](cfg *bigcache.Config) (*LocalCache[T], error) {
	if cfg == nil {
		cfg = &defaultConfig
	}
	cache, err := bigcache.NewBigCache(*cfg)
	if err != nil {
		return nil, err
	}
	return &LocalCache[T]{
		cache: cache,
	}, nil
}

func (l *LocalCache[T]) Get(key string) (res *T) {
	resBytes, err := l.cache.Get(key)
	if err != nil {
		if err != bigcache.ErrEntryNotFound {
			log.Errorf("fail to get object from localcache, key:%v, err:%v", key, err)
		}
		return nil
	}
	res = new(T)
	if err = json.Unmarshal(resBytes, &res); err != nil {
		log.Errorf("fail to unmarshal %T, key:%v, err:%v", res, key, err)
		return nil
	}
	return res
}

func (l *LocalCache[T]) Set(key string, obj *T) {
	objBytes, err := json.Marshal(obj)
	if err != nil {
		log.Errorf("fail to marshal %T, key:%v, err:%v", obj, key, err)
		return
	}
	if err = l.cache.Set(key, objBytes); err != nil {
		log.Errorf("fail to set obj to localcache, key:%v, err:%v", key, err)
	}
}

func (l *LocalCache[T]) Delete(key string) {
	if err := l.cache.Delete(key); err != nil {
		log.Errorf("fail to delete obj from localcache, key:%v, err:%v", key, err)
	}
}
