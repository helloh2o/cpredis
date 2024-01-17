package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/helloh2o/lucky/cache"
	"github.com/helloh2o/lucky/log"
	"sync/atomic"
	"time"
)

/*
# 这是个兼容Redis高版本拷贝数据到低版本的工具
*/
var (
	from  = flag.String("from", "47.109.38.173:6380", "from redis")
	to    = flag.String("to", "127.0.0.1:19000", "to redis")
	db    = flag.Int("db", 0, "redis db index")
	force = flag.Bool("force", false, "force rewrite key")
)

var (
	err          error
	cursor       uint64 
	succeed      int64
	fromRC, toRC *redis.Client
)

func main() {
	flag.Parse()
	if fromRC, err = cache.OpenRedis(fmt.Sprintf("redis://%s/?pwd=&db=%d", *from, *db)); err != nil {
		panic(err)
	}
	if toRC, err = cache.OpenRedis(fmt.Sprintf("redis://%s/?pwd=&db=%d", *to, *db)); err != nil {
		panic(err)
	}
	iter := fromRC.Scan(context.Background(), 0, "*", 0).Iterator()
	if err != nil {
		log.Error("push scan cache error %v", err)
	} else {
		begin := time.Now().Unix()
		defer func() {
			costs := time.Now().Unix() - begin
			log.Release("copy costs:%d seconds", costs)
		}()
		for iter.Next(context.Background()) {
			ctx := context.Background()
			cursor++
			key := iter.Val()
			if !*force {
				if n, _ := toRC.Exists(ctx, key).Result(); n == 1 {
					log.Debug("key:%s is existed on target", key)
					continue
				}
			} else {
				log.Release("cpKey key:%s, cursor:%d, succeed:%d", key, cursor, atomic.LoadInt64(&succeed))
				ttl := fromRC.TTL(ctx, key).Val()
				if ttl <= 0 {
					go func() {
						cpKey(ttl, key)
					}()
				} else {
					cpKey(ttl, key)
				}
			}
		}
	}
}

func cpKey(ttl time.Duration, key string) {
	ctx := context.Background()
	rkType := fromRC.Type(ctx, key).Val()
	switch rkType {
	case "string":
		if ttl == -1 {
			ttl = 0
		}
		val := fromRC.Get(ctx, key).Val()
		if err = toRC.Set(ctx, key, val, ttl).Err(); err != nil {
			panic(fmt.Sprintf("cursor:%d, %s key:%s, err%v", cursor, rkType, key, err))
		}
		atomic.AddInt64(&succeed, 1)
	case "list":
		list := fromRC.LRange(ctx, key, 0, -1).Val()
		for _, val := range list {
			if err = toRC.RPush(ctx, key, val).Err(); err != nil {
				panic(fmt.Sprintf("cursor:%d, %s key:%s, err%v", cursor, rkType, key, err))
			}
		}
		atomic.AddInt64(&succeed, 1)
		if ttl > 0 {
			toRC.Expire(ctx, key, ttl)
		}
	case "hash":
		hmp := fromRC.HGetAll(ctx, key).Val()
		for fld, val := range hmp {
			// HMSet is a deprecated version of HSet left for compatibility with Redis 3
			if err = toRC.HSet(ctx, key, []string{fld, val}).Err(); err != nil {
				panic(fmt.Sprintf("cursor:%d, %s key:%s, err%v", cursor, rkType, key, err))
			}
		}
		atomic.AddInt64(&succeed, 1)
		if ttl > 0 {
			toRC.Expire(ctx, key, ttl)
		}
	case "zset":
		scores := fromRC.ZRangeWithScores(ctx, key, 0, -1).Val()
		for i := 0; i < len(scores); i++ {
			z := scores[i]
			_, err = toRC.ZAdd(context.Background(), key, &z).Result()
			if err != nil {
				panic(fmt.Sprintf("cursor:%d, %s key:%s, err%v", cursor, rkType, key, err))
			}
		}
		atomic.AddInt64(&succeed, 1)
		if ttl > 0 {
			toRC.Expire(ctx, key, ttl)
		}
	default:
		panic(fmt.Sprintf("un support key type:%s", rkType))
	}
}
