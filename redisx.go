package main

import "github.com/redis/go-redis/v9"

func (p ReportorPlugin) NewRedisClusterManager() *redis.ClusterClient {
	// 解析redis-config

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{p.RedisHost},
		Password: p.RedisPass,
	})

	return rdb

}

func (p ReportorPlugin) NewRedisManager() *redis.Client {
	// 解析redis-config

	if len(p.RedisHost) > 1 {
		var rdb = redis.NewClient(&redis.Options{
			Addr:     p.RedisHost,
			Password: p.RedisPass,
			PoolSize: 100,
		})
		return rdb
	}

	return nil

}
