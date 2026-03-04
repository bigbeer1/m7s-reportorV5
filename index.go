package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/denisbrodbeck/machineid"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"m7s.live/v5"
	"maps"
	"slices"
	"time"
)

var defaultYaml m7s.DefaultYaml
var _ = m7s.InstallPlugin[ReportorPlugin](m7s.PluginMeta{
	DefaultYaml: defaultYaml,
})

type ReportorPlugin struct {
	m7s.Plugin
	monibucaId string // m7sId 唯一标识
	RedisHost  string `yaml:"redishost"`

	RedisType string `default:"node"` // redis类型
	RedisPass string // redis密码

	SyncServiceTime int64 `default:"30"`  // 同步服务器信息在线状态时间
	SyncTime        int64 `default:"30"`  // 同步阻塞时间
	SyncSaveTime    int64 `default:"180"` // 同步数据有效期时间

	MonibucaIp string // 用于设置MonibucaIp方便集群调度

	MonibucaPort string //用于设置MonibucaPort方便集群调度

	redisCluster *redis.ClusterClient // redisCluster客户端
	redis        *redis.Client        // redis客户端
}

type VideoChannel struct {
	StreamPath   string `json:"stream_path"`   // 流通道地址
	MonibucaId   string `json:"monibuca_id"`   // 服务器ID
	MonibucaIp   string `json:"monibuca_ip"`   // 服务器IP
	MonibucaPort string `json:"monibuca_port"` // 服务器Port
}

func (p *ReportorPlugin) Start() (err error) {
	id, _ := machineid.ProtectedID("monibuca")
	if id == "" {
		id = uuid.NewString()
	}
	p.monibucaId = id

	if p.MonibucaIp == "" {
		p.MonibucaIp = "127.0.0.1"
	}

	if p.MonibucaPort == "" {
		p.MonibucaPort = "8080"
	}

	if len(p.RedisHost) > 0 {
		switch p.RedisType {
		case "node":
			//  单体redis
			p.redis = p.NewRedisManager()

		case "cluster":
			// 集群redis
			p.redisCluster = p.NewRedisClusterManager()

		}
	}

	//同步服务器状态
	go p.SyncServiceWorker()
	go p.SyncWorker()

	return
}

// 开启同步任务
func (p *ReportorPlugin) SyncWorker() {
	for {
		time.Sleep(time.Second * time.Duration(p.SyncTime))
		p.SyncVideoChannels()
	}

}

func (p *ReportorPlugin) SyncServiceWorker() {
	for {
		// GB28181设备信息
		p.SyncService()
		time.Sleep(time.Second * time.Duration(p.SyncServiceTime))
	}

}

type M7sServiceInfo struct {
	StartTime time.Time //启动时间
	LocalIP   string
	Port      string
	Version   string
}

// 同步m7s服务端信息
func (p *ReportorPlugin) SyncService() {

	key := fmt.Sprintf("m7sService:%v", p.monibucaId)

	// 获取m7s 全局变量SysInfo  重新组装 这样容器中IP就不会存在这里了
	sysInfo := &M7sServiceInfo{StartTime: p.Server.StartTime, LocalIP: p.MonibucaIp, Port: p.MonibucaPort, Version: m7s.Version}

	data, err := json.Marshal(sysInfo)
	if err != nil {
		p.Error(fmt.Sprintf("m7sService设备数据反序列化失败:%s", err.Error()))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if p.redis != nil {
		cmd := p.redis.Set(ctx, key, data, time.Second*time.Duration(p.SyncSaveTime))
		if cmd.Err() != nil {
			p.Error(fmt.Sprintf("redis数据同步失败:%s", cmd.Err().Error()))
			return
		}
	}

	if p.redisCluster != nil {
		cmd := p.redisCluster.Set(ctx, key, data, time.Second*time.Duration(p.SyncSaveTime))
		if cmd.Err() != nil {
			p.Error(fmt.Sprintf("redis数据同步失败:%s", cmd.Err().Error()))
			return
		}
	}

}

// 同步流通道
func (p *ReportorPlugin) SyncVideoChannels() {

	p.Server.Call(func() {
		for plugin := range p.Server.Plugins.Range {
			if pullPlugin, ok := plugin.GetHandler().(m7s.IPullerPlugin); ok {
				for _, streamPath := range pullPlugin.GetPullableList() {
					p.SaveDataToRedis(streamPath)
				}
			} else if plugin.Meta.NewPuller != nil {
				for _, streamPath := range slices.Collect(maps.Keys(plugin.GetCommonConf().OnSub.Pull)) {
					p.SaveDataToRedis(streamPath.String())
				}
			}
		}
	})

}

func (p *ReportorPlugin) SaveDataToRedis(streamPath string) {
	publicKey := fmt.Sprintf("streamPath:%v", streamPath)
	privateKey := fmt.Sprintf("m7s:%v:streamPath:%v", p.monibucaId, streamPath)

	videoChannel := &VideoChannel{
		StreamPath:   streamPath,
		MonibucaId:   p.monibucaId,
		MonibucaIp:   p.MonibucaIp,
		MonibucaPort: p.MonibucaPort,
	}
	// 反序列化
	data, err := json.Marshal(videoChannel)
	if err != nil {
		p.Error(fmt.Sprintf("SyncVideoChannel设备数据反序列化失败:%s", err.Error()))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if p.redis != nil {
		cmd := p.redis.Set(ctx, publicKey, data, time.Second*time.Duration(p.SyncSaveTime))
		if cmd.Err() != nil {
			p.Error(fmt.Sprintf("redis数据同步失败:%s", cmd.Err().Error()))
			return
		}

		cmd = p.redis.Set(ctx, privateKey, data, time.Second*time.Duration(p.SyncSaveTime))
		if cmd.Err() != nil {
			p.Error(fmt.Sprintf("redis数据同步失败:%s", cmd.Err().Error()))
			return
		}
	}

	if p.redisCluster != nil {
		cmd := p.redisCluster.Set(ctx, publicKey, data, time.Second*time.Duration(p.SyncSaveTime))
		if cmd.Err() != nil {
			p.Error(fmt.Sprintf("redis数据同步失败:%s", cmd.Err().Error()))
			return
		}

		cmd = p.redisCluster.Set(ctx, privateKey, data, time.Second*time.Duration(p.SyncSaveTime))
		if cmd.Err() != nil {
			p.Error(fmt.Sprintf("redis数据同步失败:%s", cmd.Err().Error()))
			return
		}

	}
}
