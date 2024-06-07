package client

import (
	"context"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/kitex-contrib/config-zookeeper/pkg/degradation"
	"github.com/kitex-contrib/config-zookeeper/utils"
	"github.com/kitex-contrib/config-zookeeper/zookeeper"
)
	
func WithDegradation(dest, src string, zookeeperClient zookeeper.Client, opts utils.Options) []client.Option {
    param, err := zookeeperClient.ClientConfigParam(&zookeeper.ConfigParamConfig{
       Category:          degradationConfigName,
       ServerServiceName: dest,
       ClientServiceName: src,
    })
    if err != nil {
       panic(err)
    }

	for _, f := range opts.ZookeeperCustomFunctions {
		f(&param)
	}

	uid := zookeeper.GetUniqueID()
	path := param.Prefix + "/" + param.Path
	container := initDegradation(path, uid, dest, zookeeperClient)
	return []client.Option{
		client.WithACLRules(container.GetAclRule()),
		client.WithCloseCallbacks(func() error {
			// cancel the configuration listener when client is closed.
			zookeeperClient.DeregisterConfig(path, uid)
			return nil
		}),
	}
}

func initDegradation(path string, uniqueID int64, dest string, zookeeperClient zookeeper.Client) *degradation.Container {
	container := degradation.NewContainer()
	onChangeCallback := func(restoreDefault bool, data string, parser zookeeper.ConfigParser) {
		config := &degradation.Config{}
		if !restoreDefault {
			err := parser.Decode(data, config)
			if err != nil {
				klog.Warnf("[zookeeper] %s server etcd degradation config: unmarshal data %s failed: %s, skip...", path, data, err)
				return
			}
		}
		container.NotifyPolicyChange(config)
	}

	zookeeperClient.RegisterConfigCallback(context.Background(), path, uniqueID, onChangeCallback)

	return container
}