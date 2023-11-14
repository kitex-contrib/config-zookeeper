// Copyright 2023 CloudWeGo Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package zookeeper

import (
	"bytes"
	"context"
	"html/template"
	"strconv"
	"sync"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/go-zookeeper/zk"
)

var (
	ctxMap = make(map[string]context.CancelFunc)
	mu     sync.Mutex
)

// Client the wrapper of zookeeper client.
type Client interface {
	SetParser(ConfigParser)
	ClientConfigParam(cpc *ConfigParamConfig) (ConfigParam, error)
	ServerConfigParam(cpc *ConfigParamConfig) (ConfigParam, error)
	RegisterConfigCallback(context.Context, string, int64, func(string, ConfigParser))
	DeregisterConfig(string, int64)
}
type client struct {
	zConn *zk.Conn
	// support customise parser
	parser             ConfigParser
	serverPathTemplate *template.Template
	clientPathTemplate *template.Template
	prefixTemplate     *template.Template
}

type ConfigParam struct {
	Prefix string
	Path   string
}

// Options zookeeper config options. All the fields have default value.
type Options struct {
	Server           []string
	Prefix           string
	ServerPathFormat string
	ClientPathFormat string
	CustomLogger     zk.Logger
	ConfigParser     ConfigParser
}

// NewClient Create a default Zookeeper client
func NewClient(opts Options) (Client, error) {
	if opts.Server == nil {
		opts.Server = []string{ZookeeperDefaultServer}
	}
	if opts.Prefix == "" {
		opts.Prefix = ZookeeperDefaultPrefix
	}
	if opts.CustomLogger == nil {
		opts.CustomLogger = NewCustomZookeeperLogger()
	}
	if opts.ConfigParser == nil {
		opts.ConfigParser = defaultConfigParse()
	}
	if opts.ServerPathFormat == "" {
		opts.ServerPathFormat = ZookeeperDefaultServerPath
	}
	if opts.ClientPathFormat == "" {
		opts.ClientPathFormat = ZookeeperDefaultClientPath
	}

	conn, _, err := zk.Connect(opts.Server, time.Second*5, zk.WithLogger(opts.CustomLogger))
	if err != nil {
		return nil, err
	}
	prefixTemplate, err := template.New("prefix").Parse(opts.Prefix)
	if err != nil {
		return nil, err
	}
	serverPathTemplate, err := template.New("serverPathID").Parse(opts.ServerPathFormat)
	if err != nil {
		return nil, err
	}
	clientPathTemplate, err := template.New("clientPathID").Parse(opts.ClientPathFormat)
	if err != nil {
		return nil, err
	}
	c := &client{
		zConn:              conn,
		parser:             opts.ConfigParser,
		serverPathTemplate: serverPathTemplate,
		clientPathTemplate: clientPathTemplate,
		prefixTemplate:     prefixTemplate,
	}
	return c, nil
}

// SetParser support customise parser
func (c *client) SetParser(parser ConfigParser) {
	c.parser = parser
}
func (c *client) render(cpc *ConfigParamConfig, t *template.Template) (string, error) {
	var tpl bytes.Buffer
	err := t.Execute(&tpl, cpc)
	if err != nil {
		return "", err
	}
	return tpl.String(), nil
}

// ServerConfigParam render server config parameters
func (c *client) ServerConfigParam(cpc *ConfigParamConfig) (ConfigParam, error) {
	return c.configParam(cpc, c.serverPathTemplate)
}

// ClientConfigParam render client config parameters
func (c *client) ClientConfigParam(cpc *ConfigParamConfig) (ConfigParam, error) {
	return c.configParam(cpc, c.clientPathTemplate)
}

// configParam render config parameters. All the parameters can be customized with CustomFunction.
// ConfigParam explain:
//  1. Prefix: /KitexConfig by default.
//  2. ServerPath: {{.ServerServiceName}}.{{.Category}} by default.
//     ClientPath: {{.ClientServiceName}}.{{.ServerServiceName}}.{{.Category}} by default.
func (c *client) configParam(cpc *ConfigParamConfig, t *template.Template) (ConfigParam, error) {
	param := ConfigParam{}
	var err error
	param.Path, err = c.render(cpc, t)
	if err != nil {
		return param, err
	}
	param.Prefix, err = c.render(cpc, c.prefixTemplate)
	if err != nil {
		return param, err
	}
	return param, nil
}

// DeregisterConfig deregister the config.
func (c *client) DeregisterConfig(path string, uniqueID int64) {
	mu.Lock()
	clientKey := path + "/" + strconv.FormatInt(uniqueID, 10)
	cancel := ctxMap[clientKey]
	cancel()
	mu.Unlock()
}

// RegisterConfigCallback register the callback function to zookeeper client.
func (c *client) RegisterConfigCallback(ctx context.Context, path string, uniqueID int64, callback func(string, ConfigParser)) {
	clientCtx, cancel := context.WithCancel(context.Background())
	go func() {
		mu.Lock()
		clientKey := path + "/" + strconv.FormatInt(uniqueID, 10)
		ctxMap[clientKey] = cancel
		mu.Unlock()
		for {
			_, _, watchChan, err := c.zConn.ExistsW(path)
			if err != nil {
				klog.Debugf("[zookeeper] watch node %s failed %v", path, err)
				return
			}
			select {
			case <-clientCtx.Done():
				return
			case watchEvent := <-watchChan:
				if watchEvent.Type == zk.EventNodeDataChanged {
					data, _, err := c.zConn.Get(path)
					nodeValue := string(data)
					klog.Debugf("[zookeeper] config path: %s updated,data is %s", path, nodeValue)
					if err != nil {
						klog.Warnf("[zookeeper] get config %s from zookeeper failed: %v", path, err)
					}
					callback(nodeValue, c.parser)
				} else if watchEvent.Type == zk.EventNodeDeleted {
					klog.Debugf("[zookeeper] config path: %s deleted", path)
					callback("", c.parser)
				}
			}
		}
	}()
	data, _, err := c.zConn.Get(path)
	// the zookeeper client has handled the not exist error.
	if err != nil {
		klog.Debugf("[zookeeper] get config %s from zookeeper failed: %v", path, err)
		return
	}
	callback(string(data), c.parser)
}
