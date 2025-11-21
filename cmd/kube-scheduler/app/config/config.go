/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	//-------------------------------------------------------------------------------
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config"

	//-------------------------------------------------------------------------------
	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
)

//---------------------------------------------------------------------------

// Config has all the context to run a Scheduler
// Config 定义了调度器应用程序运行所需的所有核心配置。
// 它整合了调度器的配置对象、API 服务器设置、Kubernetes 客户端、事件广播器和领导者选举配置等。
type Config struct {
	//----------------------------------------------------------------
	// GodelCrdClient 是 Godel 自定义资源（如 Scheduler, PodGroup 等）的客户端接口。
	GodelCrdClient godelclient.Interface
	// GodelCrdInformerFactory 是 Godel 自定义资源的 SharedInformer 工厂，用于监听和缓存 Godel CRD 资源的变化。
	GodelCrdInformerFactory crdinformers.SharedInformerFactory
	GodelComponentConfig    config.GodelSchedulerConfiguration
	//------------------------------------------------------------------
	// ComponentConfig 是调度器服务器的主配置对象。
	// 它包含了调度策略、插件、调度周期等核心调度行为的定义。
	ComponentConfig kubeschedulerconfig.KubeSchedulerConfiguration

	// LegacyPolicySource 指向旧版调度策略的来源，用于向后兼容。
	// 现代调度器通常使用调度配置集 (Profiles)。
	LegacyPolicySource *kubeschedulerconfig.SchedulerPolicySource

	// LoopbackClientConfig 是一个用于特权环回连接的 REST 客户端配置。
	// 这种连接通常用于调度器内部与 API 服务器的通信，可能拥有更高的权限。
	LoopbackClientConfig *restclient.Config

	// InsecureServing 定义了调度器非安全 HTTP 端点的配置信息。
	// 如果为 nil，则禁用不安全端口上的服务。通常用于健康检查。
	InsecureServing *apiserver.DeprecatedInsecureServingInfo

	// InsecureMetricsServing 定义了独立的、非安全 HTTP 端点的配置信息，专门用于暴露指标。
	// 如果为 nil，则指标可能与健康检查或其他服务共享 InsecureServing 端口。
	InsecureMetricsServing *apiserver.DeprecatedInsecureServingInfo

	// Authentication 包含了 API 服务器的认证配置信息。
	Authentication apiserver.AuthenticationInfo

	// Authorization 包含了 API 服务器的授权配置信息。
	Authorization apiserver.AuthorizationInfo

	// SecureServing 定义了调度器安全 HTTPS 端点的配置信息，包括证书和密钥。
	SecureServing *apiserver.SecureServingInfo

	// Client 是一个标准的 Kubernetes API 客户端接口，用于与 API 服务器进行交互。
	Client clientset.Interface

	// KubeConfig 是用于创建 Kubernetes 客户端的原始配置对象。
	KubeConfig *restclient.Config

	// InformerFactory 是一个共享的 Informer 工厂，用于监听和缓存 Kubernetes API 对象（如 Pods, Nodes）。
	// 调度器使用缓存的数据来做出调度决策。
	InformerFactory informers.SharedInformerFactory

	// EventBroadcaster 是一个事件广播器适配器，用于记录和发送调度器相关的事件到 API 服务器。
	//lint:ignore SA1019 这个已弃用的字段目前仍需使用。迁移完成后将被移除。
	EventBroadcaster events.EventBroadcasterAdapter

	// LeaderElection 是领导者选举的配置对象，用于在高可用（HA）场景下确保只有一个调度器实例是活跃的。
	// 此字段是可选的。
	LeaderElection *leaderelection.LeaderElectionConfig
}

type completedConfig struct {
	*Config
}

// CompletedConfig same as Config, just to swap private object.
type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (c *Config) Complete() CompletedConfig {
	cc := completedConfig{c}

	if c.InsecureServing != nil {
		c.InsecureServing.Name = "healthz"
	}
	if c.InsecureMetricsServing != nil {
		c.InsecureMetricsServing.Name = "metrics"
	}

	apiserver.AuthorizeClientBearerToken(c.LoopbackClientConfig, &c.Authentication, &c.Authorization)

	return CompletedConfig{&cc}
}
