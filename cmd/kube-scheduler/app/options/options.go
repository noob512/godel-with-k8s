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

package options

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	apiserveroptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	cliflag "k8s.io/component-base/cli/flag"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/config/options"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics"
	schedulerappconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
)

// Options has all the params needed to run a Scheduler
type Options struct {
	// The default values. These are overridden if ConfigFile is set or by values in InsecureServing.
	ComponentConfig *kubeschedulerconfig.KubeSchedulerConfiguration

	SecureServing           *apiserveroptions.SecureServingOptionsWithLoopback
	CombinedInsecureServing *CombinedInsecureServingOptions
	Authentication          *apiserveroptions.DelegatingAuthenticationOptions
	Authorization           *apiserveroptions.DelegatingAuthorizationOptions
	Metrics                 *metrics.Options
	Logs                    *logs.Options
	Deprecated              *DeprecatedOptions
	LeaderElection          *componentbaseconfig.LeaderElectionConfiguration

	// ConfigFile is the location of the scheduler server's configuration file.
	ConfigFile string

	// WriteConfigTo is the path where the default configuration will be written.
	WriteConfigTo string

	Master string

	// Flags hold the parsed CLI flags.
	Flags *cliflag.NamedFlagSets
}

// NewOptions 返回默认的调度器应用程序选项。
// 该函数初始化并返回一个包含默认配置的 Options 结构体指针，
// 用于配置 Kubernetes 调度器的各个方面，如安全服务、认证、授权、领导者选举等。
func NewOptions() *Options {
	o := &Options{
		// SecureServing: 配置安全（HTTPS）服务选项，并设置为使用本地回环地址。
		SecureServing: apiserveroptions.NewSecureServingOptions().WithLoopback(),
		// CombinedInsecureServing: 配置不安全（HTTP）服务选项，包括健康检查和指标端点，
		// 并设置为使用本地回环地址。
		CombinedInsecureServing: &CombinedInsecureServingOptions{
			Healthz: (&apiserveroptions.DeprecatedInsecureServingOptions{
				BindNetwork: "tcp", // 指定绑定网络协议为 TCP。
			}).WithLoopback(), // 设置健康检查端点使用本地回环地址。
			Metrics: (&apiserveroptions.DeprecatedInsecureServingOptions{
				BindNetwork: "tcp"}).WithLoopback(), // 设置指标端点使用本地回环地址。
		},
		// Authentication: 配置调度器的认证选项，用于验证请求来源。
		Authentication: apiserveroptions.NewDelegatingAuthenticationOptions(),
		// Authorization: 配置调度器的授权选项，用于确定请求是否有权限执行操作。
		Authorization: apiserveroptions.NewDelegatingAuthorizationOptions(),
		// Deprecated: 包含一些已弃用的选项，用于向后兼容或特定的旧策略配置。
		Deprecated: &DeprecatedOptions{
			UseLegacyPolicyConfig:    false,                  // 不使用旧版策略配置。
			PolicyConfigMapNamespace: metav1.NamespaceSystem, // 指定策略 ConfigMap 所在的命名空间为系统命名空间。
		},
		// LeaderElection: 配置领导者选举选项，确保在多副本部署中只有一个调度器实例处于活动状态。
		LeaderElection: &componentbaseconfig.LeaderElectionConfiguration{
			LeaderElect:       true,                                        // 启用领导者选举。
			LeaseDuration:     metav1.Duration{Duration: 15 * time.Second}, // 租约持续时间，即领导者认为自己有效的时长。
			RenewDeadline:     metav1.Duration{Duration: 10 * time.Second}, // 续约截止时间，领导者尝试续约的截止时间。
			RetryPeriod:       metav1.Duration{Duration: 2 * time.Second},  // 重试周期，尝试获取或续租的间隔。
			ResourceLock:      "leases",                                    // 用于领导者选举的资源锁类型，这里使用 "leases"。
			ResourceName:      "kube-scheduler",                            // 用于领导者选举的资源名称。
			ResourceNamespace: "kube-system",                               // 用于领导者选举的资源命名空间。
		},
		// Metrics: 配置调度器的指标收集选项。
		Metrics: metrics.NewOptions(),
		// Logs: 配置调度器的日志记录选项。
		Logs: logs.NewOptions(),
	}

	// 配置认证选项，容忍集群内查找失败，允许远程 KubeConfig 文件可选。
	o.Authentication.TolerateInClusterLookupFailure = true // 在集群内查找认证信息失败时容忍，不直接报错。
	o.Authentication.RemoteKubeConfigFileOptional = true   // 远程 KubeConfig 文件是可选的，即使未提供也能继续运行。
	// 配置授权选项，允许远程 KubeConfig 文件可选。
	o.Authorization.RemoteKubeConfigFileOptional = true // 远程 KubeConfig 文件是可选的。

	// 设置安全服务的证书选项，使用内存中生成的证书。
	// Set the PairName but leave certificate directory blank to generate in-memory by default
	o.SecureServing.ServerCert.CertDirectory = ""          // 证书目录为空，表示使用内存中的证书。
	o.SecureServing.ServerCert.PairName = "kube-scheduler" // 证书对的名称。
	// 设置安全服务的绑定端口为默认的调度器端口。
	o.SecureServing.BindPort = kubeschedulerconfig.DefaultKubeSchedulerPort

	// 初始化命令行标志。
	o.initFlags()

	// 返回配置好的选项结构体。
	return o
}

// ApplyDeprecated obtains the deprecated CLI args and set them to `o.ComponentConfig` if specified.
func (o *Options) ApplyDeprecated() {
	if o.Flags == nil {
		return
	}

	// Obtain deprecated CLI args. Set them to cfg if specified in command line.
	deprecated := o.Flags.FlagSet("deprecated")
	if deprecated.Changed("profiling") {
		o.ComponentConfig.EnableProfiling = o.Deprecated.EnableProfiling
	}
	if deprecated.Changed("contention-profiling") {
		o.ComponentConfig.EnableContentionProfiling = o.Deprecated.EnableContentionProfiling
	}
	if deprecated.Changed("kubeconfig") {
		o.ComponentConfig.ClientConnection.Kubeconfig = o.Deprecated.Kubeconfig
	}
	if deprecated.Changed("kube-api-content-type") {
		o.ComponentConfig.ClientConnection.ContentType = o.Deprecated.ContentType
	}
	if deprecated.Changed("kube-api-qps") {
		o.ComponentConfig.ClientConnection.QPS = o.Deprecated.QPS
	}
	if deprecated.Changed("kube-api-burst") {
		o.ComponentConfig.ClientConnection.Burst = o.Deprecated.Burst
	}
	if deprecated.Changed("lock-object-namespace") {
		o.ComponentConfig.LeaderElection.ResourceNamespace = o.Deprecated.ResourceNamespace
	}
	if deprecated.Changed("lock-object-name") {
		o.ComponentConfig.LeaderElection.ResourceName = o.Deprecated.ResourceName
	}
}

// ApplyLeaderElectionTo obtains the CLI args related with leaderelection, and override the values in `cfg`.
// Then the `cfg` object is injected into the `options` object.
func (o *Options) ApplyLeaderElectionTo(cfg *kubeschedulerconfig.KubeSchedulerConfiguration) {
	if o.Flags == nil {
		return
	}
	// Obtain CLI args related with leaderelection. Set them to `cfg` if specified in command line.
	leaderelection := o.Flags.FlagSet("leader election")
	if leaderelection.Changed("leader-elect") {
		cfg.LeaderElection.LeaderElect = o.LeaderElection.LeaderElect
	}
	if leaderelection.Changed("leader-elect-lease-duration") {
		cfg.LeaderElection.LeaseDuration = o.LeaderElection.LeaseDuration
	}
	if leaderelection.Changed("leader-elect-renew-deadline") {
		cfg.LeaderElection.RenewDeadline = o.LeaderElection.RenewDeadline
	}
	if leaderelection.Changed("leader-elect-retry-period") {
		cfg.LeaderElection.RetryPeriod = o.LeaderElection.RetryPeriod
	}
	if leaderelection.Changed("leader-elect-resource-lock") {
		cfg.LeaderElection.ResourceLock = o.LeaderElection.ResourceLock
	}
	if leaderelection.Changed("leader-elect-resource-name") {
		cfg.LeaderElection.ResourceName = o.LeaderElection.ResourceName
	}
	if leaderelection.Changed("leader-elect-resource-namespace") {
		cfg.LeaderElection.ResourceNamespace = o.LeaderElection.ResourceNamespace
	}

	o.ComponentConfig = cfg
}

func splitHostIntPort(s string) (string, int, error) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", 0, err
	}
	portInt, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, err
	}
	return host, portInt, err
}

// initFlags initializes flags by section name.
func (o *Options) initFlags() {
	if o.Flags != nil {
		return
	}

	nfs := cliflag.NamedFlagSets{}
	fs := nfs.FlagSet("misc")
	fs.StringVar(&o.ConfigFile, "config", o.ConfigFile, `The path to the configuration file. The following flags can overwrite fields in this file:
  --policy-config-file
  --policy-configmap
  --policy-configmap-namespace`)
	fs.StringVar(&o.WriteConfigTo, "write-config-to", o.WriteConfigTo, "If set, write the configuration values to this file and exit.")
	fs.StringVar(&o.Master, "master", o.Master, "The address of the Kubernetes API server (overrides any value in kubeconfig)")

	o.SecureServing.AddFlags(nfs.FlagSet("secure serving"))
	o.CombinedInsecureServing.AddFlags(nfs.FlagSet("insecure serving"))
	o.Authentication.AddFlags(nfs.FlagSet("authentication"))
	o.Authorization.AddFlags(nfs.FlagSet("authorization"))
	o.Deprecated.AddFlags(nfs.FlagSet("deprecated"))
	options.BindLeaderElectionFlags(o.LeaderElection, nfs.FlagSet("leader election"))
	utilfeature.DefaultMutableFeatureGate.AddFlag(nfs.FlagSet("feature gate"))
	o.Metrics.AddFlags(nfs.FlagSet("metrics"))
	o.Logs.AddFlags(nfs.FlagSet("logs"))

	o.Flags = &nfs
}

// ApplyTo applies the scheduler options to the given scheduler app configuration.
func (o *Options) ApplyTo(c *schedulerappconfig.Config) error {
	if len(o.ConfigFile) == 0 {
		// If the --config arg is not specified, honor the deprecated as well as leader election CLI args.
		o.ApplyDeprecated()
		o.ApplyLeaderElectionTo(o.ComponentConfig)
		c.ComponentConfig = *o.ComponentConfig

		o.Deprecated.ApplyTo(c)

		if err := o.CombinedInsecureServing.ApplyTo(c, &c.ComponentConfig); err != nil {
			return err
		}
	} else {
		cfg, err := loadConfigFromFile(o.ConfigFile)
		if err != nil {
			return err
		}
		// If the --config arg is specified, honor the leader election CLI args only.
		o.ApplyLeaderElectionTo(cfg)

		if err := validation.ValidateKubeSchedulerConfiguration(cfg); err != nil {
			return err
		}

		c.ComponentConfig = *cfg

		// apply any deprecated Policy flags, if applicable
		o.Deprecated.ApplyTo(c)

		// use the loaded config file only, with the exception of --address and --port.
		if err := o.CombinedInsecureServing.ApplyToFromLoadedConfig(c, &c.ComponentConfig); err != nil {
			return err
		}
	}

	// If the user is using the legacy policy config, clear the profiles, they will be set
	// on scheduler instantiation based on the configurations in the policy file.
	if c.LegacyPolicySource != nil {
		c.ComponentConfig.Profiles = nil
	}

	if err := o.SecureServing.ApplyTo(&c.SecureServing, &c.LoopbackClientConfig); err != nil {
		return err
	}
	if o.SecureServing != nil && (o.SecureServing.BindPort != 0 || o.SecureServing.Listener != nil) {
		if err := o.Authentication.ApplyTo(&c.Authentication, c.SecureServing, nil); err != nil {
			return err
		}
		if err := o.Authorization.ApplyTo(&c.Authorization); err != nil {
			return err
		}
	}
	o.Metrics.Apply()
	o.Logs.Apply()
	return nil
}

// Validate validates all the required options.
func (o *Options) Validate() []error {
	var errs []error

	if err := validation.ValidateKubeSchedulerConfiguration(o.ComponentConfig); err != nil {
		errs = append(errs, err.Errors()...)
	}
	errs = append(errs, o.SecureServing.Validate()...)
	errs = append(errs, o.CombinedInsecureServing.Validate()...)
	errs = append(errs, o.Authentication.Validate()...)
	errs = append(errs, o.Authorization.Validate()...)
	errs = append(errs, o.Deprecated.Validate()...)
	errs = append(errs, o.Metrics.Validate()...)
	errs = append(errs, o.Logs.Validate()...)

	return errs
}

// Config return a scheduler config object
func (o *Options) Config() (*schedulerappconfig.Config, error) {
	if o.SecureServing != nil {
		if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
			return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
		}
	}

	c := &schedulerappconfig.Config{}
	if err := o.ApplyTo(c); err != nil {
		return nil, err
	}

	// Prepare kube config.
	kubeConfig, err := createKubeConfig(c.ComponentConfig.ClientConnection, o.Master)
	if err != nil {
		return nil, err
	}

	// Prepare kube clients.
	client, eventClient, err := createClients(kubeConfig)
	if err != nil {
		return nil, err
	}

	c.EventBroadcaster = events.NewEventBroadcasterAdapter(eventClient)

	// Set up leader election if enabled.
	var leaderElectionConfig *leaderelection.LeaderElectionConfig
	if c.ComponentConfig.LeaderElection.LeaderElect {
		// Use the scheduler name in the first profile to record leader election.
		schedulerName := corev1.DefaultSchedulerName
		if len(c.ComponentConfig.Profiles) != 0 {
			schedulerName = c.ComponentConfig.Profiles[0].SchedulerName
		}
		coreRecorder := c.EventBroadcaster.DeprecatedNewLegacyRecorder(schedulerName)
		leaderElectionConfig, err = makeLeaderElectionConfig(c.ComponentConfig.LeaderElection, kubeConfig, coreRecorder)
		if err != nil {
			return nil, err
		}
	}

	c.Client = client
	c.KubeConfig = kubeConfig
	c.InformerFactory = scheduler.NewInformerFactory(client, 0)
	c.LeaderElection = leaderElectionConfig

	return c, nil
}

// makeLeaderElectionConfig builds a leader election configuration. It will
// create a new resource lock associated with the configuration.
func makeLeaderElectionConfig(config componentbaseconfig.LeaderElectionConfiguration, kubeConfig *restclient.Config, recorder record.EventRecorder) (*leaderelection.LeaderElectionConfig, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("unable to get hostname: %v", err)
	}
	// add a uniquifier so that two processes on the same host don't accidentally both become active
	id := hostname + "_" + string(uuid.NewUUID())

	rl, err := resourcelock.NewFromKubeconfig(config.ResourceLock,
		config.ResourceNamespace,
		config.ResourceName,
		resourcelock.ResourceLockConfig{
			Identity:      id,
			EventRecorder: recorder,
		},
		kubeConfig,
		config.RenewDeadline.Duration)
	if err != nil {
		return nil, fmt.Errorf("couldn't create resource lock: %v", err)
	}

	return &leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   config.LeaseDuration.Duration,
		RenewDeadline:   config.RenewDeadline.Duration,
		RetryPeriod:     config.RetryPeriod.Duration,
		WatchDog:        leaderelection.NewLeaderHealthzAdaptor(time.Second * 20),
		Name:            "kube-scheduler",
		ReleaseOnCancel: true,
	}, nil
}

// createKubeConfig creates a kubeConfig from the given config and masterOverride.
// TODO remove masterOverride when CLI flags are removed.
func createKubeConfig(config componentbaseconfig.ClientConnectionConfiguration, masterOverride string) (*restclient.Config, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(masterOverride, config.Kubeconfig)
	if err != nil {
		return nil, err
	}

	kubeConfig.DisableCompression = true
	kubeConfig.AcceptContentTypes = config.AcceptContentTypes
	kubeConfig.ContentType = config.ContentType
	kubeConfig.QPS = config.QPS
	kubeConfig.Burst = int(config.Burst)

	return kubeConfig, nil
}

// createClients creates a kube client and an event client from the given kubeConfig
func createClients(kubeConfig *restclient.Config) (clientset.Interface, clientset.Interface, error) {
	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "scheduler"))
	if err != nil {
		return nil, nil, err
	}

	eventClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, nil, err
	}

	return client, eventClient, nil
}
