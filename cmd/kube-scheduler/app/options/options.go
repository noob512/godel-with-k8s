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
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"
	"math"
	"net"
	"os"
	"strconv"
	"time"
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	godelclientscheme "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned/scheme"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	//-----------------------------------------------------------------------------------
	godelschedulerconfig "github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
	Godelcomponentbaseconfig "k8s.io/component-base/config/v1alpha1"
	//----------------------------------------------------------------------------------
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
// Options 定义了调度器命令行工具 (scheduler command) 的所有可配置选项。
// 这些选项可以通过命令行标志 (CLI flags)、配置文件 (ConfigFile) 或其默认值来设置。
type Options struct {
	// ComponentConfig 存储了 Godel 调度器的核心配置，其默认值可以被 ConfigFile 或 InsecureServing 中的值覆盖。
	GodelComponentConfig godelschedulerconfig.GodelSchedulerConfiguration
	// SchedulerRenewIntervalSeconds 定义调度器在领导者选举机制中的租约续期间隔（以秒为单位）。
	// 这与 LeaderElectionConfiguration 中的 RetryPeriod 相关。
	SchedulerRenewIntervalSeconds int64
	// 以下字段是为了向后兼容而保留的，计划在未来版本中移除。
	// UnitMaxBackoffSeconds 指定调度器在处理失败时的最大退避时间（秒）。
	UnitMaxBackoffSeconds int64
	// UnitInitialBackoffSeconds 指定调度器在处理失败时的初始退避时间（秒）。
	UnitInitialBackoffSeconds int64
	// DisablePreemption 控制是否禁用调度器的抢占（Preemption）功能。
	// 如果为 true，则调度器不会尝试驱逐低优先级 Pod 来为高优先级 Pod 腾出空间。
	DisablePreemption bool
	// AttemptImpactFactorOnPriority 是一个影响优先级计算的因子，用于在调度决策中考虑潜在的抢占影响。
	AttemptImpactFactorOnPriority float64

	//---------------------------------------------------------
	// ComponentConfig 是调度器的主要配置对象。
	// 它包含了调度器的核心行为设置，如调度策略、插件配置等。
	// 其默认值可以通过 InsecureServing 中的选项或直接通过 ConfigFile 指定的配置文件来覆盖。
	ComponentConfig *kubeschedulerconfig.KubeSchedulerConfiguration

	// SecureServing 定义了调度器安全端点（HTTPS）的配置选项。
	// 包括证书、密钥、监听地址和端口等。
	SecureServing *apiserveroptions.SecureServingOptionsWithLoopback

	// CombinedInsecureServing 定义了调度器非安全端点（HTTP）的配置选项。
	// 通常用于健康检查和指标暴露。
	CombinedInsecureServing *CombinedInsecureServingOptions

	// Authentication 定义了调度器的认证配置。
	// 指定如何对请求进行身份验证（例如，使用客户端证书、Bearer Token 等）。
	Authentication *apiserveroptions.DelegatingAuthenticationOptions

	// Authorization 定义了调度器的授权配置。
	// 指定如何对已认证的请求进行权限检查（例如，使用 RBAC）。
	Authorization *apiserveroptions.DelegatingAuthorizationOptions

	// Metrics 用于配置调度器暴露的指标 (metrics) 相关选项。
	Metrics *metrics.Options

	// Logs 用于配置调度器的日志记录相关选项。
	Logs *logs.Options

	// Deprecated 用于存储已弃用的选项，以保持向后兼容性。
	Deprecated *DeprecatedOptions

	// LeaderElection 定义了调度器进行领导者选举的配置。
	// 确保在高可用（HA）集群中只有一个调度器实例处于活动状态。
	LeaderElection *componentbaseconfig.LeaderElectionConfiguration

	// ConfigFile 指定调度器服务器配置文件的路径。
	// 如果设置了此字段，将加载该文件中的配置，并可能覆盖默认值。
	ConfigFile string

	// WriteConfigTo 指定一个路径，用于将默认的调度器配置写入到该路径下的文件。
	// 通常用于生成初始配置模板。
	WriteConfigTo string

	// Master 指定 Kubernetes API 服务器的地址（URL）。
	// 调度器将与该地址通信以获取集群状态和调度 Pod。
	Master string

	// Flags 存储了解析后的命令行标志 (CLI flags)。
	// 这些标志最终会被映射到 Options 结构体的各个字段中。
	Flags *cliflag.NamedFlagSets
}

// ------------------------------------------------------------------------------------------------
const (
	// DefaultUnitInitialBackoffInSeconds is the default value for the initial backoff duration
	// for unschedulable units. To change the default podInitialBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultUnitInitialBackoffInSeconds = 10
	// DefaultUnitMaxBackoffInSeconds is the default value for the max backoff duration
	// for unschedulable units. To change the default unitMaxBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultUnitMaxBackoffInSeconds = 300
	// DefaultDisablePreemption is the default value for the option to disable preemption ability
	// for unschedulable pods.
	DefaultDisablePreemption        = true
	CandidateSelectPolicyBest       = "Best"
	CandidateSelectPolicyBetter     = "Better"
	CandidateSelectPolicyRandom     = "Random"
	BetterPreemptionPolicyAscending = "Ascending"
	BetterPreemptionPolicyDichotomy = "Dichotomy"
	// DefaultBlockQueue is the default value for the option to use block queue for SchedulingQueue.
	DefaultBlockQueue = false
	// DefaultPodUpgradePriorityInMinutes is the default upgrade priority duration for godel sort.
	DefaultPodUpgradePriorityInMinutes = 5
	// DefaultGodelSchedulerName defines the name of default scheduler.
	DefaultGodelSchedulerName = "my-cus-godel-scheduler"
	// DefaultRenewIntervalInSeconds is the default value for the renew interval duration for scheduler.
	DefaultRenewIntervalInSeconds = 30

	// DefaultSchedulerName is default high level scheduler name
	DefaultSchedulerName = "godel-scheduler"

	// DefaultClientConnectionQPS is default scheduler qps
	DefaultClientConnectionQPS = 10000.0
	// DefaultClientConnectionBurst is default scheduler burst
	DefaultClientConnectionBurst = 10000

	// DefaultIDC is default idc name for godel scheduler
	DefaultIDC = "lq"
	// DefaultCluster is default cluster name for godel scheduler
	DefaultCluster = "default"
	// DefaultTracer is default tracer name for godel scheduler
	DefaultTracer = string(tracing.NoopConfig)

	DefaultSubClusterKey = ""

	// DefaultAttemptImpactFactorOnPriority is the default attempt factors used by godel sort
	DefaultAttemptImpactFactorOnPriority = 10.0

	DefaultMaxWaitingDeletionDuration = 120

	DefaultReservationTimeOutSeconds = 60
)

const (
	// DefaultPercentageOfNodesToScore defines the percentage of nodes of all nodes
	// that once found feasible, the scheduler stops looking for more nodes.
	// A value of 0 means adaptive, meaning the scheduler figures out a proper default.
	DefaultPercentageOfNodesToScore = 0

	DefaultIncreasedPercentageOfNodesToScore = 0

	// MaxCustomPriorityScore is the max score UtilizationShapePoint expects.
	MaxCustomPriorityScore int64 = 10

	// MaxTotalScore is the maximum total score.
	MaxTotalScore int64 = math.MaxInt64

	// MaxWeight defines the max weight value allowed for custom PriorityPolicy
	MaxWeight = MaxTotalScore / MaxCustomPriorityScore
)

func newDefaultComponentConfig() (*godelschedulerconfig.GodelSchedulerConfiguration, error) {
	cfg := godelschedulerconfig.GodelSchedulerConfiguration{}

	SetDefaults_GodelSchedulerConfiguration(&cfg)
	return &cfg, nil
}

// SetDefaults_GodelSchedulerConfiguration 为 Godel 调度器配置结构体设置合理的默认值。
// 该函数确保即使用户未显式配置某些字段，调度器也能以安全、合理的默认行为运行。
func SetDefaults_GodelSchedulerConfiguration(obj *godelschedulerconfig.GodelSchedulerConfiguration) {
	klog.Info("使用内部版本的配置")

	// 2. 客户端连接（ClientConnection）与绑定地址（Healthz/Metrics）配置
	{
		// 默认使用 Protobuf 格式与 Kubernetes API Server 通信，以提升性能
		if len(obj.ClientConnection.ContentType) == 0 {
			obj.ClientConnection.ContentType = "application/vnd.kubernetes.protobuf"
		}

		// 调度器对 QPS 和 Burst 有特定需求，设置专属默认值（高于通用客户端）
		if obj.ClientConnection.QPS == 0.0 {
			obj.ClientConnection.QPS = DefaultClientConnectionQPS
		}
		if obj.ClientConnection.Burst == 0 {
			obj.ClientConnection.Burst = DefaultClientConnectionBurst
		}
	}

	// 3. 调试相关配置（Profiling）
	{
		// 默认启用性能分析（pprof）
		if obj.EnableProfiling == nil {
			enableProfiling := true
			obj.EnableProfiling = &enableProfiling
		}

		// 若启用了性能分析，则默认也启用竞争分析（contention profiling）
		if *obj.EnableProfiling && obj.EnableContentionProfiling == nil {
			enableContentionProfiling := true
			obj.EnableContentionProfiling = &enableContentionProfiling
		}
	}

	// 4. Godel 调度器核心配置
	{
		// 若未设置 Godel 调度器名称，使用默认名称
		if len(obj.GodelSchedulerName) == 0 {
			obj.GodelSchedulerName = "my-cus-k8s"+DefaultGodelSchedulerName //这个可以自定义
		}

		// 设置 Kubernetes 侧使用的调度器名称（用于 Pod.Spec.SchedulerName）
		if obj.SchedulerName == nil {
			defaultValue := DefaultSchedulerName
			//这个必需与Kubernetes 中调度器的标识名（对应 Pod.spec.schedulerName）一致
			obj.SchedulerName = &defaultValue
		}

		// 若未配置追踪器（Tracer），使用无操作（No-op）默认选项
		if obj.Tracer == nil {
			obj.Tracer = tracing.DefaultNoopOptions()
		}

		// 设置子集群标识的标签键（用于多集群调度）
		if obj.SubClusterKey == nil {
			defaultValue := DefaultSubClusterKey
			obj.SubClusterKey = &defaultValue
		}

		// 设置资源预留（Reservation）的超时时间（秒），若未配置或非法则使用默认值
		if obj.ReservationTimeOutSeconds <= 0 {
			obj.ReservationTimeOutSeconds = DefaultReservationTimeOutSeconds
		}
	}

	// 5. Godel 调度器默认 Profile（调度策略配置）
	{
		// 若未设置默认 Profile，创建一个空的
		if obj.DefaultProfile == nil {
			obj.DefaultProfile = &godelschedulerconfig.GodelSchedulerProfile{}
		}

		// 设置默认调度时要评分的节点百分比（用于性能优化）
		if obj.DefaultProfile.PercentageOfNodesToScore == nil {
			percentageOfNodesToScore := int32(DefaultPercentageOfNodesToScore)
			obj.DefaultProfile.PercentageOfNodesToScore = &percentageOfNodesToScore
		}

		// 设置负载较高时增加的节点评分百分比
		if obj.DefaultProfile.IncreasedPercentageOfNodesToScore == nil {
			increasedPercentageOfNodesToScore := int32(DefaultIncreasedPercentageOfNodesToScore)
			obj.DefaultProfile.IncreasedPercentageOfNodesToScore = &increasedPercentageOfNodesToScore
		}

		// 设置调度单元（如 Pod）重试的初始退避时间（秒）
		if obj.DefaultProfile.UnitInitialBackoffSeconds == nil {
			defaultUnitInitialBackoffInSeconds := int64(DefaultUnitInitialBackoffInSeconds)
			obj.DefaultProfile.UnitInitialBackoffSeconds = &defaultUnitInitialBackoffInSeconds
		}

		// 设置调度单元重试的最大退避时间（秒）
		if obj.DefaultProfile.UnitMaxBackoffSeconds == nil {
			defaultUnitMaxBackoffInSeconds := int64(DefaultUnitMaxBackoffInSeconds)
			obj.DefaultProfile.UnitMaxBackoffSeconds = &defaultUnitMaxBackoffInSeconds
		}

		// 设置重试次数对调度优先级的影响因子
		if obj.DefaultProfile.AttemptImpactFactorOnPriority == nil {
			attemptImpactFactorOnPriority := DefaultAttemptImpactFactorOnPriority
			obj.DefaultProfile.AttemptImpactFactorOnPriority = &attemptImpactFactorOnPriority
		}

		// 默认不禁用抢占（Preemption）
		if obj.DefaultProfile.DisablePreemption == nil {
			value := true
			obj.DefaultProfile.DisablePreemption = &value
		}

		// 默认不禁用阻塞队列（BlockQueue）功能
		if obj.DefaultProfile.DisablePreemption == nil {
			value := false
			obj.DefaultProfile.DisablePreemption = &value
		}

		// 设置等待删除 Pod 的最大容忍时长（用于资源回收）
		if obj.DefaultProfile.MaxWaitingDeletionDuration == 0 {
			obj.DefaultProfile.MaxWaitingDeletionDuration = DefaultMaxWaitingDeletionDuration
		}

		// 设置候选节点选择策略（如随机选择）
		if obj.DefaultProfile.CandidatesSelectPolicy == nil {
			value := CandidateSelectPolicyRandom
			obj.DefaultProfile.CandidatesSelectPolicy = &value
		}

		// 设置更优节点选择策略列表（用于抢占决策，如升序、二分查找等）
		if obj.DefaultProfile.BetterSelectPolicies == nil {
			obj.DefaultProfile.BetterSelectPolicies = &godelschedulerconfig.StringSlice{
				BetterPreemptionPolicyAscending,
				BetterPreemptionPolicyDichotomy,
			}
		}
	}
}

// createClients creates a kube client and an event client from the given config and masterOverride.
// TODO remove masterOverride when CLI flags are removed.
// createNewClients 根据提供的配置信息创建多个 Kubernetes API 客户端实例。
// 它会创建用于常规调度操作的客户端、用于领导者选举的客户端、用于事件记录的客户端以及用于 Godel 自定义资源 (CRD) 的客户端。
// 参数:
// - config: 包含连接 API 服务器所需的基本配置，如 kubeconfig 文件路径、QPS、Burst 等。
// - masterOverride: 可选的 API 服务器地址，如果提供，将覆盖 kubeconfig 文件中的地址。
// - timeout: 用于领导者选举客户端的请求超时时间。
// 返回值:
// - clientset.Interface: 用于常规调度操作的标准 Kubernetes 资源客户端。
// - clientset.Interface: 用于领导者选举的 Kubernetes 资源客户端（具有特定的超时设置）。
// - clientset.Interface: 用于事件记录的 Kubernetes 资源客户端（共享相同的底层配置）。
// - godelclient.Interface: 用于操作 Godel 自定义资源 (CRD) 的客户端。
// - error: 如果在创建任何客户端过程中发生错误，则返回该错误。
func createNewClients(config Godelcomponentbaseconfig.ClientConnectionConfiguration, masterOverride string, timeout time.Duration) (clientset.Interface, clientset.Interface, clientset.Interface, godelclient.Interface, error) {
	// 检查是否既没有提供 kubeconfig 文件路径，也没有提供 master 地址。
	// 这种情况下，客户端将尝试使用默认的 InCluster 配置，这在某些环境中可能不适用。
	if len(config.Kubeconfig) == 0 && len(masterOverride) == 0 {
		klog.InfoS("WARN: Neither --kubeconfig nor --master was specified. Using default API client. This might not work")
	}

	// 使用 clientcmd 包加载 kubeconfig 文件（如果指定了路径），
	// 并根据 masterOverride 参数（如果非空）覆盖其中的服务器地址，生成一个 *rest.Config。
	// 这是创建所有客户端的基础配置。
	kubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: config.Kubeconfig}, // 指定 kubeconfig 文件路径
		&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterOverride}}, // 覆盖服务器地址
	).ClientConfig()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 根据基础配置设置一些特定参数。
	kubeConfig.DisableCompression = true // 禁用响应压缩，可能对小请求有性能优势。
	kubeConfig.AcceptContentTypes = config.AcceptContentTypes // 设置客户端接受的内容类型。
	kubeConfig.ContentType = config.ContentType               // 设置请求的默认内容类型。
	kubeConfig.QPS = config.QPS                               // 设置每秒查询率限制。
	kubeConfig.Burst = int(config.Burst)                      // 设置突发请求量限制。

	// 使用配置创建第一个客户端，用于常规的调度操作。
	// 为这个客户端添加 "scheduler" 用户代理标识。
	client, err := clientset.NewForConfig(restclient.AddUserAgent(kubeConfig, "scheduler"))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 创建一个基础配置的浅拷贝，用于领导者选举客户端。
	// 浅拷贝意味着指针字段仍然指向相同的对象，但我们可以修改非指针字段。
	restConfig := *kubeConfig
	// 为领导者选举客户端设置特定的请求超时时间。
	restConfig.Timeout = timeout
	// 使用这个带有超时设置的配置创建领导者选举客户端，并添加 "leader-election" 用户代理标识。
	leaderElectionClient, err := clientset.NewForConfig(restclient.AddUserAgent(&restConfig, "leader-election"))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 将 Godel CRD 的类型注册到标准的 Kubernetes 客户端 Scheme 中。
	// 这样标准的 clientset 也能序列化/反序列化 Godel 的 CRD 对象（如果需要的话）。
	// utilruntime.Must 确保 AddToScheme 操作成功，失败则 panic。
	utilruntime.Must(godelclientscheme.AddToScheme(clientsetscheme.Scheme))
	// 使用基础配置创建事件客户端。通常事件记录不需要特殊的超时或用户代理（复用基础配置的用户代理或无）。
	eventClient, err := clientset.NewForConfig(kubeConfig)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 为 Godel CRD 客户端创建一个独立的 *rest.Config。
	// 注意：这里再次加载了 kubeconfig 和 masterOverride，这可能与上面的 kubeConfig 是相同的。
	// 如果这些配置是完全一样的，复用 kubeConfig 并仅修改必要的部分（如 UserAgent）可能更高效。
	crdKubeConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: config.Kubeconfig},
		&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterOverride}}).ClientConfig()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 为 Godel CRD 客户端配置基础参数。
	crdKubeConfig.DisableCompression = true
	crdKubeConfig.QPS = config.QPS
	crdKubeConfig.Burst = int(config.Burst)

	// 使用配置创建 Godel CRD 客户端，并添加 "scheduler" 用户代理标识。
	godelCrdClient, err := godelclient.NewForConfig(restclient.AddUserAgent(crdKubeConfig, "scheduler"))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// 所有客户端创建成功，返回它们和 nil 错误。
	return client, leaderElectionClient, eventClient, godelCrdClient, nil
}

// --------------------------------------------------------------------------------------------------------------------
// NewOptions 返回默认的调度器应用程序选项。
// 该函数初始化并返回一个包含默认配置的 Options 结构体指针，
// 用于配置 Kubernetes 调度器的各个方面，如安全服务、认证、授权、领导者选举等。
func NewOptions() *Options {
	cfg, err := newDefaultComponentConfig()
	if err != nil {
		return nil
	}
	//-------------------------------------------------------
	o := &Options{
		//----------------------------------
		GodelComponentConfig:          *cfg,
		SchedulerRenewIntervalSeconds: godelschedulerconfig.DefaultRenewIntervalInSeconds,
		//----------------------------------
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
// ApplyTo 方法将当前 Options 对象中的配置（命令行标志、配置文件路径等）应用到传入的 schedulerappconfig.Config 对象 c 上。
// 它处理了两种情况：从命令行标志配置和从配置文件加载配置，并确保相关设置（如认证、授权、指标、日志等）被正确应用。
func (o *Options) ApplyTo(c *schedulerappconfig.Config) error {
	// 检查是否指定了配置文件路径。
	if len(o.ConfigFile) == 0 {
		klog.Info("选择len(o.ConfigFile) == 0")
		//这是会选择的路径
		// 情况一：未指定配置文件 (--config 未设置)。
		// 1. 应用已弃用的命令行标志 (ApplyDeprecated)。
		o.ApplyDeprecated()
		// 2. 将命令行中关于领导者选举的标志应用到 ComponentConfig 中。
		o.ApplyLeaderElectionTo(o.ComponentConfig)
		// 3. 将 Options 中的 ComponentConfig (已根据命令行标志更新) 赋值给最终的 Config 对象。
		c.ComponentConfig = *o.ComponentConfig
		c.GodelComponentConfig = o.GodelComponentConfig

		o.Deprecated.ApplyTo(c)

		// 5. 将非安全服务 (HTTP) 的选项（如地址、端口）应用到 Config 对象。
		if err := o.CombinedInsecureServing.ApplyTo(c, &c.ComponentConfig); err != nil {
			return err
		}
	} else {
		klog.Info("选择len(o.ConfigFile) 不为0时的路径")
		// 情况二：指定了配置文件 (--config 已设置)。
		// 1. 从指定的文件路径加载调度器配置对象。
		cfg, err := loadConfigFromFile(o.ConfigFile)
		if err != nil {
			return err
		}
		// 2. 即使使用了配置文件，仍然将命令行中关于领导者选举的标志应用到加载的配置对象上，以允许命令行覆盖。
		o.ApplyLeaderElectionTo(cfg)

		// 3. 验证加载的配置对象是否符合规范。
		if err := validation.ValidateKubeSchedulerConfiguration(cfg); err != nil {
			return err
		}

		// 4. 将验证后的配置对象赋值给最终的 Config 对象。
		c.ComponentConfig = *cfg

		// 5. 如果配置文件中包含了旧版策略，同样应用已弃用的选项。
		o.Deprecated.ApplyTo(c)

		// 6. 从加载的配置应用非安全服务设置。这里可能只应用部分特定选项（如 --address 和 --port）。
		if err := o.CombinedInsecureServing.ApplyToFromLoadedConfig(c, &c.ComponentConfig); err != nil {
			return err
		}
	}
	//上述else逻辑不会执行
	// 如果使用了旧版的调度策略配置 (LegacyPolicySource)，则清除 ComponentConfig 中的 Profiles。
	// 因为在调度器实例化时，会根据旧版策略文件重新生成 Profiles。
	if c.LegacyPolicySource != nil {
		c.ComponentConfig.Profiles = nil
	}
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
// Config 方法根据当前的 Options 配置对象，构建并返回一个用于启动调度器的完整配置对象 (schedulerappconfig.Config) 和一个可能的错误。
// 该方法负责初始化安全证书、创建 Kubernetes 客户端、设置事件广播器和领导者选举（如果启用）。
func (o *Options) Config() (*schedulerappconfig.Config, error) {
	// 如果配置了安全服务 (SecureServing)，则尝试为其生成自签名证书。
	// 这通常用于本地测试或开发环境，证书将用于 localhost 和 127.0.0.1。
	if o.SecureServing != nil {
		if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
			return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
		}
	}

	// 创建一个空的调度器应用配置对象。
	c := &schedulerappconfig.Config{}
	// 将当前 Options 中的配置（如命令行标志、认证、授权等）应用到这个配置对象 c 上。
	if err := o.ApplyTo(c); err != nil {
		return nil, err
	}

	// 根据 ComponentConfig 中的 ClientConnection 设置和 Master 地址，创建用于与 Kubernetes API 服务器通信的 KubeConfig。
	kubeConfig, err := createKubeConfig(c.ComponentConfig.ClientConnection, o.Master)
	if err != nil {
		return nil, err
	}

	client, _, eventClient, godelCrdClient, err := createNewClients(
		c.GodelComponentConfig.ClientConnection,
		o.Master,
		c.GodelComponentConfig.LeaderElection.RenewDeadline.Duration,
	)

	// 使用上一步创建的 KubeConfig，生成 Kubernetes API 客户端 (client) 和事件客户端 (eventClient)。
	client, eventClient, err = createClients(kubeConfig)
	if err != nil {
		return nil, err
	}
	//------------------------------------------------------------
	// Godel CRD 相关客户端和 InformerFactory。
	c.GodelCrdClient = godelCrdClient
	c.GodelCrdInformerFactory = crdinformers.NewSharedInformerFactory(c.GodelCrdClient, 0)
	//------------------------------------------------------------

	// 创建一个事件广播器 (EventBroadcaster)，用于记录和发送调度器相关的事件到 Kubernetes API。
	// 它使用事件客户端与 API 服务器交互。
	c.EventBroadcaster = events.NewEventBroadcasterAdapter(eventClient)

	// 将创建好的客户端、KubeConfig、Informer 工厂和领导者选举配置赋值给最终的配置对象 c。
	c.Client = client
	c.KubeConfig = kubeConfig
	// 创建一个 Informer 工厂，用于监听和缓存 Kubernetes API 服务器中的资源对象（如 Pods, Nodes 等）。
	// 参数 0 表示使用默认的 Resync 周期。
	c.InformerFactory = scheduler.NewInformerFactory(client, 0)

	// 所有配置准备就绪，返回最终的配置对象。
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
