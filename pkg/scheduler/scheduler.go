/*
Copyright 2014 The Kubernetes Authors.

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

package scheduler

import (
	"context"
	"fmt"
	"github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/events"

	//--------------------------------------------------------------------------------
	godelclient "github.com/kubewharf/godel-scheduler-api/pkg/client/clientset/versioned"
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	godelframework "github.com/kubewharf/godel-scheduler/pkg/framework/api"
	"github.com/kubewharf/godel-scheduler/pkg/scheduler/apis/config"
	godelcache "github.com/kubewharf/godel-scheduler/pkg/scheduler/cache"
	godelutil "github.com/kubewharf/godel-scheduler/pkg/util"
	"k8s.io/apimachinery/pkg/util/clock"
	//--------------------------------------------------------------------------------
	"io/ioutil"
	corelisters "k8s.io/client-go/listers/core/v1"
	"math/rand"
	"os"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkplugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
	"k8s.io/kubernetes/pkg/scheduler/internal/parallelize"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/profile"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// SchedulerError is the reason recorded for events when an error occurs during scheduling a pod.
	SchedulerError = "SchedulerError"
	// Percentage of plugin metrics to be sampled.
	pluginMetricsSamplePercent = 10
	// Duration the scheduler will wait before expiring an assumed pod.
	// See issue #106361 for more details about this parameter and its value.
	durationToExpireAssumedPod = 2 * time.Minute
)
//-----------------------------------------------------------------
type subClusterConfig struct {
	PercentageOfNodesToScore          int32
	IncreasedPercentageOfNodesToScore int32

	MaxWaitingDeletionDuration int64

	UseBlockQueue                 bool
	UnitInitialBackoffSeconds     int64
	UnitMaxBackoffSeconds         int64
	AttemptImpactFactorOnPriority float64

	BasePlugins             godelframework.PluginCollectionSet
	PluginConfigs           []config.PluginConfig
	PreemptionPluginConfigs []config.PluginConfig
	UnitQueueSortPlugin     *godelframework.PluginSpec

	DisablePreemption      bool
	CandidatesSelectPolicy string
	BetterSelectPolicies   []string

	EnableStore map[string]bool
}

const (
	// maxUpdateRetries is the number of immediate, successive retries the Scheduler will attempt
	// when renewing the Scheduler status before it waits for the renewal interval before trying again,
	// similar to what we do for node status retries
	maxUpdateRetries = 5
	// sleep is the default interval for retry
	sleep = 100 * time.Millisecond
)

// StatusMaintainer manages creating and renewing the status for this Scheduler
type StatusMaintainer interface {
	Run(stopCh <-chan struct{})
}
type maintainer struct {
	crdClient     godelclient.Interface
	schedulerName string
	renewInterval time.Duration
	clock         clock.Clock
}

// Run 启动调度器状态维护器的主循环。
// 它会定期同步调度器的状态信息到自定义资源定义（CRD）中，直到收到停止信号。
func (c *maintainer) Run(stopCh <-chan struct{}) {
	// 检查 CRD 客户端是否已初始化。
	// CRD 客户端用于与存储调度器状态的 CRD 进行交互。
	if c.crdClient == nil {
		// 如果客户端为 nil，记录错误日志并退出程序。
		// 这通常意味着配置错误或依赖注入失败。
		klog.Info("c.crdClient为nil")
		klog.ErrorS(nil, "Exited the scheduler status maintainer because the CRD client was nil", "schedulerName", c.schedulerName)
		// 确保日志被刷新到输出，然后以退出码 1 结束程序。
	}
	// 启动一个无限循环，定期执行同步操作。
	// wait.Until 会按照 c.renewInterval 指定的时间间隔，调用 c.sync 方法。
	// 当 stopCh 通道被关闭时，循环会停止，Run 方法返回。
	klog.Info("c.crdClient不为nil")
	wait.Until(c.sync, c.renewInterval, stopCh)
}

// sync attempts to update the status for Scheduler
// update Status.LastUpdateTime at the moment
// sync 同步调度器的当前状态到其对应的 CRD（自定义资源定义）中。
// 它尝试更新 CRD 对象以反映调度器的最新状态（如健康状况、活跃节点列表等）。
// 如果更新失败，它会记录一条信息日志，然后在下次定时周期重试。
func (c *maintainer) sync() {
	// 调用 ensureSchedulerUpToDate 函数来执行实际的状态更新逻辑。
	// 该函数会使用 c.crdClient 与 API Server 通信，更新与 c.schedulerName 相关的 CRD 状态。
	if err := ensureSchedulerUpToDate(c.crdClient, c.clock, c.schedulerName); err != nil {
		// 如果更新失败（例如网络问题、权限不足、资源冲突等），
		// 记录一条 Info 级别的日志，告知用户更新失败，并将在下一次 sync 周期（由 c.renewInterval 控制）重试。
		klog.InfoS("Failed to update scheduler status, will retry later", "schedulerName", c.schedulerName, "renewInterval", c.renewInterval)
	}
	// 如果更新成功，函数静默返回，等待下一次定时调用。
}

// ensureSchedulerUpToDate try to update scheduler status, if failed, retry after sleep duration, at most maxUpdateRetries
// ensureSchedulerUpToDate 确保调度器的状态在 CRD 中保持最新。
// 它会尝试最多 maxUpdateRetries 次调用 updateSchedulerStatus 来更新状态。
// 如果所有重试都失败，则返回一个错误。
func ensureSchedulerUpToDate(client godelclient.Interface, clock clock.Clock, schedulerName string) error {
	// 循环最多 maxUpdateRetries 次，尝试更新调度器状态。
	for i := 0; i < maxUpdateRetries; i++ {
		// 调用 updateSchedulerStatus 尝试更新调度器在 CRD 中的状态。
		err := updateSchedulerStatus(client, schedulerName)
		klog.Info("更新一次调度器资源")
		if err != nil {
			// 如果更新失败，记录一条 Info 日志，包含错误信息。
			klog.InfoS("Failed to update scheduler, will retry later", "schedulerName", schedulerName, "err", err)
			// 使用 clock.Sleep 暂停 sleep 时间，然后再进行下一次重试。
			// 这可以避免在失败时进行过于频繁的 API 调用。
			clock.Sleep(sleep)
			// 继续下一次循环尝试更新。
			continue
		}
		// 如果更新成功（err 为 nil），则退出函数，返回 nil。
		return nil
	}
	// 如果循环结束仍未成功（即 maxUpdateRetries 次都失败了），
	// 返回一个格式化的错误，表明所有重试都已用尽。
	return fmt.Errorf("failed %d attempts to update scheduler status", maxUpdateRetries)
}

// updateSchedulerStatus tries to update Scheduler status to apiserver, if Scheduler not exists, add new Scheduler to apiserver
// updateSchedulerStatus 更新或创建指定名称的调度器 CRD 对象及其状态。
// 它会尝试获取现有的调度器 CRD，如果存在则更新其状态中的最后更新时间；
// 如果不存在，则创建一个新的调度器 CRD 对象，并随后更新其状态。
func updateSchedulerStatus(client godelclient.Interface, schedulerName string) error {
	// 获取当前时间，用于记录最后更新时间。
	now := metav1.Now()

	//schedulerName+="-my-custom"
	// 尝试从 API Server 获取指定名称的调度器 CRD 对象。
	existed, err := godelutil.GetScheduler(client, schedulerName)
	if err == nil && existed != nil {
		// 如果获取成功且对象存在。
		// 创建一个现有对象的深拷贝，以避免修改原始对象。
		updated := existed.DeepCopy()
		// 更新深拷贝对象的状态，设置最后更新时间为当前时间。
		updated.Status.LastUpdateTime = &now

		// 调用工具函数更新调度器 CRD 的状态子资源。
		if _, err := godelutil.UpdateSchedulerStatus(client, updated); err != nil {
			// 如果更新状态失败，包装错误信息并返回，以便调用方进行重试。
			err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
			return err
		}
		// 更新成功，返回 nil。
		return nil
	}

	// 检查获取失败的原因是否是因为对象不存在 (IsNotFound)。
	// 如果不是 NotFound 错误，说明是其他问题（如网络、权限等），记录错误并返回。
	if !apierrors.IsNotFound(err) {
		err = fmt.Errorf("failed to get scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}

	// 如果错误是 NotFound (apierrors.IsNotFound(err) 为 true)，
	// 表示调度器 CRD 对象不存在。记录警告日志。
	klog.InfoS("WARN: scheduler was gone, should check this", "schedulerName", schedulerName, "err", err)

	// 准备创建一个新的调度器 CRD 对象。
	schedulerCRD := &v1alpha1.Scheduler{
		ObjectMeta: metav1.ObjectMeta{
			Name: schedulerName, // 设置对象名称。
		},
		// Status 字段可能在创建时是空的，稍后会单独更新。
	}

	// 调用工具函数创建调度器 CRD 对象。
	created, err := godelutil.PostScheduler(client, schedulerCRD)
	if err != nil {
		// 检查创建失败的原因是否是因为对象已存在 (IsAlreadyExists)。
		// 这可能发生在并发场景下。
		if apierrors.IsAlreadyExists(err) {
			// 如果是因为已存在，记录警告日志，但不视为错误，返回 nil。
			// 这可能意味着在检查和创建之间，另一个实例已经创建了该对象。
			klog.InfoS("WARN: skipped register because scheduler already existed", "schedulerName", schedulerName, "err", err)
			return nil
		}
		// 如果是其他创建错误，包装错误信息并返回。
		err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}

	// 创建成功。通常，创建对象时无法直接设置 status 子资源。
	// 因此，需要单独更新 status。
	// 更新刚创建对象的状态，设置最后更新时间为当前时间。
	created.Status.LastUpdateTime = &now
	// 调用工具函数更新调度器 CRD 的状态子资源。
	if _, err := godelutil.UpdateSchedulerStatus(client, created); err != nil {
		// 如果更新状态失败，包装错误信息并返回。
		err = fmt.Errorf("failed to update scheduler %v, will retry later, error is %v", schedulerName, err)
		return err
	}
	// 成功创建并更新状态，返回 nil。
	return nil
}

// NewSchedulerStatusMaintainer constructs and returns a maintainer
func NewSchedulerStatusMaintainer(clock clock.Clock, client godelclient.Interface, schedulerName string, renewIntervalSeconds int64) StatusMaintainer {
	renewInterval := time.Duration(renewIntervalSeconds) * time.Second
	return &maintainer{
		crdClient:     client,
		schedulerName: schedulerName,
		renewInterval: renewInterval,
		clock:         clock,
	}
}

// onSchedulerUpdate 是一个事件处理器函数，当监听的 Scheduler 自定义资源 (CRD) 发生更新时被调用。
// 此函数目前的实现是无条件地触发调度器的调度开关 (ScheduleSwitch)，
// 但具体的处理逻辑（在 Process 函数的回调中）是空的。
// 参数 `oldObj` 和 `newObj` 分别代表更新前和更新后的 Scheduler 对象，
// 但在此函数中并未使用它们的具体内容。
func (sched *Scheduler) onSchedulerUpdate(oldObj, newObj interface{}) {
	klog.Info("onSchedulerUpdate函数被调用")
	//// 调用调度开关的 Process 方法。
	//// 这通常用于通知调度器其内部状态或配置可能已更改，需要进行相应处理。
	//sched.ScheduleSwitch.Process(
	//	//
	//	// 目前硬编码为 framework.SwitchTypeAll，表示可能需要处理所有类型的变更。
	//	godelframework.SwitchTypeAll,
	//	// 传入一个空的处理函数。
	//	// 这个函数接收一个 ScheduleDataSet 参数，该参数理论上包含更新所需的数据。
	//	// 但当前实现中，此函数体为空，意味着没有实际的处理逻辑。
	//	func(dataSet ScheduleDataSet) {},
	//)
}
//-----------------------------------------------------------

// Scheduler watches for new unscheduled pods. It attempts to find
// nodes that they fit on and writes bindings back to the api server.
// Scheduler 是 Kubernetes 调度器的核心结构体。
// 它整合了调度器的各个组件，包括缓存、算法、扩展器、队列和配置集，负责执行 Pod 的调度逻辑。
type Scheduler struct {
	//------------------------------------------------------------
	// Name 用于标识这个 Godel 调度器实例的名称。
	Name string
	// SchedulerName 是更高层级的调度器名称，用于选择哪些 Pod 应由 Godel 调度器负责，
	// 并过滤掉不相关的 Pod。
	// Pod 的 Spec.SchedulerName 必须与此字段匹配，才会被此调度器处理。
	SchedulerName *string

	// informerFactory 是标准 Kubernetes 核心资源的 SharedInformer 工厂。
	informerFactory informers.SharedInformerFactory

	// crdInformerFactory 是 Godel 自定义资源的 SharedInformer 工厂。
	crdInformerFactory crdinformers.SharedInformerFactory

	// crdClient 是 Godel 自定义资源（如 Scheduler, PodGroup 等）的客户端接口。
	crdClient godelclient.Interface

	// options 存储调度器的配置选项。
	options schedulerOptions

	// podLister 是 Pod 资源的 Lister，提供对 Pod 信息的只读缓存访问。
	podLister corelisters.PodLister

	// commonCache 是调度器使用的缓存接口，用于存储和管理节点、Pod 等资源的状态信息。
	commonCache godelcache.SchedulerCache

	// mayHasPreemption 标记此调度器实例是否可能执行抢占（Preemption）操作。
	mayHasPreemption bool
	// defaultSubClusterConfig 是默认子集群的配置。
	defaultSubClusterConfig *subClusterConfig


	// schedulerMaintainer 是一个状态维护器，负责维护和更新调度器自身的状态。
	schedulerMaintainer StatusMaintainer


	// recorder 是事件记录器，用于向 Kubernetes API Server 发送调度器相关的事件。
	// 根据 KEP 383，这应该是新的 events.k8s.io/v1 API 的适配器。
	recorder events.EventRecorder

	//------------------------------------------------------------
	// SchedulerCache 是调度器的内部缓存，维护了集群的节点和 Pod 状态。
	// NodeLister 和 Algorithm 预期能观察到通过此缓存所做的更改。
	// 这是调度决策所需数据的主要来源。
	SchedulerCache internalcache.Cache

	// Algorithm 是调度器的核心调度算法实现。
	// 它负责执行具体的调度流程，如过滤节点和为 Pod 评分。
	// （在新版调度框架中，这个字段可能被 Profiles 中的框架取代，但可能仍保留用于兼容性或特定逻辑）
	Algorithm ScheduleAlgorithm

	// Extenders 是一个调度扩展器列表。
	// 调度器在执行调度决策时会调用这些扩展器，以支持更复杂的调度逻辑或外部系统集成。
	Extenders []framework.Extender

	// NextPod 是一个函数，用于从调度队列中获取下一个待调度的 Pod。
	// 它会阻塞直到有新的 Pod 可用，避免了使用通道时 Pod 可能变旧的问题。
	NextPod func() *framework.QueuedPodInfo

	// Error 是一个回调函数，当调度过程中发生错误时调用。
	// 它接收出错的 Pod 信息和错误详情。
	Error func(*framework.QueuedPodInfo, error)

	// StopEverything 是一个只读的信号通道。
	// 关闭此通道会通知调度器的所有组件停止运行。
	StopEverything <-chan struct{}

	// SchedulingQueue 是存储等待调度的 Pod 的队列。
	// Pod 在这里排队等待被调度器处理。
	SchedulingQueue internalqueue.SchedulingQueue

	// Profiles 是调度配置集的映射。
	// 每个配置集定义了一套独立的调度策略和插件，允许一个调度器实例支持多种调度行为。
	Profiles profile.Map

	// client 是一个标准的 Kubernetes API 客户端，用于与 API 服务器通信（如绑定 Pod 到节点）。
	client clientset.Interface
}

type schedulerOptions struct {
	componentConfigVersion   string
	kubeConfig               *restclient.Config
	legacyPolicySource       *schedulerapi.SchedulerPolicySource
	percentageOfNodesToScore int32
	podInitialBackoffSeconds int64
	podMaxBackoffSeconds     int64
	// Contains out-of-tree plugins to be merged with the in-tree registry.
	frameworkOutOfTreeRegistry frameworkruntime.Registry
	profiles                   []schedulerapi.KubeSchedulerProfile
	extenders                  []schedulerapi.Extender
	frameworkCapturer          FrameworkCapturer
	parallelism                int32
	applyDefaultProfile        bool
}

// Option configures a Scheduler
type Option func(*schedulerOptions)

// WithComponentConfigVersion sets the component config version to the
// KubeSchedulerConfiguration version used. The string should be the full
// scheme group/version of the external type we converted from (for example
// "kubescheduler.config.k8s.io/v1beta2")
func WithComponentConfigVersion(apiVersion string) Option {
	return func(o *schedulerOptions) {
		o.componentConfigVersion = apiVersion
	}
}

// WithKubeConfig sets the kube config for Scheduler.
func WithKubeConfig(cfg *restclient.Config) Option {
	return func(o *schedulerOptions) {
		o.kubeConfig = cfg
	}
}

// WithProfiles sets profiles for Scheduler. By default, there is one profile
// with the name "default-scheduler".
func WithProfiles(p ...schedulerapi.KubeSchedulerProfile) Option {
	return func(o *schedulerOptions) {
		o.profiles = p
		o.applyDefaultProfile = false
	}
}

// WithParallelism sets the parallelism for all scheduler algorithms. Default is 16.
func WithParallelism(threads int32) Option {
	return func(o *schedulerOptions) {
		o.parallelism = threads
	}
}

// WithLegacyPolicySource sets legacy policy config file source.
func WithLegacyPolicySource(source *schedulerapi.SchedulerPolicySource) Option {
	return func(o *schedulerOptions) {
		o.legacyPolicySource = source
	}
}

// WithPercentageOfNodesToScore sets percentageOfNodesToScore for Scheduler, the default value is 50
func WithPercentageOfNodesToScore(percentageOfNodesToScore int32) Option {
	return func(o *schedulerOptions) {
		o.percentageOfNodesToScore = percentageOfNodesToScore
	}
}

// WithFrameworkOutOfTreeRegistry sets the registry for out-of-tree plugins. Those plugins
// will be appended to the default registry.
func WithFrameworkOutOfTreeRegistry(registry frameworkruntime.Registry) Option {
	return func(o *schedulerOptions) {
		o.frameworkOutOfTreeRegistry = registry
	}
}

// WithPodInitialBackoffSeconds sets podInitialBackoffSeconds for Scheduler, the default value is 1
func WithPodInitialBackoffSeconds(podInitialBackoffSeconds int64) Option {
	return func(o *schedulerOptions) {
		o.podInitialBackoffSeconds = podInitialBackoffSeconds
	}
}

// WithPodMaxBackoffSeconds sets podMaxBackoffSeconds for Scheduler, the default value is 10
func WithPodMaxBackoffSeconds(podMaxBackoffSeconds int64) Option {
	return func(o *schedulerOptions) {
		o.podMaxBackoffSeconds = podMaxBackoffSeconds
	}
}

// WithExtenders sets extenders for the Scheduler
func WithExtenders(e ...schedulerapi.Extender) Option {
	return func(o *schedulerOptions) {
		o.extenders = e
	}
}

// FrameworkCapturer is used for registering a notify function in building framework.
type FrameworkCapturer func(schedulerapi.KubeSchedulerProfile)

// WithBuildFrameworkCapturer sets a notify function for getting buildFramework details.
func WithBuildFrameworkCapturer(fc FrameworkCapturer) Option {
	return func(o *schedulerOptions) {
		o.frameworkCapturer = fc
	}
}

var defaultSchedulerOptions = schedulerOptions{
	percentageOfNodesToScore: schedulerapi.DefaultPercentageOfNodesToScore,
	podInitialBackoffSeconds: int64(internalqueue.DefaultPodInitialBackoffDuration.Seconds()),
	podMaxBackoffSeconds:     int64(internalqueue.DefaultPodMaxBackoffDuration.Seconds()),
	parallelism:              int32(parallelize.DefaultParallelism),
	// Ideally we would statically set the default profile here, but we can't because
	// creating the default profile may require testing feature gates, which may get
	// set dynamically in tests. Therefore, we delay creating it until New is actually
	// invoked.
	applyDefaultProfile: true,
}

//-------------------------------------------------------------
type GodelschedulerOptions struct {
	defaultProfile     *config.GodelSchedulerProfile
	subClusterProfiles map[string]config.GodelSchedulerProfile

	renewInterval int64
	subClusterKey string
}
var defaultGodelSchedulerOptions = GodelschedulerOptions{
	renewInterval: config.DefaultRenewIntervalInSeconds,
	subClusterKey: config.DefaultSubClusterKey,
}
//--------------------------------------------------------

// New 返回一个新的调度器实例。
// 该函数负责初始化调度器的核心组件，包括缓存、插件注册表、配置器，并根据配置创建调度器对象。
//
// 参数:
// - client: 用于与 Kubernetes API 服务器通信的客户端接口。
// - informerFactory: 共享的 Informer 工厂，用于监听和缓存 API 对象。
// - recorderFactory: 用于创建事件记录器的工厂，用于向 API 服务器发送事件。
// - stopCh: 一个只读的通道，用于接收停止调度器的信号。
// - opts: 可选的配置选项，用于自定义调度器行为。
//
// 返回:
// - *Scheduler: 创建好的调度器实例。
// - error: 如果在创建过程中发生任何错误，则返回错误。
func New(
	godelSchedulerName string,
	schedulerName *string,
	crdClient godelclient.Interface,
	crdInformerFactory crdinformers.SharedInformerFactory,
	client clientset.Interface,
	informerFactory informers.SharedInformerFactory,
	recorderFactory profile.RecorderFactory,
	stopCh <-chan struct{},
	opts ...Option) (*Scheduler, error) {

	// 如果 stopCh 为 nil，则使用一个永远不会关闭的通道 wait.NeverStop。
	stopEverything := stopCh
	if stopEverything == nil {
		stopEverything = wait.NeverStop
	}

	// 从默认调度器选项开始，并应用所有传入的选项来覆盖默认值。
	options := defaultSchedulerOptions
	//--------------------------------------------------
	Godeloptions:=defaultGodelSchedulerOptions
	globalClock := clock.RealClock{}
	podLister := informerFactory.Core().V1().Pods().Lister()
	//-------------------------------------------------
	for _, opt := range opts {
		opt(&options)
	}

	// 如果需要应用默认配置文件，则从 Scheme 的默认值创建配置文件。
	//日志中没有打印，大概率没有使用下述代码
	//if options.applyDefaultProfile {
	//	klog.Info("应用默认配置文件")
	//	var versionedCfg v1beta2.KubeSchedulerConfiguration
	//	scheme.Scheme.Default(&versionedCfg) // 对版本化的配置对象设置默认值。
	//	cfg := config.KubeSchedulerConfiguration{}
	//	// 将版本化的配置对象转换为内部版本。
	//	if err := scheme.Scheme.Convert(&versionedCfg, &cfg, nil); err != nil {
	//		return nil, err
	//	}
	//	// 使用转换后的配置文件。
	//	options.profiles = cfg.Profiles
	//}

	// 创建调度器内部缓存，用于存储 Pod、Node 等信息。
	// durationToExpireAssumedPod 是假设 Pod 的过期时间。
	schedulerCache := internalcache.New(durationToExpireAssumedPod, stopEverything)

	// 创建内置（树内）插件的注册表。
	registry := frameworkplugins.NewInTreeRegistry()
	// 将外部（树外）插件注册表合并到内置注册表中。
	if err := registry.Merge(options.frameworkOutOfTreeRegistry); err != nil {
		return nil, err
	}

	// 创建一个空的快照，用于存储 Node 信息，供调度框架使用。
	snapshot := internalcache.NewEmptySnapshot()
	// 创建集群事件映射，用于跟踪不同事件类型需要触发的调度器配置文件。
	clusterEventMap := make(map[framework.ClusterEvent]sets.String)

	// 创建配置器，它负责根据配置选项和插件注册表创建调度器的核心组件。
	configurator := &Configurator{
		componentConfigVersion:   options.componentConfigVersion,                                        // 组件配置版本。
		client:                   client,                                                                // API 客户端。
		kubeConfig:               options.kubeConfig,                                                    // KubeConfig。
		recorderFactory:          recorderFactory,                                                       // 事件记录器工厂。
		informerFactory:          informerFactory,                                                       // Informer 工厂。
		schedulerCache:           schedulerCache,                                                        // 调度器缓存。
		StopEverything:           stopEverything,                                                        // 停止信号通道。
		percentageOfNodesToScore: options.percentageOfNodesToScore,                                      // 评分节点的百分比。
		podInitialBackoffSeconds: options.podInitialBackoffSeconds,                                      // Pod 初始回退秒数。
		podMaxBackoffSeconds:     options.podMaxBackoffSeconds,                                          // Pod 最大回退秒数。
		profiles:                 append([]schedulerapi.KubeSchedulerProfile(nil), options.profiles...), // 调度器配置文件。
		registry:                 registry,                                                              // 插件注册表。
		nodeInfoSnapshot:         snapshot,                                                              // Node 信息快照。
		extenders:                options.extenders,                                                     // 调度器扩展器。
		frameworkCapturer:        options.frameworkCapturer,                                             // 框架捕获器。
		parallellism:             options.parallelism,                                                   // 并行度。
		clusterEventMap:          clusterEventMap,                                                       // 集群事件映射。
	}

	// 注册调度器相关的指标。
	metrics.Register()

	var sched *Scheduler
	// 判断是否使用了旧版策略配置（Legacy Policy Source）。
	if options.legacyPolicySource == nil {
		//走的是这条路
		// 没有使用旧版策略，直接从组件配置创建调度器。
		klog.Info("没有使用旧版策略，直接从组件配置创建调度器。")
		sc, err := configurator.create(godelSchedulerName,schedulerName,stopCh)
		if err != nil {
			return nil, fmt.Errorf("couldn't create scheduler: %v", err)
		}
		sched = sc
		sched.mayHasPreemption=false
		sched.schedulerMaintainer=NewSchedulerStatusMaintainer(globalClock, crdClient, godelSchedulerName, Godeloptions.renewInterval)
		sched.podLister=podLister
		sched.informerFactory=informerFactory
		sched.crdInformerFactory=crdInformerFactory
	} else {
		klog.Info("使用旧版策略，从文件或configMap中加载")
		// 使用了旧版策略，需要从文件或 ConfigMap 加载策略。
		policy := &schedulerapi.Policy{}
		switch {
		case options.legacyPolicySource.File != nil:
			// 从文件加载策略。
			if err := initPolicyFromFile(options.legacyPolicySource.File.Path, policy); err != nil {
				return nil, err
			}
		case options.legacyPolicySource.ConfigMap != nil:
			// 从 ConfigMap 加载策略。
			if err := initPolicyFromConfigMap(client, options.legacyPolicySource.ConfigMap, policy); err != nil {
				return nil, err
			}
		}
		// 将从策略中解析出的扩展器设置到配置器上。
		// 在使用策略的情况下，这些扩展器不会通过组件配置选项预先设置。
		configurator.extenders = policy.Extenders
		// 根据加载的策略创建调度器。
		sc, err := configurator.createFromPolicy(*policy)
		if err != nil {
			return nil, fmt.Errorf("couldn't create scheduler from policy: %v", err)
		}
		sched = sc
	}

	// 对配置器创建的调度器实例进行额外的调整。
	sched.StopEverything = stopEverything // 设置停止信号通道。
	sched.client = client                 // 设置 API 客户端。

	// 构建动态客户端和动态 Informer 工厂（用于处理 CRD 等动态资源）。
	var dynInformerFactory dynamicinformer.DynamicSharedInformerFactory
	// options.kubeConfig 在测试中可能为 nil。
	if options.kubeConfig != nil {
		klog.Info("创建动态客户端")
		// 创建动态客户端。
		dynClient := dynamic.NewForConfigOrDie(options.kubeConfig)
		// 创建动态 Informer 工厂。
		dynInformerFactory = dynamicinformer.NewFilteredDynamicSharedInformerFactory(dynClient, 0, v1.NamespaceAll, nil)
	}

	// 为调度器添加所有必要的事件处理器，监听 Pod、Node 等资源的变化。
	addAllEventHandlers(sched, informerFactory, dynInformerFactory,crdInformerFactory, unionedGVKs(clusterEventMap))

	// 返回创建好的调度器实例。
	return sched, nil
}

func unionedGVKs(m map[framework.ClusterEvent]sets.String) map[framework.GVK]framework.ActionType {
	gvkMap := make(map[framework.GVK]framework.ActionType)
	for evt := range m {
		if _, ok := gvkMap[evt.Resource]; ok {
			gvkMap[evt.Resource] |= evt.ActionType
		} else {
			gvkMap[evt.Resource] = evt.ActionType
		}
	}
	return gvkMap
}

// initPolicyFromFile initialize policy from file
func initPolicyFromFile(policyFile string, policy *schedulerapi.Policy) error {
	// Use a policy serialized in a file.
	_, err := os.Stat(policyFile)
	if err != nil {
		return fmt.Errorf("missing policy config file %s", policyFile)
	}
	data, err := ioutil.ReadFile(policyFile)
	if err != nil {
		return fmt.Errorf("couldn't read policy config: %v", err)
	}
	err = runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), []byte(data), policy)
	if err != nil {
		return fmt.Errorf("invalid policy: %v", err)
	}
	return nil
}

// initPolicyFromConfigMap initialize policy from configMap
func initPolicyFromConfigMap(client clientset.Interface, policyRef *schedulerapi.SchedulerPolicyConfigMapSource, policy *schedulerapi.Policy) error {
	// Use a policy serialized in a config map value.
	policyConfigMap, err := client.CoreV1().ConfigMaps(policyRef.Namespace).Get(context.TODO(), policyRef.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("couldn't get policy config map %s/%s: %v", policyRef.Namespace, policyRef.Name, err)
	}
	data, found := policyConfigMap.Data[schedulerapi.SchedulerPolicyConfigMapKey]
	if !found {
		return fmt.Errorf("missing policy config map value at key %q", schedulerapi.SchedulerPolicyConfigMapKey)
	}
	err = runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), []byte(data), policy)
	if err != nil {
		return fmt.Errorf("invalid policy: %v", err)
	}
	return nil
}

// Run begins watching and scheduling. It starts scheduling and blocked until the context is done.
func (sched *Scheduler) Run(ctx context.Context) {
	go sched.schedulerMaintainer.Run(sched.StopEverything)
	sched.SchedulingQueue.Run()
	wait.UntilWithContext(ctx, sched.scheduleOne, 0)
	sched.SchedulingQueue.Close()
}

// recordSchedulingFailure records an event for the pod that indicates the
// pod has failed to schedule. Also, update the pod condition and nominated node name if set.
func (sched *Scheduler) recordSchedulingFailure(fwk framework.Framework, podInfo *framework.QueuedPodInfo, err error, reason string, nominatedNode string) {
	sched.Error(podInfo, err)

	// Update the scheduling queue with the nominated pod information. Without
	// this, there would be a race condition between the next scheduling cycle
	// and the time the scheduler receives a Pod Update for the nominated pod.
	// Here we check for nil only for tests.
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.AddNominatedPod(podInfo.PodInfo, nominatedNode)
	}

	pod := podInfo.Pod
	msg := truncateMessage(err.Error())
	fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", msg)
	if err := updatePod(sched.client, pod, &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  reason,
		Message: err.Error(),
	}, nominatedNode); err != nil {
		klog.ErrorS(err, "Error updating pod", "pod", klog.KObj(pod))
	}
}

// truncateMessage truncates a message if it hits the NoteLengthLimit.
func truncateMessage(message string) string {
	max := validation.NoteLengthLimit
	if len(message) <= max {
		return message
	}
	suffix := " ..."
	return message[:max-len(suffix)] + suffix
}

func updatePod(client clientset.Interface, pod *v1.Pod, condition *v1.PodCondition, nominatedNode string) error {
	klog.V(3).InfoS("Updating pod condition", "pod", klog.KObj(pod), "conditionType", condition.Type, "conditionStatus", condition.Status, "conditionReason", condition.Reason)
	podStatusCopy := pod.Status.DeepCopy()
	// NominatedNodeName is updated only if we are trying to set it, and the value is
	// different from the existing one.
	if !podutil.UpdatePodCondition(podStatusCopy, condition) &&
		(len(nominatedNode) == 0 || pod.Status.NominatedNodeName == nominatedNode) {
		return nil
	}
	if nominatedNode != "" {
		podStatusCopy.NominatedNodeName = nominatedNode
	}
	return util.PatchPodStatus(client, pod, podStatusCopy)
}

// assume signals to the cache that a pod is already in the cache, so that binding can be asynchronous.
// assume modifies `assumed`.
func (sched *Scheduler) assume(assumed *v1.Pod, host string) error {
	// Optimistically assume that the binding will succeed and send it to apiserver
	// in the background.
	// If the binding fails, scheduler will release resources allocated to assumed pod
	// immediately.
	assumed.Spec.NodeName = host

	if err := sched.SchedulerCache.AssumePod(assumed); err != nil {
		klog.ErrorS(err, "scheduler cache AssumePod failed")
		return err
	}
	// if "assumed" is a nominated pod, we should remove it from internal cache
	if sched.SchedulingQueue != nil {
		sched.SchedulingQueue.DeleteNominatedPodIfExists(assumed)
	}

	return nil
}

// bind binds a pod to a given node defined in a binding object.
// The precedence for binding is: (1) extenders and (2) framework plugins.
// We expect this to run asynchronously, so we handle binding metrics internally.
func (sched *Scheduler) bind(ctx context.Context, fwk framework.Framework, assumed *v1.Pod, targetNode string, state *framework.CycleState) (err error) {
	defer func() {
		sched.finishBinding(fwk, assumed, targetNode, err)
	}()

	bound, err := sched.extendersBinding(assumed, targetNode)
	if bound {
		return err
	}
	bindStatus := fwk.RunBindPlugins(ctx, state, assumed, targetNode)
	if bindStatus.IsSuccess() {
		return nil
	}
	if bindStatus.Code() == framework.Error {
		return bindStatus.AsError()
	}
	return fmt.Errorf("bind status: %s, %v", bindStatus.Code().String(), bindStatus.Message())
}

// TODO(#87159): Move this to a Plugin.
func (sched *Scheduler) extendersBinding(pod *v1.Pod, node string) (bool, error) {
	for _, extender := range sched.Extenders {
		if !extender.IsBinder() || !extender.IsInterested(pod) {
			continue
		}
		return true, extender.Bind(&v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID},
			Target:     v1.ObjectReference{Kind: "Node", Name: node},
		})
	}
	return false, nil
}

func (sched *Scheduler) finishBinding(fwk framework.Framework, assumed *v1.Pod, targetNode string, err error) {
	if finErr := sched.SchedulerCache.FinishBinding(assumed); finErr != nil {
		klog.ErrorS(finErr, "scheduler cache FinishBinding failed")
	}
	if err != nil {
		klog.V(1).InfoS("Failed to bind pod", "pod", klog.KObj(assumed))
		return
	}

	fwk.EventRecorder().Eventf(assumed, nil, v1.EventTypeNormal, "Scheduled", "Binding", "Successfully assigned %v/%v to %v", assumed.Namespace, assumed.Name, targetNode)
}

// scheduleOne does the entire scheduling workflow for a single pod. It is serialized on the scheduling algorithm's host fitting.
func (sched *Scheduler) scheduleOne(ctx context.Context) {
	podInfo := sched.NextPod()
	// pod could be nil when schedulerQueue is closed
	if podInfo == nil || podInfo.Pod == nil {
		return
	}
	pod := podInfo.Pod
	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		// This shouldn't happen, because we only accept for scheduling the pods
		// which specify a scheduler name that matches one of the profiles.
		klog.ErrorS(err, "Error occurred")
		return
	}
	if sched.skipPodSchedule(fwk, pod) {
		return
	}

	klog.V(3).InfoS("Attempting to schedule pod", "pod", klog.KObj(pod))

	// Synchronously attempt to find a fit for the pod.
	start := time.Now()
	state := framework.NewCycleState()
	state.SetRecordPluginMetrics(rand.Intn(100) < pluginMetricsSamplePercent)
	// Initialize an empty podsToActivate struct, which will be filled up by plugins or stay empty.
	podsToActivate := framework.NewPodsToActivate()
	state.Write(framework.PodsToActivateKey, podsToActivate)

	schedulingCycleCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	scheduleResult, err := sched.Algorithm.Schedule(schedulingCycleCtx, sched.Extenders, fwk, state, pod)
	if err != nil {
		// Schedule() may have failed because the pod would not fit on any host, so we try to
		// preempt, with the expectation that the next time the pod is tried for scheduling it
		// will fit due to the preemption. It is also possible that a different pod will schedule
		// into the resources that were preempted, but this is harmless.
		nominatedNode := ""
		if fitError, ok := err.(*framework.FitError); ok {
			if !fwk.HasPostFilterPlugins() {
				klog.V(3).InfoS("No PostFilter plugins are registered, so no preemption will be performed")
			} else {
				// Run PostFilter plugins to try to make the pod schedulable in a future scheduling cycle.
				result, status := fwk.RunPostFilterPlugins(ctx, state, pod, fitError.Diagnosis.NodeToStatusMap)
				if status.Code() == framework.Error {
					klog.ErrorS(nil, "Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", status)
				} else {
					klog.V(5).InfoS("Status after running PostFilter plugins for pod", "pod", klog.KObj(pod), "status", status)
				}
				if status.IsSuccess() && result != nil {
					nominatedNode = result.NominatedNodeName
				}
			}
			// Pod did not fit anywhere, so it is counted as a failure. If preemption
			// succeeds, the pod should get counted as a success the next time we try to
			// schedule it. (hopefully)
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
		} else if err == ErrNoNodesAvailable {
			// No nodes available is counted as unschedulable rather than an error.
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
		} else {
			klog.ErrorS(err, "Error selecting node for pod", "pod", klog.KObj(pod))
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		}
		sched.recordSchedulingFailure(fwk, podInfo, err, v1.PodReasonUnschedulable, nominatedNode)
		return
	}
	metrics.SchedulingAlgorithmLatency.Observe(metrics.SinceInSeconds(start))
	// Tell the cache to assume that a pod now is running on a given node, even though it hasn't been bound yet.
	// This allows us to keep scheduling without waiting on binding to occur.
	assumedPodInfo := podInfo.DeepCopy()
	assumedPod := assumedPodInfo.Pod
	// assume modifies `assumedPod` by setting NodeName=scheduleResult.SuggestedHost
	err = sched.assume(assumedPod, scheduleResult.SuggestedHost)
	if err != nil {
		metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		// This is most probably result of a BUG in retrying logic.
		// We report an error here so that pod scheduling can be retried.
		// This relies on the fact that Error will check if the pod has been bound
		// to a node and if so will not add it back to the unscheduled pods queue
		// (otherwise this would cause an infinite loop).
		sched.recordSchedulingFailure(fwk, assumedPodInfo, err, SchedulerError, "")
		return
	}

	// Run the Reserve method of reserve plugins.
	if sts := fwk.RunReservePluginsReserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost); !sts.IsSuccess() {
		metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
		// trigger un-reserve to clean up state associated with the reserved Pod
		fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		if forgetErr := sched.SchedulerCache.ForgetPod(assumedPod); forgetErr != nil {
			klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
		}
		sched.recordSchedulingFailure(fwk, assumedPodInfo, sts.AsError(), SchedulerError, "")
		return
	}

	// Run "permit" plugins.
	runPermitStatus := fwk.RunPermitPlugins(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
	if runPermitStatus.Code() != framework.Wait && !runPermitStatus.IsSuccess() {
		var reason string
		if runPermitStatus.IsUnschedulable() {
			metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
			reason = v1.PodReasonUnschedulable
		} else {
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			reason = SchedulerError
		}
		// One of the plugins returned status different than success or wait.
		fwk.RunReservePluginsUnreserve(schedulingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		if forgetErr := sched.SchedulerCache.ForgetPod(assumedPod); forgetErr != nil {
			klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
		}
		sched.recordSchedulingFailure(fwk, assumedPodInfo, runPermitStatus.AsError(), reason, "")
		return
	}

	// At the end of a successful scheduling cycle, pop and move up Pods if needed.
	if len(podsToActivate.Map) != 0 {
		sched.SchedulingQueue.Activate(podsToActivate.Map)
		// Clear the entries after activation.
		podsToActivate.Map = make(map[string]*v1.Pod)
	}

	// bind the pod to its host asynchronously (we can do this b/c of the assumption step above).
	go func() {
		bindingCycleCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		metrics.SchedulerGoroutines.WithLabelValues(metrics.Binding).Inc()
		defer metrics.SchedulerGoroutines.WithLabelValues(metrics.Binding).Dec()

		waitOnPermitStatus := fwk.WaitOnPermit(bindingCycleCtx, assumedPod)
		if !waitOnPermitStatus.IsSuccess() {
			var reason string
			if waitOnPermitStatus.IsUnschedulable() {
				metrics.PodUnschedulable(fwk.ProfileName(), metrics.SinceInSeconds(start))
				reason = v1.PodReasonUnschedulable
			} else {
				metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
				reason = SchedulerError
			}
			// trigger un-reserve plugins to clean up state associated with the reserved Pod
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if forgetErr := sched.SchedulerCache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
			}
			sched.recordSchedulingFailure(fwk, assumedPodInfo, waitOnPermitStatus.AsError(), reason, "")
			return
		}

		// Run "prebind" plugins.
		preBindStatus := fwk.RunPreBindPlugins(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
		if !preBindStatus.IsSuccess() {
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			// trigger un-reserve plugins to clean up state associated with the reserved Pod
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if forgetErr := sched.SchedulerCache.ForgetPod(assumedPod); forgetErr != nil {
				klog.ErrorS(forgetErr, "scheduler cache ForgetPod failed")
			}
			sched.recordSchedulingFailure(fwk, assumedPodInfo, preBindStatus.AsError(), SchedulerError, "")
			return
		}

		err := sched.bind(bindingCycleCtx, fwk, assumedPod, scheduleResult.SuggestedHost, state)
		if err != nil {
			metrics.PodScheduleError(fwk.ProfileName(), metrics.SinceInSeconds(start))
			// trigger un-reserve plugins to clean up state associated with the reserved Pod
			fwk.RunReservePluginsUnreserve(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)
			if err := sched.SchedulerCache.ForgetPod(assumedPod); err != nil {
				klog.ErrorS(err, "scheduler cache ForgetPod failed")
			}
			sched.recordSchedulingFailure(fwk, assumedPodInfo, fmt.Errorf("binding rejected: %w", err), SchedulerError, "")
		} else {
			// Calculating nodeResourceString can be heavy. Avoid it if klog verbosity is below 2.
			if klog.V(2).Enabled() {
				klog.InfoS("Successfully bound pod to node", "pod", klog.KObj(pod), "node", scheduleResult.SuggestedHost, "evaluatedNodes", scheduleResult.EvaluatedNodes, "feasibleNodes", scheduleResult.FeasibleNodes)
			}
			metrics.PodScheduled(fwk.ProfileName(), metrics.SinceInSeconds(start))
			metrics.PodSchedulingAttempts.Observe(float64(podInfo.Attempts))
			metrics.PodSchedulingDuration.WithLabelValues(getAttemptsLabel(podInfo)).Observe(metrics.SinceInSeconds(podInfo.InitialAttemptTimestamp))

			// Run "postbind" plugins.
			fwk.RunPostBindPlugins(bindingCycleCtx, state, assumedPod, scheduleResult.SuggestedHost)

			// At the end of a successful binding cycle, move up Pods if needed.
			if len(podsToActivate.Map) != 0 {
				sched.SchedulingQueue.Activate(podsToActivate.Map)
				// Unlike the logic in scheduling cycle, we don't bother deleting the entries
				// as `podsToActivate.Map` is no longer consumed.
			}
		}
	}()
}

func getAttemptsLabel(p *framework.QueuedPodInfo) string {
	// We breakdown the pod scheduling duration by attempts capped to a limit
	// to avoid ending up with a high cardinality metric.
	if p.Attempts >= 15 {
		return "15+"
	}
	return strconv.Itoa(p.Attempts)
}

func (sched *Scheduler) frameworkForPod(pod *v1.Pod) (framework.Framework, error) {
	fwk, ok := sched.Profiles[pod.Spec.SchedulerName]
	if !ok {
		return nil, fmt.Errorf("profile not found for scheduler name %q", pod.Spec.SchedulerName)
	}
	return fwk, nil
}

// skipPodSchedule returns true if we could skip scheduling the pod for specified cases.
func (sched *Scheduler) skipPodSchedule(fwk framework.Framework, pod *v1.Pod) bool {
	// Case 1: pod is being deleted.
	if pod.DeletionTimestamp != nil {
		fwk.EventRecorder().Eventf(pod, nil, v1.EventTypeWarning, "FailedScheduling", "Scheduling", "skip schedule deleting pod: %v/%v", pod.Namespace, pod.Name)
		klog.V(3).InfoS("Skip schedule deleting pod", "pod", klog.KObj(pod))
		return true
	}

	// Case 2: pod has been assumed could be skipped.
	// An assumed pod can be added again to the scheduling queue if it got an update event
	// during its previous scheduling cycle but before getting assumed.
	isAssumed, err := sched.SchedulerCache.IsAssumedPod(pod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to check whether pod %s/%s is assumed: %v", pod.Namespace, pod.Name, err))
		return false
	}
	return isAssumed
}

// NewInformerFactory creates a SharedInformerFactory and initializes a scheduler specific
// in-place podInformer.
func NewInformerFactory(cs clientset.Interface, resyncPeriod time.Duration) informers.SharedInformerFactory {
	informerFactory := informers.NewSharedInformerFactory(cs, resyncPeriod)
	informerFactory.InformerFor(&v1.Pod{}, newPodInformer)
	return informerFactory
}

// newPodInformer creates a shared index informer that returns only non-terminal pods.
func newPodInformer(cs clientset.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	selector := fmt.Sprintf("status.phase!=%v,status.phase!=%v", v1.PodSucceeded, v1.PodFailed)
	tweakListOptions := func(options *metav1.ListOptions) {
		options.FieldSelector = selector
	}
	return coreinformers.NewFilteredPodInformer(cs, metav1.NamespaceAll, resyncPeriod, nil, tweakListOptions)
}
