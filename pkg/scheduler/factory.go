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
	"errors"
	"fmt"
	"time"

	//------------------------------------------------------------------
	commoncache "github.com/kubewharf/godel-scheduler/pkg/common/cache"
	godelcache "github.com/kubewharf/godel-scheduler/pkg/scheduler/cache"
	//------------------------------------------------------------------

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/v1beta2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkplugins "k8s.io/kubernetes/pkg/scheduler/framework/plugins"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultpreemption"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/queuesort"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	internalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
	cachedebugger "k8s.io/kubernetes/pkg/scheduler/internal/cache/debugger"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

// Binder knows how to write a binding.
type Binder interface {
	Bind(binding *v1.Binding) error
}

// Configurator defines I/O, caching, and other functionality needed to
// construct a new scheduler.
type Configurator struct {
	client     clientset.Interface
	kubeConfig *restclient.Config

	recorderFactory profile.RecorderFactory

	informerFactory informers.SharedInformerFactory

	// Close this to stop all reflectors
	StopEverything <-chan struct{}

	schedulerCache internalcache.Cache

	componentConfigVersion string

	// Always check all predicates even if the middle of one predicate fails.
	alwaysCheckAllPredicates bool

	// percentageOfNodesToScore specifies percentage of all nodes to score in each scheduling cycle.
	percentageOfNodesToScore int32

	podInitialBackoffSeconds int64

	podMaxBackoffSeconds int64

	profiles          []schedulerapi.KubeSchedulerProfile
	registry          frameworkruntime.Registry
	nodeInfoSnapshot  *internalcache.Snapshot
	extenders         []schedulerapi.Extender
	frameworkCapturer FrameworkCapturer
	parallellism      int32
	// A "cluster event" -> "plugin names" map.
	clusterEventMap map[framework.ClusterEvent]sets.String
}

// create 方法根据 Configurator 中的配置（如插件、扩展器、配置集等）创建并初始化一个完整的 Scheduler 实例。
// 它负责处理扩展器 (Extenders)、配置插件参数、创建调度队列、设置缓存调试器等。
func (c *Configurator) create(godelSchedulerName string, schedulerName *string,stopEverything <-chan struct{}) (*Scheduler, error) {
	var extenders []framework.Extender // 存储所有扩展器实例。
	var ignoredExtendedResources []string // 存储扩展器管理的、应被调度器忽略的资源名称列表。

	// 如果配置了扩展器。
	if len(c.extenders) != 0 {
		klog.Info("确实使用了拓展组件因为len(c.extenders) != 0")
		var ignorableExtenders []framework.Extender // 存储可忽略错误的扩展器。

		// 遍历所有配置的扩展器定义。
		for ii := range c.extenders {
			klog.V(2).InfoS("Creating extender", "extender", c.extenders[ii])
			// 根据配置创建 HTTP 扩展器实例。
			extender, err := NewHTTPExtender(&c.extenders[ii])
			if err != nil {
				return nil, err
			}

			// 根据扩展器是否可忽略错误，将其添加到不同的列表。
			if !extender.IsIgnorable() {
				extenders = append(extenders, extender)
			} else {
				ignorableExtenders = append(ignorableExtenders, extender)
			}

			// 收集扩展器管理的、应被调度器忽略的资源。
			for _, r := range c.extenders[ii].ManagedResources {
				if r.IgnoredByScheduler {
					ignoredExtendedResources = append(ignoredExtendedResources, r.Name)
				}
			}
		}
		// 将可忽略错误的扩展器追加到扩展器列表的末尾。
		extenders = append(extenders, ignorableExtenders...)
	}

	// 如果从扩展器配置中找到了任何应被忽略的扩展资源。
	if len(ignoredExtendedResources) > 0 {
		klog.Info("从扩展器配置中找到了任何应被忽略的扩展资源")
		// 遍历所有调度配置集。
		for i := range c.profiles {
			prof := &c.profiles[i]
			var found = false
			// 在当前配置集的插件配置中查找 NodeResourcesFit 插件。
			for k := range prof.PluginConfig {
				if prof.PluginConfig[k].Name == noderesources.FitName {
					// 找到后，更新其参数，将扩展资源添加到忽略列表中。
					pc := &prof.PluginConfig[k]
					args, ok := pc.Args.(*schedulerapi.NodeResourcesFitArgs)
					if !ok {
						return nil, fmt.Errorf("want args to be of type NodeResourcesFitArgs, got %T", pc.Args)
					}
					args.IgnoredResources = ignoredExtendedResources
					found = true
					break
				}
			}
			// 如果在配置集中找不到 NodeResourcesFit 插件，则报错。
			if !found {
				return nil, fmt.Errorf("can't find NodeResourcesFitArgs in plugin config")
			}
		}
	}
	//-----------------------------------------------------------------------
	// 从 Informer Factory 获取核心资源的 Lister 和 Informer
	podLister := c.informerFactory.Core().V1().Pods().Lister()
	podInformer := c.informerFactory.Core().V1().Pods()

	// 检查是否启用了抢占（preemption）功能
	// mayHasPreemption := parseProfilesBoolConfiguration(options, profileNeedPreemption)

	// 构建通用缓存（commonCache）的初始化参数包装器
	handlerWrapper :=commoncache.MakeCacheHandlerWrapper().
		ComponentName(godelSchedulerName).
		SchedulerType(*schedulerName).
		PodAssumedTTL(15 * time.Minute). // Pod 假定（assumed）状态的 TTL
		Period(10 * time.Second).        // 缓存定期同步周期
		StopCh(stopEverything).
		PodLister(podLister).
		PodInformer(podInformer)
	//-----------------------------------------------------------------------

	// 创建 Pod 提名器 (Pod Nominator)，用于处理抢占逻辑。
	// 它需要 Pod 列表器 (Pod Lister) 来获取 Pod 信息。
	nominator := internalqueue.NewPodNominator(c.informerFactory.Core().V1().Pods().Lister())

	// 根据配置集定义、插件注册表、事件记录器等信息，创建并初始化所有调度配置集 (Profiles)。
	// 每个 Profile 都是一个独立的调度框架实例。
	profiles, err := profile.NewMap(c.profiles, c.registry, c.recorderFactory,
		frameworkruntime.WithComponentConfigVersion(c.componentConfigVersion), // 设置组件配置版本
		frameworkruntime.WithClientSet(c.client),                             // 提供 Kubernetes 客户端
		frameworkruntime.WithKubeConfig(c.kubeConfig),                        // 提供 KubeConfig
		frameworkruntime.WithInformerFactory(c.informerFactory),              // 提供 Informer 工厂
		frameworkruntime.WithSnapshotSharedLister(c.nodeInfoSnapshot),        // 提供节点信息快照列表器
		frameworkruntime.WithRunAllFilters(c.alwaysCheckAllPredicates),       // 设置是否总是运行所有过滤器
		frameworkruntime.WithPodNominator(nominator),                         // 提供提名器
		frameworkruntime.WithCaptureProfile(frameworkruntime.CaptureProfile(c.frameworkCapturer)), // 设置框架捕获器
		frameworkruntime.WithClusterEventMap(c.clusterEventMap),              // 提供集群事件映射
		frameworkruntime.WithParallelism(int(c.parallellism)),                // 设置并行度
		frameworkruntime.WithExtenders(extenders),                            // 提供扩展器列表
	)
	if err != nil {
		return nil, fmt.Errorf("initializing profiles: %v", err)
	}
	// 确保至少创建了一个配置集。
	if len(profiles) == 0 {
		return nil, errors.New("at least one profile is required")
	}

	// 获取第一个配置集的队列排序函数 (QueueSort plugin)。
	// 所有配置集必须使用相同的队列排序插件。
	lessFn := profiles[c.profiles[0].SchedulerName].QueueSortFunc()
	// 创建调度队列 (SchedulingQueue)，用于存放等待调度的 Pod。
	// 队列的排序由 lessFn 决定，并配置了回退时间、提名器等。
	podQueue := internalqueue.NewSchedulingQueue(
		lessFn,
		c.informerFactory,
		internalqueue.WithPodInitialBackoffDuration(time.Duration(c.podInitialBackoffSeconds)*time.Second), // 初始回退时间
		internalqueue.WithPodMaxBackoffDuration(time.Duration(c.podMaxBackoffSeconds)*time.Second),       // 最大回退时间
		internalqueue.WithPodNominator(nominator),                                                         // 提名器
		internalqueue.WithClusterEventMap(c.clusterEventMap),                                            // 集群事件映射
	)

	// 创建缓存调试器 (Cache Debugger)，用于监控和调试调度器内部缓存的状态。
	debugger := cachedebugger.New(
		c.informerFactory.Core().V1().Nodes().Lister(), // 节点列表器
		c.informerFactory.Core().V1().Pods().Lister(),  // Pod 列表器
		c.schedulerCache,                               // 调度器缓存
		podQueue,                                      // 调度队列
	)
	// 启动调试器监听信号，以便在接收到特定信号时打印缓存信息。
	debugger.ListenForSignal(c.StopEverything)

	// 创建通用调度算法 (GenericScheduler)，它是调度决策的核心逻辑。
	algo := NewGenericScheduler(
		c.schedulerCache,                    // 调度器缓存
		c.nodeInfoSnapshot,                  // 节点信息快照
		c.percentageOfNodesToScore,          // 评分节点的百分比阈值
	)
	scheduler := &Scheduler{
		Name:            godelSchedulerName,
		SchedulerName: 	 schedulerName,
		commonCache: godelcache.New(handlerWrapper.Obj()),
		SchedulerCache:  c.schedulerCache,
		Algorithm:       algo,
		Extenders:       extenders,
		Profiles:        profiles,
		NextPod:         internalqueue.MakeNextPodFunc(podQueue),
		Error:           MakeDefaultErrorFunc(c.client, c.informerFactory.Core().V1().Pods().Lister(), podQueue, c.schedulerCache),
		StopEverything:  c.StopEverything,
		SchedulingQueue: podQueue,
	}

	// 创建并返回最终的 Scheduler 实例。
	return scheduler, nil
}

// createFromPolicy creates a scheduler from the legacy policy file.
func (c *Configurator) createFromPolicy(policy schedulerapi.Policy) (*Scheduler, error) {
	lr := frameworkplugins.NewLegacyRegistry()
	args := &frameworkplugins.ConfigProducerArgs{}

	klog.V(2).InfoS("Creating scheduler from configuration", "policy", policy)

	// validate the policy configuration
	if err := validation.ValidatePolicy(policy); err != nil {
		return nil, err
	}

	// If profiles is already set, it means the user is using both CC and policy config, error out
	// since these configs are no longer merged and they should not be used simultaneously.
	if c.profiles != nil {
		return nil, fmt.Errorf("profiles and policy config both set, this should not happen")
	}

	predicateKeys := sets.NewString()
	if policy.Predicates == nil {
		predicateKeys = lr.DefaultPredicates
	} else {
		for _, predicate := range policy.Predicates {
			klog.V(2).InfoS("Registering predicate", "predicate", predicate.Name)
			predicateName, err := lr.ProcessPredicatePolicy(predicate, args)
			if err != nil {
				return nil, err
			}
			predicateKeys.Insert(predicateName)
		}
	}

	priorityKeys := make(map[string]int64)
	if policy.Priorities == nil {
		klog.V(2).InfoS("Using default priorities")
		priorityKeys = lr.DefaultPriorities
	} else {
		for _, priority := range policy.Priorities {
			if priority.Name == frameworkplugins.EqualPriority {
				klog.V(2).InfoS("Skip registering priority", "priority", priority.Name)
				continue
			}
			klog.V(2).InfoS("Registering priority", "priority", priority.Name)
			priorityName, err := lr.ProcessPriorityPolicy(priority, args)
			if err != nil {
				return nil, err
			}
			priorityKeys[priorityName] = priority.Weight
		}
	}

	// HardPodAffinitySymmetricWeight in the policy config takes precedence over
	// CLI configuration.
	if policy.HardPodAffinitySymmetricWeight != 0 {
		args.InterPodAffinityArgs = &schedulerapi.InterPodAffinityArgs{
			HardPodAffinityWeight: policy.HardPodAffinitySymmetricWeight,
		}
	}

	// When AlwaysCheckAllPredicates is set to true, scheduler checks all the configured
	// predicates even after one or more of them fails.
	if policy.AlwaysCheckAllPredicates {
		c.alwaysCheckAllPredicates = policy.AlwaysCheckAllPredicates
	}

	klog.V(2).InfoS("Creating scheduler", "predicates", predicateKeys, "priorities", priorityKeys)

	// Combine all framework configurations. If this results in any duplication, framework
	// instantiation should fail.

	// "PrioritySort", "DefaultPreemption" and "DefaultBinder" were neither predicates nor priorities
	// before. We add them by default.
	plugins := schedulerapi.Plugins{
		QueueSort: schedulerapi.PluginSet{
			Enabled: []schedulerapi.Plugin{{Name: queuesort.Name}},
		},
		PostFilter: schedulerapi.PluginSet{
			Enabled: []schedulerapi.Plugin{{Name: defaultpreemption.Name}},
		},
		Bind: schedulerapi.PluginSet{
			Enabled: []schedulerapi.Plugin{{Name: defaultbinder.Name}},
		},
	}
	var pluginConfig []schedulerapi.PluginConfig
	var err error
	if plugins, pluginConfig, err = lr.AppendPredicateConfigs(predicateKeys, args, plugins, pluginConfig); err != nil {
		return nil, err
	}
	if plugins, pluginConfig, err = lr.AppendPriorityConfigs(priorityKeys, args, plugins, pluginConfig); err != nil {
		return nil, err
	}
	if pluginConfig, err = dedupPluginConfigs(pluginConfig); err != nil {
		return nil, err
	}

	c.profiles = []schedulerapi.KubeSchedulerProfile{
		{
			SchedulerName: v1.DefaultSchedulerName,
			Plugins:       &plugins,
			PluginConfig:  pluginConfig,
		},
	}

	if err := defaultPluginConfigArgs(&c.profiles[0]); err != nil {
		return nil, err
	}

	//---------------------------------------
	//这里是一处修改，因为大概率用不上，所以直接返回nil,nil了
	//原先是return c.create()
	return nil,nil
}

func defaultPluginConfigArgs(prof *schedulerapi.KubeSchedulerProfile) error {
	scheme := v1beta2.GetPluginArgConversionScheme()
	existingConfigs := sets.NewString()
	for j := range prof.PluginConfig {
		existingConfigs.Insert(prof.PluginConfig[j].Name)
		// For existing plugin configs, we don't apply any defaulting, the assumption
		// is that the legacy registry does it already.
	}

	// Append default configs for plugins that didn't have one explicitly set.
	for _, name := range prof.Plugins.Names() {
		if existingConfigs.Has(name) {
			continue
		}
		gvk := v1beta2.SchemeGroupVersion.WithKind(name + "Args")
		args, err := scheme.New(gvk)
		if err != nil {
			if runtime.IsNotRegisteredError(err) {
				// This plugin is out-of-tree or doesn't require configuration.
				continue
			}
			return err
		}
		scheme.Default(args)
		internalArgs, err := scheme.ConvertToVersion(args, schedulerapi.SchemeGroupVersion)
		if err != nil {
			return fmt.Errorf("converting %q into internal type: %w", gvk.Kind, err)
		}
		prof.PluginConfig = append(prof.PluginConfig, schedulerapi.PluginConfig{
			Name: name,
			Args: internalArgs,
		})
	}

	return nil
}

// dedupPluginConfigs removes duplicates from pluginConfig, ensuring that,
// if a plugin name is repeated, the arguments are the same.
func dedupPluginConfigs(pc []schedulerapi.PluginConfig) ([]schedulerapi.PluginConfig, error) {
	args := make(map[string]runtime.Object)
	result := make([]schedulerapi.PluginConfig, 0, len(pc))
	for _, c := range pc {
		if v, found := args[c.Name]; !found {
			result = append(result, c)
			args[c.Name] = c.Args
		} else if !cmp.Equal(v, c.Args) {
			// This should be unreachable.
			return nil, fmt.Errorf("inconsistent configuration produced for plugin %s", c.Name)
		}
	}
	return result, nil
}

// MakeDefaultErrorFunc construct a function to handle pod scheduler error
func MakeDefaultErrorFunc(client clientset.Interface, podLister corelisters.PodLister, podQueue internalqueue.SchedulingQueue, schedulerCache internalcache.Cache) func(*framework.QueuedPodInfo, error) {
	return func(podInfo *framework.QueuedPodInfo, err error) {
		pod := podInfo.Pod
		if err == ErrNoNodesAvailable {
			klog.V(2).InfoS("Unable to schedule pod; no nodes are registered to the cluster; waiting", "pod", klog.KObj(pod))
		} else if fitError, ok := err.(*framework.FitError); ok {
			// Inject UnschedulablePlugins to PodInfo, which will be used later for moving Pods between queues efficiently.
			podInfo.UnschedulablePlugins = fitError.Diagnosis.UnschedulablePlugins
			klog.V(2).InfoS("Unable to schedule pod; no fit; waiting", "pod", klog.KObj(pod), "err", err)
		} else if apierrors.IsNotFound(err) {
			klog.V(2).InfoS("Unable to schedule pod, possibly due to node not found; waiting", "pod", klog.KObj(pod), "err", err)
			if errStatus, ok := err.(apierrors.APIStatus); ok && errStatus.Status().Details.Kind == "node" {
				nodeName := errStatus.Status().Details.Name
				// when node is not found, We do not remove the node right away. Trying again to get
				// the node and if the node is still not found, then remove it from the scheduler cache.
				_, err := client.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
				if err != nil && apierrors.IsNotFound(err) {
					node := v1.Node{ObjectMeta: metav1.ObjectMeta{Name: nodeName}}
					if err := schedulerCache.RemoveNode(&node); err != nil {
						klog.V(4).InfoS("Node is not found; failed to remove it from the cache", "node", node.Name)
					}
				}
			}
		} else {
			klog.ErrorS(err, "Error scheduling pod; retrying", "pod", klog.KObj(pod))
		}

		// Check if the Pod exists in informer cache.
		cachedPod, err := podLister.Pods(pod.Namespace).Get(pod.Name)
		if err != nil {
			klog.InfoS("Pod doesn't exist in informer cache", "pod", klog.KObj(pod), "err", err)
			return
		}

		// In the case of extender, the pod may have been bound successfully, but timed out returning its response to the scheduler.
		// It could result in the live version to carry .spec.nodeName, and that's inconsistent with the internal-queued version.
		if len(cachedPod.Spec.NodeName) != 0 {
			klog.InfoS("Pod has been assigned to node. Abort adding it back to queue.", "pod", klog.KObj(pod), "node", cachedPod.Spec.NodeName)
			return
		}

		// As <cachedPod> is from SharedInformer, we need to do a DeepCopy() here.
		podInfo.PodInfo = framework.NewPodInfo(cachedPod.DeepCopy())
		if err := podQueue.AddUnschedulableIfNotPresent(podInfo, podQueue.SchedulingCycle()); err != nil {
			klog.ErrorS(err, "Error occurred")
		}
	}
}
