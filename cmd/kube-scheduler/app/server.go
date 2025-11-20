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

// Package app implements a Server object for running the scheduler.
package app

import (
	"context"
	"fmt"
	"net/http"
	"os"
	goruntime "runtime"

	"github.com/spf13/cobra"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/cli/globalflag"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/term"
	"k8s.io/component-base/version"
	"k8s.io/component-base/version/verflag"
	"k8s.io/klog/v2"
	schedulerserverconfig "k8s.io/kubernetes/cmd/kube-scheduler/app/config"
	"k8s.io/kubernetes/cmd/kube-scheduler/app/options"
	"k8s.io/kubernetes/pkg/scheduler"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/latest"
	"k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics/resources"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

// Option configures a framework.Registry.
type Option func(runtime.Registry) error

// NewSchedulerCommand creates a *cobra.Command object with default parameters and registryOptions
// NewSchedulerCommand 创建并返回一个 Cobra 命令，用于启动 kube-scheduler。
// 该命令封装了调度器的配置选项、使用说明、参数验证逻辑和执行入口。
//
// 参数:
// - registryOptions: 可选的 Option 参数，用于进一步配置调度器选项注册表。
//
// 返回:
// - *cobra.Command: 代表 kube-scheduler 的 Cobra 命令对象。
func NewSchedulerCommand(registryOptions ...Option) *cobra.Command {
	// 初始化调度器选项。这些选项包含了调度器运行所需的各种配置（如服务地址、认证、授权、领导者选举等）。
	opts := options.NewOptions()

	// 创建 Cobra 命令实例。
	cmd := &cobra.Command{
		// Use: 定义命令的使用方式，这里是 "kube-scheduler"。
		Use: "kube-scheduler",
		// Long: 提供命令的详细描述，解释了调度器的作用和基本工作原理。
		Long: `The Kubernetes scheduler is a control plane process which assigns
Pods to Nodes. The scheduler determines which Nodes are valid placements for
each Pod in the scheduling queue according to constraints and available
resources. The scheduler then ranks each valid Node and binds the Pod to a
suitable Node. Multiple different schedulers may be used within a cluster;
kube-scheduler is the reference implementation.
See [scheduling](https://kubernetes.io/docs/concepts/scheduling-eviction/)
for more information about scheduling and the kube-scheduler component.`,
		// Run: 定义命令执行时的核心逻辑。
		// 它调用 runCommand 函数，传入 Cobra 命令、配置选项和注册表选项。
		// 如果 runCommand 返回错误，则将错误信息打印到标准错误输出并退出程序。
		Run: func(cmd *cobra.Command, args []string) {
			if err := runCommand(cmd, opts, registryOptions...); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
		},
		// Args: 定义命令参数的验证逻辑。
		// 此处逻辑确保该命令不接受任何位置参数。如果提供了参数，则返回错误。
		Args: func(cmd *cobra.Command, args []string) error {
			for _, arg := range args {
				if len(arg) > 0 {
					return fmt.Errorf("%q does not take any arguments, got %q", cmd.CommandPath(), args)
				}
			}
			return nil
		},
	}

	// 获取选项对象中定义的标志集合 (FlagSets)。
	nfs := opts.Flags
	// 添加全局标志（如 --version, --help 等）到 "global" 标志集中。
	verflag.AddFlags(nfs.FlagSet("global"))
	// 添加全局标志（如 --kubeconfig, --logtostderr 等）到 "global" 标志集，并与命令名称关联。
	globalflag.AddGlobalFlags(nfs.FlagSet("global"), cmd.Name())
	// 获取命令的顶级标志集。
	fs := cmd.Flags()
	// 将选项对象中的所有标志集添加到命令的顶级标志集中，使这些标志在命令行可用。
	for _, f := range nfs.FlagSets {
		fs.AddFlagSet(f)
	}

	// 获取终端宽度，用于格式化输出帮助信息。
	cols, _, _ := term.TerminalSize(cmd.OutOrStdout())
	// 设置命令的帮助信息和使用方法格式化函数。
	cliflag.SetUsageAndHelpFunc(cmd, *nfs, cols)

	// 标记 "config" 标志的值应被视为文件名，并提供 "yaml", "yml", "json" 作为文件扩展名补全建议。
	cmd.MarkFlagFilename("config", "yaml", "yml", "json")

	// 返回配置完成的 Cobra 命令。
	return cmd
}

// runCommand runs the scheduler.
func runCommand(cmd *cobra.Command, opts *options.Options, registryOptions ...Option) error {
	verflag.PrintAndExitIfRequested()
	cliflag.PrintFlags(cmd.Flags())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		stopCh := server.SetupSignalHandler()
		<-stopCh
		cancel()
	}()

	cc, sched, err := Setup(ctx, opts, registryOptions...)
	if err != nil {
		return err
	}

	return Run(ctx, cc, sched)
}

// Run 根据给定的配置执行调度器。仅在发生错误或上下文完成时返回。
// 该函数负责初始化并启动 Kubernetes 调度器，包括设置健康检查、指标、事件广播、Informer 以及可选的领导者选举。
//
// 参数:
// - ctx: 用于取消调度器执行的上下文。
// - cc: 调度器服务器的完整配置，包含所有必要设置。
// - sched: 要运行的调度器实例。
//
// 返回:
// - error: 如果调度器启动或运行失败，则返回错误。
func Run(ctx context.Context, cc *schedulerserverconfig.CompletedConfig, sched *scheduler.Scheduler) error {
	// 为了便于调试，立即记录版本信息
	klog.InfoS("Starting Kubernetes Scheduler version", "version", version.Get())

	//// Configz 注册。
	//// 将组件配置注册到 configz 包，用于内省。
	//if cz, err := configz.New("componentconfig"); err == nil {
	//	cz.Set(cc.ComponentConfig)
	//} else {
	//	return fmt.Errorf("unable to register configz: %s", err)
	//}

	// 准备事件广播器。
	// 启动事件广播器，将事件记录到 API 服务器。
	cc.EventBroadcaster.StartRecordingToSink(ctx.Done())
	//
	//// 设置健康检查。
	//// 如果启用了领导者选举，则将领导者选举的 WatchDog 添加为健康检查项。
	//var checks []healthz.HealthChecker
	//if cc.ComponentConfig.LeaderElection.LeaderElect {
	//	checks = append(checks, cc.LeaderElection.WatchDog)
	//}

	//// 启动健康检查服务器。
	//// 启动一个不安全的 HTTP 服务器来提供健康检查端点。
	//if cc.InsecureServing != nil {
	//	separateMetrics := cc.InsecureMetricsServing != nil
	//	handler := buildHandlerChain(newHealthzHandler(&cc.ComponentConfig, cc.InformerFactory, isLeader, separateMetrics, checks...), nil, nil)
	//	if err := cc.InsecureServing.Serve(handler, 0, ctx.Done()); err != nil {
	//		return fmt.Errorf("failed to start healthz server: %v", err)
	//	}
	//}
	//// 启动指标服务器（不安全）。
	//// 启动一个不安全的 HTTP 服务器来提供指标端点。
	//if cc.InsecureMetricsServing != nil {
	//	handler := buildHandlerChain(newMetricsHandler(&cc.ComponentConfig, cc.InformerFactory, isLeader), nil, nil)
	//	if err := cc.InsecureMetricsServing.Serve(handler, 0, ctx.Done()); err != nil {
	//		return fmt.Errorf("failed to start metrics server: %v", err)
	//	}
	//}
	//// 启动安全服务器（健康检查和指标，如果已配置）。
	//// 启动一个安全的 HTTPS 服务器来提供健康检查和指标端点，
	//// 并进行身份验证和授权。
	//if cc.SecureServing != nil {
	//	handler := buildHandlerChain(newHealthzHandler(&cc.ComponentConfig, cc.InformerFactory, isLeader, false, checks...), cc.Authentication.Authenticator, cc.Authorization.Authorizer)
	//	//
	//	if _, err := cc.SecureServing.Serve(handler, 0, ctx.Done()); err != nil {
	//		// 对于安全处理器，尽早失败，移除上面的旧错误循环
	//		return fmt.Errorf("failed to start secure server: %v", err)
	//	}
	//}

	// 启动所有 Informer。
	// 启动 Informer 工厂，开始观察和缓存 API 对象。
	cc.InformerFactory.Start(ctx.Done())

	// 在调度之前等待所有缓存同步。
	// 等待所有 Informer 缓存与 API 服务器完全同步，
	// 以确保调度器拥有最新的数据。
	cc.InformerFactory.WaitForCacheSync(ctx.Done())

	// 启动调度器循环。
	sched.Run(ctx)
	// 此返回语句在正常情况下（如果 sched.Run 是阻塞调用）不应该被达到。
	// 它可能表示 sched.Run 内部处理的特定终止条件。
	return fmt.Errorf("finished without leader elect")
}

// buildHandlerChain wraps the given handler with the standard filters.
func buildHandlerChain(handler http.Handler, authn authenticator.Request, authz authorizer.Authorizer) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}
	failedHandler := genericapifilters.Unauthorized(scheme.Codecs)

	handler = genericapifilters.WithAuthorization(handler, authz, scheme.Codecs)
	handler = genericapifilters.WithAuthentication(handler, authn, failedHandler, nil)
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	handler = genericapifilters.WithCacheControl(handler)
	handler = genericfilters.WithHTTPLogging(handler)
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

func installMetricHandler(pathRecorderMux *mux.PathRecorderMux, informers informers.SharedInformerFactory, isLeader func() bool) {
	configz.InstallHandler(pathRecorderMux)
	pathRecorderMux.Handle("/metrics", legacyregistry.HandlerWithReset())

	resourceMetricsHandler := resources.Handler(informers.Core().V1().Pods().Lister())
	pathRecorderMux.HandleFunc("/metrics/resources", func(w http.ResponseWriter, req *http.Request) {
		if !isLeader() {
			return
		}
		resourceMetricsHandler.ServeHTTP(w, req)
	})
}

// newMetricsHandler builds a metrics server from the config.
func newMetricsHandler(config *kubeschedulerconfig.KubeSchedulerConfiguration, informers informers.SharedInformerFactory, isLeader func() bool) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux("kube-scheduler")
	installMetricHandler(pathRecorderMux, informers, isLeader)
	if config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routes.DebugFlags{}.Install(pathRecorderMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	return pathRecorderMux
}

// newHealthzHandler creates a healthz server from the config, and will also
// embed the metrics handler if the healthz and metrics address configurations
// are the same.
func newHealthzHandler(config *kubeschedulerconfig.KubeSchedulerConfiguration, informers informers.SharedInformerFactory, isLeader func() bool, separateMetrics bool, checks ...healthz.HealthChecker) http.Handler {
	pathRecorderMux := mux.NewPathRecorderMux("kube-scheduler")
	healthz.InstallHandler(pathRecorderMux, checks...)
	if !separateMetrics {
		installMetricHandler(pathRecorderMux, informers, isLeader)
	}
	if config.EnableProfiling {
		routes.Profiling{}.Install(pathRecorderMux)
		if config.EnableContentionProfiling {
			goruntime.SetBlockProfileRate(1)
		}
		routes.DebugFlags{}.Install(pathRecorderMux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	return pathRecorderMux
}

func getRecorderFactory(cc *schedulerserverconfig.CompletedConfig) profile.RecorderFactory {
	return func(name string) events.EventRecorder {
		return cc.EventBroadcaster.NewRecorder(name)
	}
}

// WithPlugin creates an Option based on plugin name and factory. Please don't remove this function: it is used to register out-of-tree plugins,
// hence there are no references to it from the kubernetes scheduler code base.
func WithPlugin(name string, factory runtime.PluginFactory) Option {
	return func(registry runtime.Registry) error {
		return registry.Register(name, factory)
	}
}

// Setup 根据命令行参数和选项创建一个完整的配置和一个调度器实例。
// 该函数负责初始化调度器的配置、验证选项、完成配置对象、注册外部插件、创建事件记录器工厂，
// 并最终实例化调度器对象。
//
// 参数:
// - ctx: 用于取消操作的上下文。
// - opts: 包含调度器配置选项的对象。
// - outOfTreeRegistryOptions: 用于注册外部（树外）插件的可选 Option 参数。
//
// 返回:
// - *schedulerserverconfig.CompletedConfig: 完整的调度器服务器配置。
// - *scheduler.Scheduler: 创建好的调度器实例。
// - error: 如果在设置过程中发生任何错误，则返回错误。
func Setup(ctx context.Context, opts *options.Options, outOfTreeRegistryOptions ...Option) (*schedulerserverconfig.CompletedConfig, *scheduler.Scheduler, error) {
	// 获取最新的默认组件配置，并将其设置到 opts.ComponentConfig 中。
	if cfg, err := latest.Default(); err != nil {
		return nil, nil, err
	} else {
		opts.ComponentConfig = cfg
	}

	// 验证所有配置选项是否有效。
	if errs := opts.Validate(); len(errs) > 0 {
		return nil, nil, utilerrors.NewAggregate(errs)
	}

	// 基于验证后的选项创建一个基础配置对象。
	c, err := opts.Config()
	if err != nil {
		return nil, nil, err
	}

	// 将基础配置对象转换为完整的配置对象，填充所有默认值和依赖项。
	cc := c.Complete()

	// 创建一个运行时注册表，用于注册外部（树外）插件。
	outOfTreeRegistry := make(runtime.Registry)
	// 遍历并应用所有提供的外部注册选项，将插件注册到 outOfTreeRegistry。
	for _, option := range outOfTreeRegistryOptions {
		if err := option(outOfTreeRegistry); err != nil {
			return nil, nil, err
		}
	}

	// 创建事件记录器工厂，用于调度器向 API 服务器发送事件。
	recorderFactory := getRecorderFactory(&cc)
	// 用于捕获和存储完成的调度器配置文件（Profiles）的切片。
	completedProfiles := make([]kubeschedulerconfig.KubeSchedulerProfile, 0)
	// 创建调度器实例。
	// 传入客户端、Informer 工厂、事件记录器工厂、停止信号通道以及各种配置选项。
	sched, err := scheduler.New(cc.Client,
		cc.InformerFactory,
		recorderFactory,
		ctx.Done(),
		scheduler.WithComponentConfigVersion(cc.ComponentConfig.TypeMeta.APIVersion),        // 设置组件配置版本。
		scheduler.WithKubeConfig(cc.KubeConfig),                                             // 设置 KubeConfig。
		scheduler.WithProfiles(cc.ComponentConfig.Profiles...),                              // 设置调度器配置文件。
		scheduler.WithLegacyPolicySource(cc.LegacyPolicySource),                             // 设置旧版策略源。
		scheduler.WithPercentageOfNodesToScore(cc.ComponentConfig.PercentageOfNodesToScore), // 设置评分节点的百分比。
		scheduler.WithFrameworkOutOfTreeRegistry(outOfTreeRegistry),                         // 设置外部插件注册表。
		scheduler.WithPodMaxBackoffSeconds(cc.ComponentConfig.PodMaxBackoffSeconds),         // 设置 Pod 最大回退秒数。
		scheduler.WithPodInitialBackoffSeconds(cc.ComponentConfig.PodInitialBackoffSeconds), // 设置 Pod 初始回退秒数。
		scheduler.WithExtenders(cc.ComponentConfig.Extenders...),                            // 设置调度器扩展器。
		scheduler.WithParallelism(cc.ComponentConfig.Parallelism),                           // 设置并行度。
		scheduler.WithBuildFrameworkCapturer(func(profile kubeschedulerconfig.KubeSchedulerProfile) {
			// 当框架实例化时会处理配置文件以设置默认插件和配置。捕获它们用于日志记录。
			completedProfiles = append(completedProfiles, profile)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	// 如果配置了写入配置文件，则将组件配置和完成的配置文件记录到日志或写入指定文件。
	//if err := options.LogOrWriteConfig(opts.WriteConfigTo, &cc.ComponentConfig, completedProfiles); err != nil {
	//	return nil, nil, err
	//}

	// 返回完整的配置和创建好的调度器实例。
	return &cc, sched, nil
}
