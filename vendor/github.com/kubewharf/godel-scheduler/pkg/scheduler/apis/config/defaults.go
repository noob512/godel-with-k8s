/*
Copyright 2023 The Godel Scheduler Authors.

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
	"net"
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog"
	utilpointer "k8s.io/utils/pointer"

	defaultsconfig "github.com/kubewharf/godel-scheduler/pkg/apis/config"
	"github.com/kubewharf/godel-scheduler/pkg/util/tracing"
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_GodelSchedulerConfiguration 为 Godel 调度器配置结构体设置合理的默认值。
// 该函数确保即使用户未显式配置某些字段，调度器也能以安全、合理的默认行为运行。
func SetDefaults_GodelSchedulerConfiguration(obj *GodelSchedulerConfiguration) {
	klog.Info("使用内部版本的配置")

	// 1. LeaderElection 与 SchedulerRenewIntervalSeconds 配置
	{
		// 使用 Kubernetes 默认的 LeaderElection 配置（如租约时间、重试周期等）
		defaultsconfig.SetDefaultLeaderElectionConfiguration(&obj.LeaderElection)

		// 若未指定 Leader 选举使用的资源名称，默认使用调度器名称
		if len(obj.LeaderElection.ResourceName) == 0 {
			obj.LeaderElection.ResourceName = DefaultSchedulerName
		}

		// 若未设置调度器租约续期间隔（秒），使用默认值
		if obj.SchedulerRenewIntervalSeconds == 0 {
			obj.SchedulerRenewIntervalSeconds = DefaultRenewIntervalInSeconds
		}
	}

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

		// 处理 Healthz 服务绑定地址（用于健康检查）
		// 规则：
		// - 若为空，默认绑定到所有接口（0.0.0.0）+ 默认端口
		// - 若仅指定端口（如 ":10251"），则主机设为 0.0.0.0
		// - 若指定的是有效 IP 地址但无端口，则附加默认端口
		// - 其他无效情况回退到默认绑定地址
		if len(obj.HealthzBindAddress) == 0 {
			obj.HealthzBindAddress = DefaultBindAddress
		} else {
			if host, port, err := net.SplitHostPort(obj.HealthzBindAddress); err == nil {
				if len(host) == 0 {
					host = DefaultGodelSchedulerAddress // 实际应为 "0.0.0.0"
				}
				obj.HealthzBindAddress = net.JoinHostPort(host, port)
			} else {
				if host := net.ParseIP(obj.HealthzBindAddress); host != nil {
					obj.HealthzBindAddress = net.JoinHostPort(obj.HealthzBindAddress, strconv.Itoa(DefaultInsecureSchedulerPort))
				} else {
					// TODO: 未来应返回错误而非静默使用默认值
					obj.HealthzBindAddress = DefaultBindAddress
				}
			}
		}

		// 同样逻辑应用于 Metrics（指标暴露）绑定地址
		if len(obj.MetricsBindAddress) == 0 {
			obj.MetricsBindAddress = DefaultBindAddress
		} else {
			if host, port, err := net.SplitHostPort(obj.MetricsBindAddress); err == nil {
				if len(host) == 0 {
					host = DefaultGodelSchedulerAddress
				}
				obj.MetricsBindAddress = net.JoinHostPort(host, port)
			} else {
				if host := net.ParseIP(obj.MetricsBindAddress); host != nil {
					obj.MetricsBindAddress = net.JoinHostPort(obj.MetricsBindAddress, strconv.Itoa(DefaultInsecureSchedulerPort))
				} else {
					obj.MetricsBindAddress = DefaultBindAddress
				}
			}
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
			obj.GodelSchedulerName = DefaultGodelSchedulerName//这个可以自定义
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
			obj.DefaultProfile = &GodelSchedulerProfile{}
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
			obj.DefaultProfile.DisablePreemption = utilpointer.BoolPtr(DefaultDisablePreemption)
		}

		// 默认不禁用阻塞队列（BlockQueue）功能
		if obj.DefaultProfile.BlockQueue == nil {
			obj.DefaultProfile.BlockQueue = utilpointer.BoolPtr(DefaultBlockQueue)
		}

		// 设置等待删除 Pod 的最大容忍时长（用于资源回收）
		if obj.DefaultProfile.MaxWaitingDeletionDuration == 0 {
			obj.DefaultProfile.MaxWaitingDeletionDuration = DefaultMaxWaitingDeletionDuration
		}

		// 设置候选节点选择策略（如随机选择）
		if obj.DefaultProfile.CandidatesSelectPolicy == nil {
			obj.DefaultProfile.CandidatesSelectPolicy = utilpointer.String(CandidateSelectPolicyRandom)
		}

		// 设置更优节点选择策略列表（用于抢占决策，如升序、二分查找等）
		if obj.DefaultProfile.BetterSelectPolicies == nil {
			obj.DefaultProfile.BetterSelectPolicies = &StringSlice{
				BetterPreemptionPolicyAscending,
				BetterPreemptionPolicyDichotomy,
			}
		}
	}
}
