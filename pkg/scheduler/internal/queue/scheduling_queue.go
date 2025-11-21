/*
Copyright 2017 The Kubernetes Authors.

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

// This file contains structures that implement scheduling queue types.
// Scheduling queues hold pods waiting to be scheduled. This file implements a/
// priority queue which has two sub queues. One sub-queue holds pods that are
// being considered for scheduling. This is called activeQ. Another queue holds
// pods that are already tried and are determined to be unschedulable. The latter
// is called unschedulableQ.

package queue

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/interpodaffinity"
	"k8s.io/kubernetes/pkg/scheduler/internal/heap"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// If the pod stays in unschedulableQ longer than the unschedulableQTimeInterval,
	// the pod will be moved from unschedulableQ to activeQ.
	unschedulableQTimeInterval = 60 * time.Second

	queueClosed = "scheduling queue is closed"
)

const (
	// DefaultPodInitialBackoffDuration is the default value for the initial backoff duration
	// for unschedulable pods. To change the default podInitialBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultPodInitialBackoffDuration time.Duration = 1 * time.Second
	// DefaultPodMaxBackoffDuration is the default value for the max backoff duration
	// for unschedulable pods. To change the default podMaxBackoffDurationSeconds used by the
	// scheduler, update the ComponentConfig value in defaults.go
	DefaultPodMaxBackoffDuration time.Duration = 10 * time.Second
)

// PreEnqueueCheck is a function type. It's used to build functions that
// run against a Pod and the caller can choose to enqueue or skip the Pod
// by the checking result.
type PreEnqueueCheck func(pod *v1.Pod) bool

// SchedulingQueue is an interface for a queue to store pods waiting to be scheduled.
// The interface follows a pattern similar to cache.FIFO and cache.Heap and
// makes it easy to use those data structures as a SchedulingQueue.
type SchedulingQueue interface {
	framework.PodNominator
	Add(pod *v1.Pod) error
	// Activate moves the given pods to activeQ iff they're in unschedulableQ or backoffQ.
	// The passed-in pods are originally compiled from plugins that want to activate Pods,
	// by injecting the pods through a reserved CycleState struct (PodsToActivate).
	Activate(pods map[string]*v1.Pod)
	// AddUnschedulableIfNotPresent adds an unschedulable pod back to scheduling queue.
	// The podSchedulingCycle represents the current scheduling cycle number which can be
	// returned by calling SchedulingCycle().
	AddUnschedulableIfNotPresent(pod *framework.QueuedPodInfo, podSchedulingCycle int64) error
	// SchedulingCycle returns the current number of scheduling cycle which is
	// cached by scheduling queue. Normally, incrementing this number whenever
	// a pod is popped (e.g. called Pop()) is enough.
	SchedulingCycle() int64
	// Pop removes the head of the queue and returns it. It blocks if the
	// queue is empty and waits until a new item is added to the queue.
	Pop() (*framework.QueuedPodInfo, error)
	Update(oldPod, newPod *v1.Pod) error
	Delete(pod *v1.Pod) error
	MoveAllToActiveOrBackoffQueue(event framework.ClusterEvent, preCheck PreEnqueueCheck)
	AssignedPodAdded(pod *v1.Pod)
	AssignedPodUpdated(pod *v1.Pod)
	PendingPods() []*v1.Pod
	// Close closes the SchedulingQueue so that the goroutine which is
	// waiting to pop items can exit gracefully.
	Close()
	// NumUnschedulablePods returns the number of unschedulable pods exist in the SchedulingQueue.
	NumUnschedulablePods() int
	// Run starts the goroutines managing the queue.
	Run()
}

// NewSchedulingQueue initializes a priority queue as a new scheduling queue.
func NewSchedulingQueue(
	lessFn framework.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option) SchedulingQueue {
	return NewPriorityQueue(lessFn, informerFactory, opts...)
}

// NominatedNodeName returns nominated node name of a Pod.
func NominatedNodeName(pod *v1.Pod) string {
	return pod.Status.NominatedNodeName
}

// PriorityQueue implements a scheduling queue.
// The head of PriorityQueue is the highest priority pending pod. This structure
// has three sub queues. One sub-queue holds pods that are being considered for
// scheduling. This is called activeQ and is a Heap. Another queue holds
// pods that are already tried and are determined to be unschedulable. The latter
// is called unschedulableQ. The third queue holds pods that are moved from
// unschedulable queues and will be moved to active queue when backoff are completed.
type PriorityQueue struct {
	// PodNominator abstracts the operations to maintain nominated Pods.
	framework.PodNominator

	stop  chan struct{}
	clock util.Clock

	// pod initial backoff duration.
	podInitialBackoffDuration time.Duration
	// pod maximum backoff duration.
	podMaxBackoffDuration time.Duration

	lock sync.RWMutex
	cond sync.Cond

	// activeQ is heap structure that scheduler actively looks at to find pods to
	// schedule. Head of heap is the highest priority pod.
	activeQ *heap.Heap
	// podBackoffQ is a heap ordered by backoff expiry. Pods which have completed backoff
	// are popped from this heap before the scheduler looks at activeQ
	podBackoffQ *heap.Heap
	// unschedulableQ holds pods that have been tried and determined unschedulable.
	unschedulableQ *UnschedulablePodsMap
	// schedulingCycle represents sequence number of scheduling cycle and is incremented
	// when a pod is popped.
	schedulingCycle int64
	// moveRequestCycle caches the sequence number of scheduling cycle when we
	// received a move request. Unschedulable pods in and before this scheduling
	// cycle will be put back to activeQueue if we were trying to schedule them
	// when we received move request.
	moveRequestCycle int64

	clusterEventMap map[framework.ClusterEvent]sets.String

	// closed indicates that the queue is closed.
	// It is mainly used to let Pop() exit its control loop while waiting for an item.
	closed bool

	nsLister listersv1.NamespaceLister
}

type priorityQueueOptions struct {
	clock                     util.Clock
	podInitialBackoffDuration time.Duration
	podMaxBackoffDuration     time.Duration
	podNominator              framework.PodNominator
	clusterEventMap           map[framework.ClusterEvent]sets.String
}

// Option configures a PriorityQueue
type Option func(*priorityQueueOptions)

// WithClock sets clock for PriorityQueue, the default clock is util.RealClock.
func WithClock(clock util.Clock) Option {
	return func(o *priorityQueueOptions) {
		o.clock = clock
	}
}

// WithPodInitialBackoffDuration sets pod initial backoff duration for PriorityQueue.
func WithPodInitialBackoffDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podInitialBackoffDuration = duration
	}
}

// WithPodMaxBackoffDuration sets pod max backoff duration for PriorityQueue.
func WithPodMaxBackoffDuration(duration time.Duration) Option {
	return func(o *priorityQueueOptions) {
		o.podMaxBackoffDuration = duration
	}
}

// WithPodNominator sets pod nominator for PriorityQueue.
func WithPodNominator(pn framework.PodNominator) Option {
	return func(o *priorityQueueOptions) {
		o.podNominator = pn
	}
}

// WithClusterEventMap sets clusterEventMap for PriorityQueue.
func WithClusterEventMap(m map[framework.ClusterEvent]sets.String) Option {
	return func(o *priorityQueueOptions) {
		o.clusterEventMap = m
	}
}

var defaultPriorityQueueOptions = priorityQueueOptions{
	clock:                     util.RealClock{},
	podInitialBackoffDuration: DefaultPodInitialBackoffDuration,
	podMaxBackoffDuration:     DefaultPodMaxBackoffDuration,
}

// Making sure that PriorityQueue implements SchedulingQueue.
var _ SchedulingQueue = &PriorityQueue{}

// newQueuedPodInfoForLookup builds a QueuedPodInfo object for a lookup in the queue.
func newQueuedPodInfoForLookup(pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	// Since this is only used for a lookup in the queue, we only need to set the Pod,
	// and so we avoid creating a full PodInfo, which is expensive to instantiate frequently.
	return &framework.QueuedPodInfo{
		PodInfo:              &framework.PodInfo{Pod: pod},
		UnschedulablePlugins: sets.NewString(plugins...),
	}
}

// NewPriorityQueue creates a PriorityQueue object.
func NewPriorityQueue(
	lessFn framework.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) *PriorityQueue {
	options := defaultPriorityQueueOptions
	for _, opt := range opts {
		opt(&options)
	}

	comp := func(podInfo1, podInfo2 interface{}) bool {
		pInfo1 := podInfo1.(*framework.QueuedPodInfo)
		pInfo2 := podInfo2.(*framework.QueuedPodInfo)
		return lessFn(pInfo1, pInfo2)
	}

	if options.podNominator == nil {
		options.podNominator = NewPodNominator(informerFactory.Core().V1().Pods().Lister())
	}

	pq := &PriorityQueue{
		PodNominator:              options.podNominator,
		clock:                     options.clock,
		stop:                      make(chan struct{}),
		podInitialBackoffDuration: options.podInitialBackoffDuration,
		podMaxBackoffDuration:     options.podMaxBackoffDuration,
		activeQ:                   heap.NewWithRecorder(podInfoKeyFunc, comp, metrics.NewActivePodsRecorder()),
		unschedulableQ:            newUnschedulablePodsMap(metrics.NewUnschedulablePodsRecorder()),
		moveRequestCycle:          -1,
		clusterEventMap:           options.clusterEventMap,
	}
	pq.cond.L = &pq.lock
	pq.podBackoffQ = heap.NewWithRecorder(podInfoKeyFunc, pq.podsCompareBackoffCompleted, metrics.NewBackoffPodsRecorder())
	if utilfeature.DefaultFeatureGate.Enabled(features.PodAffinityNamespaceSelector) {
		pq.nsLister = informerFactory.Core().V1().Namespaces().Lister()
	}

	return pq
}

// Run 启动两个后台 goroutine，用于定期维护调度队列。
// 一个 goroutine 负责将完成退避时间的 Pod 从 backoffQ 移回 activeQ。
// 另一个 goroutine 负责将长时间停留在 unschedulableQ 中的 Pod 移回 activeQ 或 backoffQ。
// 这两个 goroutine 会持续运行，直到接收到停止信号 (p.stop)。
func (p *PriorityQueue) Run() {
	// 启动一个 goroutine，以 1 秒的间隔调用 p.flushBackoffQCompleted 方法。
	// 这确保了在 backoffQ 中等待的 Pod 能够在其退避时间结束后尽快被移回 activeQ，参与下一轮调度。
	go wait.Until(p.flushBackoffQCompleted, 1.0*time.Second, p.stop)

	// 启动另一个 goroutine，以 30 秒的间隔调用 p.flushUnschedulableQLeftover 方法。
	// 这允许在 unschedulableQ 中停留超过阈值的 Pod 有机会被重新评估和调度，
	// 防止它们因集群状态变化而被永久搁置。
	go wait.Until(p.flushUnschedulableQLeftover, 30*time.Second, p.stop)
}

// Add adds a pod to the active queue. It should be called only when a new pod
// is added so there is no chance the pod is already in active/unschedulable/backoff queues
// Add 将一个 Pod 添加到调度队列的 activeQ 中。
// 这是 PriorityQueue 实现的 framework.PodQueue 接口的一部分。
// 该方法确保 Pod 在添加到 activeQ 之前，不会同时存在于 unschedulableQ 或 podBackoffQ 中，
// 以维护队列之间 Pod 状态的一致性。
func (p *PriorityQueue) Add(pod *v1.Pod) error {
	// 加锁以保证并发安全
	p.lock.Lock()
	defer p.lock.Unlock()

	// 创建 QueuedPodInfo 对象，该对象封装了 Pod 及其在队列中的元数据（如入队时间戳、次序号等）
	pInfo := p.newQueuedPodInfo(pod)

	// 将 PodInfo 添加到 activeQ。activeQ 中的 Pod 是下一轮调度循环的候选者。
	if err := p.activeQ.Add(pInfo); err != nil {
		// 如果添加失败（例如队列已满或内部错误），记录错误并返回
		klog.ErrorS(err, "Error adding pod to the scheduling queue", "pod", klog.KObj(pod))
		return err
	}

	// 检查 Pod 是否意外地存在于 unschedulableQ 中。
	// 理论上，在调用 Add 之前，Pod 不应该在 unschedulableQ 中，因为 Add 通常由 Pod 创建事件触发。
	// 但如果存在，说明队列状态不一致，需要清理 unschedulableQ 中的记录。
	if p.unschedulableQ.get(pod) != nil {
		// 记录错误日志，表明存在状态不一致问题
		klog.ErrorS(nil, "Error: pod is already in the unschedulable queue", "pod", klog.KObj(pod))
		// 从 unschedulableQ 中删除该 Pod
		p.unschedulableQ.delete(pod)
	}

	// 检查 Pod 是否意外地存在于 podBackoffQ 中。
	// 与 unschedulableQ 类似，如果存在，说明队列状态不一致。
	// 这里使用 Delete 尝试从 podBackoffQ 中移除，如果成功 (err == nil)，说明 Pod 确实存在于 backoffQ。
	if err := p.podBackoffQ.Delete(pInfo); err == nil {
		// 记录错误日志，表明 Pod 存在于 backoffQ 中
		klog.ErrorS(nil, "Error: pod is already in the podBackoff queue", "pod", klog.KObj(pod))
		// Delete 已经将 Pod 从 backoffQ 中移除，无需额外操作
	}

	// 更新指标：记录一个 Pod 被添加到 activeQ (来源为 PodAdd)
	metrics.SchedulerQueueIncomingPods.WithLabelValues("active", PodAdd).Inc()

	// 将 Pod 添加到 PodNominator 中。
	// PodNominator 用于记录 Pod 的提名节点 (nominated node)，用于处理抢占逻辑。
	// 第二个参数 "" 表示没有特定的提名节点 (通常在 Add 时为空)。
	p.PodNominator.AddNominatedPod(pInfo.PodInfo, "")

	// 广播条件变量，通知等待在 p.cond 上的其他 Goroutine (例如调度循环)
	// 队列状态已更新，可能有新的 Pod 可以被调度。
	p.cond.Broadcast()

	// 成功添加，返回 nil
	return nil
}

// Activate moves the given pods to activeQ iff they're in unschedulableQ or backoffQ.
func (p *PriorityQueue) Activate(pods map[string]*v1.Pod) {
	p.lock.Lock()
	defer p.lock.Unlock()

	activated := false
	for _, pod := range pods {
		if p.activate(pod) {
			activated = true
		}
	}

	if activated {
		p.cond.Broadcast()
	}
}

func (p *PriorityQueue) activate(pod *v1.Pod) bool {
	// Verify if the pod is present in activeQ.
	if _, exists, _ := p.activeQ.Get(newQueuedPodInfoForLookup(pod)); exists {
		// No need to activate if it's already present in activeQ.
		return false
	}
	var pInfo *framework.QueuedPodInfo
	// Verify if the pod is present in unschedulableQ or backoffQ.
	if pInfo = p.unschedulableQ.get(pod); pInfo == nil {
		// If the pod doesn't belong to unschedulableQ or backoffQ, don't activate it.
		if obj, exists, _ := p.podBackoffQ.Get(newQueuedPodInfoForLookup(pod)); !exists {
			klog.ErrorS(nil, "To-activate pod does not exist in unschedulableQ or backoffQ", "pod", klog.KObj(pod))
			return false
		} else {
			pInfo = obj.(*framework.QueuedPodInfo)
		}
	}

	if pInfo == nil {
		// Redundant safe check. We shouldn't reach here.
		klog.ErrorS(nil, "Internal error: cannot obtain pInfo")
		return false
	}

	if err := p.activeQ.Add(pInfo); err != nil {
		klog.ErrorS(err, "Error adding pod to the scheduling queue", "pod", klog.KObj(pod))
		return false
	}
	p.unschedulableQ.delete(pod)
	p.podBackoffQ.Delete(pInfo)
	metrics.SchedulerQueueIncomingPods.WithLabelValues("active", ForceActivate).Inc()
	p.PodNominator.AddNominatedPod(pInfo.PodInfo, "")
	return true
}

// isPodBackingoff returns true if a pod is still waiting for its backoff timer.
// If this returns true, the pod should not be re-tried.
func (p *PriorityQueue) isPodBackingoff(podInfo *framework.QueuedPodInfo) bool {
	boTime := p.getBackoffTime(podInfo)
	return boTime.After(p.clock.Now())
}

// SchedulingCycle returns current scheduling cycle.
func (p *PriorityQueue) SchedulingCycle() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.schedulingCycle
}

// AddUnschedulableIfNotPresent inserts a pod that cannot be scheduled into
// the queue, unless it is already in the queue. Normally, PriorityQueue puts
// unschedulable pods in `unschedulableQ`. But if there has been a recent move
// request, then the pod is put in `podBackoffQ`.
func (p *PriorityQueue) AddUnschedulableIfNotPresent(pInfo *framework.QueuedPodInfo, podSchedulingCycle int64) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	pod := pInfo.Pod
	if p.unschedulableQ.get(pod) != nil {
		return fmt.Errorf("Pod %v is already present in unschedulable queue", klog.KObj(pod))
	}

	if _, exists, _ := p.activeQ.Get(pInfo); exists {
		return fmt.Errorf("Pod %v is already present in the active queue", klog.KObj(pod))
	}
	if _, exists, _ := p.podBackoffQ.Get(pInfo); exists {
		return fmt.Errorf("Pod %v is already present in the backoff queue", klog.KObj(pod))
	}

	// Refresh the timestamp since the pod is re-added.
	pInfo.Timestamp = p.clock.Now()

	// If a move request has been received, move it to the BackoffQ, otherwise move
	// it to unschedulableQ.
	if p.moveRequestCycle >= podSchedulingCycle {
		if err := p.podBackoffQ.Add(pInfo); err != nil {
			return fmt.Errorf("error adding pod %v to the backoff queue: %v", pod.Name, err)
		}
		metrics.SchedulerQueueIncomingPods.WithLabelValues("backoff", ScheduleAttemptFailure).Inc()
	} else {
		p.unschedulableQ.addOrUpdate(pInfo)
		metrics.SchedulerQueueIncomingPods.WithLabelValues("unschedulable", ScheduleAttemptFailure).Inc()
	}

	p.PodNominator.AddNominatedPod(pInfo.PodInfo, "")
	return nil
}

// flushBackoffQCompleted 将 backoffQ 中已完成退避（backoff）时间的 Pod 移动到 activeQ。
//
// 在 Kubernetes 调度器中，当 Pod 因调度失败等原因被放入 backoffQ 时，
// 会根据一定的策略计算一个退避时间。这个方法会周期性地检查 backoffQ 的队首，
// 如果队首 Pod 的退避时间已过，则将其移回 activeQ 以供重新调度。
// 该方法会持续处理队列，直到遇到一个尚未完成退避的 Pod 或队列为空。
func (p *PriorityQueue) flushBackoffQCompleted() {
	// 加锁以保证并发安全
	p.lock.Lock()
	defer p.lock.Unlock()

	// 使用 for 循环持续处理队列，直到队列为空或遇到未完成退避的 Pod
	for {
		// 查看 backoffQ 的队首元素，但不移除它
		// 这里使用 Peek 而不是 Pop 是为了先检查退避时间是否已过，避免不必要的 Pop 和后续的错误处理
		rawPodInfo := p.podBackoffQ.Peek()
		if rawPodInfo == nil {
			// 如果队列为空，直接返回
			return
		}
		// 断言类型并获取 Pod 对象
		pod := rawPodInfo.(*framework.QueuedPodInfo).Pod
		// 计算该 Pod 的退避结束时间
		boTime := p.getBackoffTime(rawPodInfo.(*framework.QueuedPodInfo))

		// 检查当前时间是否已经超过了该 Pod 的退避结束时间
		if boTime.After(p.clock.Now()) {
			// 如果退避时间未过，说明队列中后续的 Pod 也一定未过退避时间（因为队列是按时间排序的）
			// 因此可以安全地退出循环
			return
		}

		// 退避时间已过，从 backoffQ 中移除该 Pod
		_, err := p.podBackoffQ.Pop()
		if err != nil {
			// 理论上不应该发生，因为 Peek 刚刚确认了元素存在
			// 如果发生错误，记录日志并退出循环
			klog.ErrorS(err, "Unable to pop pod from backoff queue despite backoff completion", "pod", klog.KObj(pod))
			return
		}

		// 将已完成退避的 Pod 添加到 activeQ，使其可以被重新调度
		p.activeQ.Add(rawPodInfo)

		// 更新指标：记录一个 Pod 从 backoffQ 移动到 activeQ
		metrics.SchedulerQueueIncomingPods.WithLabelValues("active", BackoffComplete).Inc()

		// 注意：defer p.cond.Broadcast() 放在循环内部是不寻常的。
		// 通常 p.cond.Broadcast() 会在所有 Pod 处理完毕后调用一次。
		// 这里的写法意味着每处理一个 Pod 就广播一次，可能会带来性能开销。
		// 这行代码的作用是通知等待在条件变量 p.cond 上的其他 Goroutine（例如调度循环）队列状态已更新。
		defer p.cond.Broadcast()
	}
}

// flushUnschedulableQLeftover moves pod which stays in unschedulableQ longer than the unschedulableQTimeInterval
// to activeQ.
// flushUnschedulableQLeftover 将在 unschedulableQ 中停留时间过长（超过 unschedulableQTimeInterval）的 Pod
// 重新移回 activeQ 或 backoffQ，以便再次尝试调度。
//
// 这个方法通常在调度器的周期性清理逻辑中被调用，防止 Pod 长时间滞留在 unschedulableQ 中。
// 例如，集群资源状况可能已发生变化（如节点扩容、其他 Pod 被删除），使得原本无法调度的 Pod 变得可调度。
func (p *PriorityQueue) flushUnschedulableQLeftover() {
	// 加锁以保证并发安全
	p.lock.Lock()
	defer p.lock.Unlock()

	var podsToMove []*framework.QueuedPodInfo // 存储需要移动的 Pod 信息
	currentTime := p.clock.Now()              // 获取当前时间

	// 遍历 unschedulableQ 中的所有 Pod
	for _, pInfo := range p.unschedulableQ.podInfoMap {
		lastScheduleTime := pInfo.Timestamp // 获取 Pod 上次尝试调度的时间戳
		// 计算当前时间与上次调度时间的差值
		if currentTime.Sub(lastScheduleTime) > unschedulableQTimeInterval {
			// 如果时间差超过了预设的阈值 (unschedulableQTimeInterval)
			// 将该 Pod 加入待移动列表
			podsToMove = append(podsToMove, pInfo)
		}
	}

	// 如果有待移动的 Pod
	if len(podsToMove) > 0 {
		// 调用内部方法将这些 Pod 移回 activeQ 或 backoffQ
		// 移动原因标记为 UnschedulableTimeout，表示因在 unschedulableQ 中超时而移动
		p.movePodsToActiveOrBackoffQueue(podsToMove, UnschedulableTimeout)
	}
}

// Pop removes the head of the active queue and returns it. It blocks if the
// activeQ is empty and waits until a new item is added to the queue. It
// increments scheduling cycle when a pod is popped.
func (p *PriorityQueue) Pop() (*framework.QueuedPodInfo, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for p.activeQ.Len() == 0 {
		// When the queue is empty, invocation of Pop() is blocked until new item is enqueued.
		// When Close() is called, the p.closed is set and the condition is broadcast,
		// which causes this loop to continue and return from the Pop().
		if p.closed {
			return nil, fmt.Errorf(queueClosed)
		}
		p.cond.Wait()
	}
	obj, err := p.activeQ.Pop()
	if err != nil {
		return nil, err
	}
	pInfo := obj.(*framework.QueuedPodInfo)
	pInfo.Attempts++
	p.schedulingCycle++
	return pInfo, err
}

// isPodUpdated checks if the pod is updated in a way that it may have become
// schedulable. It drops status of the pod and compares it with old version.
func isPodUpdated(oldPod, newPod *v1.Pod) bool {
	strip := func(pod *v1.Pod) *v1.Pod {
		p := pod.DeepCopy()
		p.ResourceVersion = ""
		p.Generation = 0
		p.Status = v1.PodStatus{}
		p.ManagedFields = nil
		p.Finalizers = nil
		return p
	}
	return !reflect.DeepEqual(strip(oldPod), strip(newPod))
}

// Update updates a pod in the active or backoff queue if present. Otherwise, it removes
// the item from the unschedulable queue if pod is updated in a way that it may
// become schedulable and adds the updated one to the active queue.
// If pod is not present in any of the queues, it is added to the active queue.
func (p *PriorityQueue) Update(oldPod, newPod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if oldPod != nil {
		oldPodInfo := newQueuedPodInfoForLookup(oldPod)
		// If the pod is already in the active queue, just update it there.
		if oldPodInfo, exists, _ := p.activeQ.Get(oldPodInfo); exists {
			pInfo := updatePod(oldPodInfo, newPod)
			p.PodNominator.UpdateNominatedPod(oldPod, pInfo.PodInfo)
			return p.activeQ.Update(pInfo)
		}

		// If the pod is in the backoff queue, update it there.
		if oldPodInfo, exists, _ := p.podBackoffQ.Get(oldPodInfo); exists {
			pInfo := updatePod(oldPodInfo, newPod)
			p.PodNominator.UpdateNominatedPod(oldPod, pInfo.PodInfo)
			return p.podBackoffQ.Update(pInfo)
		}
	}

	// If the pod is in the unschedulable queue, updating it may make it schedulable.
	if usPodInfo := p.unschedulableQ.get(newPod); usPodInfo != nil {
		pInfo := updatePod(usPodInfo, newPod)
		p.PodNominator.UpdateNominatedPod(oldPod, pInfo.PodInfo)
		if isPodUpdated(oldPod, newPod) {
			if p.isPodBackingoff(usPodInfo) {
				if err := p.podBackoffQ.Add(pInfo); err != nil {
					return err
				}
				p.unschedulableQ.delete(usPodInfo.Pod)
			} else {
				if err := p.activeQ.Add(pInfo); err != nil {
					return err
				}
				p.unschedulableQ.delete(usPodInfo.Pod)
				p.cond.Broadcast()
			}
		} else {
			// Pod update didn't make it schedulable, keep it in the unschedulable queue.
			p.unschedulableQ.addOrUpdate(pInfo)
		}

		return nil
	}
	// If pod is not in any of the queues, we put it in the active queue.
	pInfo := p.newQueuedPodInfo(newPod)
	if err := p.activeQ.Add(pInfo); err != nil {
		return err
	}
	p.PodNominator.AddNominatedPod(pInfo.PodInfo, "")
	p.cond.Broadcast()
	return nil
}

// Delete deletes the item from either of the two queues. It assumes the pod is
// only in one queue.
func (p *PriorityQueue) Delete(pod *v1.Pod) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.PodNominator.DeleteNominatedPodIfExists(pod)
	if err := p.activeQ.Delete(newQueuedPodInfoForLookup(pod)); err != nil {
		// The item was probably not found in the activeQ.
		p.podBackoffQ.Delete(newQueuedPodInfoForLookup(pod))
		p.unschedulableQ.delete(pod)
	}
	return nil
}

// AssignedPodAdded is called when a bound pod is added. Creation of this pod
// may make pending pods with matching affinity terms schedulable.
func (p *PriorityQueue) AssignedPodAdded(pod *v1.Pod) {
	p.lock.Lock()
	p.movePodsToActiveOrBackoffQueue(p.getUnschedulablePodsWithMatchingAffinityTerm(pod), AssignedPodAdd)
	p.lock.Unlock()
}

// AssignedPodUpdated is called when a bound pod is updated. Change of labels
// may make pending pods with matching affinity terms schedulable.
func (p *PriorityQueue) AssignedPodUpdated(pod *v1.Pod) {
	p.lock.Lock()
	p.movePodsToActiveOrBackoffQueue(p.getUnschedulablePodsWithMatchingAffinityTerm(pod), AssignedPodUpdate)
	p.lock.Unlock()
}

// MoveAllToActiveOrBackoffQueue moves all pods from unschedulableQ to activeQ or backoffQ.
// This function adds all pods and then signals the condition variable to ensure that
// if Pop() is waiting for an item, it receives it after all the pods are in the
// queue and the head is the highest priority pod.
func (p *PriorityQueue) MoveAllToActiveOrBackoffQueue(event framework.ClusterEvent, preCheck PreEnqueueCheck) {
	p.lock.Lock()
	defer p.lock.Unlock()
	unschedulablePods := make([]*framework.QueuedPodInfo, 0, len(p.unschedulableQ.podInfoMap))
	for _, pInfo := range p.unschedulableQ.podInfoMap {
		if preCheck == nil || preCheck(pInfo.Pod) {
			unschedulablePods = append(unschedulablePods, pInfo)
		}
	}
	p.movePodsToActiveOrBackoffQueue(unschedulablePods, event)
}

// NOTE: this function assumes lock has been acquired in caller
func (p *PriorityQueue) movePodsToActiveOrBackoffQueue(podInfoList []*framework.QueuedPodInfo, event framework.ClusterEvent) {
	moved := false
	for _, pInfo := range podInfoList {
		// If the event doesn't help making the Pod schedulable, continue.
		// Note: we don't run the check if pInfo.UnschedulablePlugins is nil, which denotes
		// either there is some abnormal error, or scheduling the pod failed by plugins other than PreFilter, Filter and Permit.
		// In that case, it's desired to move it anyways.
		if len(pInfo.UnschedulablePlugins) != 0 && !p.podMatchesEvent(pInfo, event) {
			continue
		}
		moved = true
		pod := pInfo.Pod
		if p.isPodBackingoff(pInfo) {
			if err := p.podBackoffQ.Add(pInfo); err != nil {
				klog.ErrorS(err, "Error adding pod to the backoff queue", "pod", klog.KObj(pod))
			} else {
				metrics.SchedulerQueueIncomingPods.WithLabelValues("backoff", event.Label).Inc()
				p.unschedulableQ.delete(pod)
			}
		} else {
			if err := p.activeQ.Add(pInfo); err != nil {
				klog.ErrorS(err, "Error adding pod to the scheduling queue", "pod", klog.KObj(pod))
			} else {
				metrics.SchedulerQueueIncomingPods.WithLabelValues("active", event.Label).Inc()
				p.unschedulableQ.delete(pod)
			}
		}
	}
	p.moveRequestCycle = p.schedulingCycle
	if moved {
		p.cond.Broadcast()
	}
}

// getUnschedulablePodsWithMatchingAffinityTerm returns unschedulable pods which have
// any affinity term that matches "pod".
// NOTE: this function assumes lock has been acquired in caller.
func (p *PriorityQueue) getUnschedulablePodsWithMatchingAffinityTerm(pod *v1.Pod) []*framework.QueuedPodInfo {
	nsSelectorEnabled := p.nsLister != nil
	var nsLabels labels.Set
	if nsSelectorEnabled {
		nsLabels = interpodaffinity.GetNamespaceLabelsSnapshot(pod.Namespace, p.nsLister)
	}
	var podsToMove []*framework.QueuedPodInfo
	for _, pInfo := range p.unschedulableQ.podInfoMap {
		for _, term := range pInfo.RequiredAffinityTerms {
			if term.Matches(pod, nsLabels, nsSelectorEnabled) {
				podsToMove = append(podsToMove, pInfo)
				break
			}
		}

	}
	return podsToMove
}

// PendingPods returns all the pending pods in the queue. This function is
// used for debugging purposes in the scheduler cache dumper and comparer.
func (p *PriorityQueue) PendingPods() []*v1.Pod {
	p.lock.RLock()
	defer p.lock.RUnlock()
	var result []*v1.Pod
	for _, pInfo := range p.activeQ.List() {
		result = append(result, pInfo.(*framework.QueuedPodInfo).Pod)
	}
	for _, pInfo := range p.podBackoffQ.List() {
		result = append(result, pInfo.(*framework.QueuedPodInfo).Pod)
	}
	for _, pInfo := range p.unschedulableQ.podInfoMap {
		result = append(result, pInfo.Pod)
	}
	return result
}

// Close closes the priority queue.
func (p *PriorityQueue) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()
	close(p.stop)
	p.closed = true
	p.cond.Broadcast()
}

// DeleteNominatedPodIfExists deletes <pod> from nominatedPods.
func (npm *nominator) DeleteNominatedPodIfExists(pod *v1.Pod) {
	npm.Lock()
	npm.delete(pod)
	npm.Unlock()
}

// AddNominatedPod adds a pod to the nominated pods of the given node.
// This is called during the preemption process after a node is nominated to run
// the pod. We update the structure before sending a request to update the pod
// object to avoid races with the following scheduling cycles.
func (npm *nominator) AddNominatedPod(pi *framework.PodInfo, nodeName string) {
	npm.Lock()
	npm.add(pi, nodeName)
	npm.Unlock()
}

// NominatedPodsForNode returns pods that are nominated to run on the given node,
// but they are waiting for other pods to be removed from the node.
func (npm *nominator) NominatedPodsForNode(nodeName string) []*framework.PodInfo {
	npm.RLock()
	defer npm.RUnlock()
	// TODO: we may need to return a copy of []*Pods to avoid modification
	// on the caller side.
	return npm.nominatedPods[nodeName]
}

func (p *PriorityQueue) podsCompareBackoffCompleted(podInfo1, podInfo2 interface{}) bool {
	pInfo1 := podInfo1.(*framework.QueuedPodInfo)
	pInfo2 := podInfo2.(*framework.QueuedPodInfo)
	bo1 := p.getBackoffTime(pInfo1)
	bo2 := p.getBackoffTime(pInfo2)
	return bo1.Before(bo2)
}

// NumUnschedulablePods returns the number of unschedulable pods exist in the SchedulingQueue.
func (p *PriorityQueue) NumUnschedulablePods() int {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.unschedulableQ.podInfoMap)
}

// newQueuedPodInfo builds a QueuedPodInfo object.
func (p *PriorityQueue) newQueuedPodInfo(pod *v1.Pod, plugins ...string) *framework.QueuedPodInfo {
	now := p.clock.Now()
	return &framework.QueuedPodInfo{
		PodInfo:                 framework.NewPodInfo(pod),
		Timestamp:               now,
		InitialAttemptTimestamp: now,
		UnschedulablePlugins:    sets.NewString(plugins...),
	}
}

// getBackoffTime returns the time that podInfo completes backoff
func (p *PriorityQueue) getBackoffTime(podInfo *framework.QueuedPodInfo) time.Time {
	duration := p.calculateBackoffDuration(podInfo)
	backoffTime := podInfo.Timestamp.Add(duration)
	return backoffTime
}

// calculateBackoffDuration is a helper function for calculating the backoffDuration
// based on the number of attempts the pod has made.
func (p *PriorityQueue) calculateBackoffDuration(podInfo *framework.QueuedPodInfo) time.Duration {
	duration := p.podInitialBackoffDuration
	for i := 1; i < podInfo.Attempts; i++ {
		duration = duration * 2
		if duration > p.podMaxBackoffDuration {
			return p.podMaxBackoffDuration
		}
	}
	return duration
}

func updatePod(oldPodInfo interface{}, newPod *v1.Pod) *framework.QueuedPodInfo {
	pInfo := oldPodInfo.(*framework.QueuedPodInfo)
	pInfo.Update(newPod)
	return pInfo
}

// UnschedulablePodsMap holds pods that cannot be scheduled. This data structure
// is used to implement unschedulableQ.
type UnschedulablePodsMap struct {
	// podInfoMap is a map key by a pod's full-name and the value is a pointer to the QueuedPodInfo.
	podInfoMap map[string]*framework.QueuedPodInfo
	keyFunc    func(*v1.Pod) string
	// metricRecorder updates the counter when elements of an unschedulablePodsMap
	// get added or removed, and it does nothing if it's nil
	metricRecorder metrics.MetricRecorder
}

// Add adds a pod to the unschedulable podInfoMap.
func (u *UnschedulablePodsMap) addOrUpdate(pInfo *framework.QueuedPodInfo) {
	podID := u.keyFunc(pInfo.Pod)
	if _, exists := u.podInfoMap[podID]; !exists && u.metricRecorder != nil {
		u.metricRecorder.Inc()
	}
	u.podInfoMap[podID] = pInfo
}

// Delete deletes a pod from the unschedulable podInfoMap.
func (u *UnschedulablePodsMap) delete(pod *v1.Pod) {
	podID := u.keyFunc(pod)
	if _, exists := u.podInfoMap[podID]; exists && u.metricRecorder != nil {
		u.metricRecorder.Dec()
	}
	delete(u.podInfoMap, podID)
}

// Get returns the QueuedPodInfo if a pod with the same key as the key of the given "pod"
// is found in the map. It returns nil otherwise.
func (u *UnschedulablePodsMap) get(pod *v1.Pod) *framework.QueuedPodInfo {
	podKey := u.keyFunc(pod)
	if pInfo, exists := u.podInfoMap[podKey]; exists {
		return pInfo
	}
	return nil
}

// Clear removes all the entries from the unschedulable podInfoMap.
func (u *UnschedulablePodsMap) clear() {
	u.podInfoMap = make(map[string]*framework.QueuedPodInfo)
	if u.metricRecorder != nil {
		u.metricRecorder.Clear()
	}
}

// newUnschedulablePodsMap initializes a new object of UnschedulablePodsMap.
func newUnschedulablePodsMap(metricRecorder metrics.MetricRecorder) *UnschedulablePodsMap {
	return &UnschedulablePodsMap{
		podInfoMap:     make(map[string]*framework.QueuedPodInfo),
		keyFunc:        util.GetPodFullName,
		metricRecorder: metricRecorder,
	}
}

// nominator is a structure that stores pods nominated to run on nodes.
// It exists because nominatedNodeName of pod objects stored in the structure
// may be different than what scheduler has here. We should be able to find pods
// by their UID and update/delete them.
type nominator struct {
	// podLister is used to verify if the given pod is alive.
	podLister listersv1.PodLister
	// nominatedPods is a map keyed by a node name and the value is a list of
	// pods which are nominated to run on the node. These are pods which can be in
	// the activeQ or unschedulableQ.
	nominatedPods map[string][]*framework.PodInfo
	// nominatedPodToNode is map keyed by a Pod UID to the node name where it is
	// nominated.
	nominatedPodToNode map[types.UID]string

	sync.RWMutex
}

func (npm *nominator) add(pi *framework.PodInfo, nodeName string) {
	// always delete the pod if it already exist, to ensure we never store more than
	// one instance of the pod.
	npm.delete(pi.Pod)

	nnn := nodeName
	if len(nnn) == 0 {
		nnn = NominatedNodeName(pi.Pod)
		if len(nnn) == 0 {
			return
		}
	}

	if npm.podLister != nil {
		// If the pod was removed or if it was already scheduled, don't nominate it.
		updatedPod, err := npm.podLister.Pods(pi.Pod.Namespace).Get(pi.Pod.Name)
		if err != nil {
			klog.V(4).InfoS("Pod doesn't exist in podLister, aborted adding it to the nominator", "pod", klog.KObj(pi.Pod))
			return
		}
		if updatedPod.Spec.NodeName != "" {
			klog.V(4).InfoS("Pod is already scheduled to a node, aborted adding it to the nominator", "pod", klog.KObj(pi.Pod), "node", updatedPod.Spec.NodeName)
			return
		}
	}

	npm.nominatedPodToNode[pi.Pod.UID] = nnn
	for _, npi := range npm.nominatedPods[nnn] {
		if npi.Pod.UID == pi.Pod.UID {
			klog.V(4).InfoS("Pod already exists in the nominator", "pod", klog.KObj(npi.Pod))
			return
		}
	}
	npm.nominatedPods[nnn] = append(npm.nominatedPods[nnn], pi)
}

func (npm *nominator) delete(p *v1.Pod) {
	nnn, ok := npm.nominatedPodToNode[p.UID]
	if !ok {
		return
	}
	for i, np := range npm.nominatedPods[nnn] {
		if np.Pod.UID == p.UID {
			npm.nominatedPods[nnn] = append(npm.nominatedPods[nnn][:i], npm.nominatedPods[nnn][i+1:]...)
			if len(npm.nominatedPods[nnn]) == 0 {
				delete(npm.nominatedPods, nnn)
			}
			break
		}
	}
	delete(npm.nominatedPodToNode, p.UID)
}

// UpdateNominatedPod updates the <oldPod> with <newPod>.
func (npm *nominator) UpdateNominatedPod(oldPod *v1.Pod, newPodInfo *framework.PodInfo) {
	npm.Lock()
	defer npm.Unlock()
	// In some cases, an Update event with no "NominatedNode" present is received right
	// after a node("NominatedNode") is reserved for this pod in memory.
	// In this case, we need to keep reserving the NominatedNode when updating the pod pointer.
	nodeName := ""
	// We won't fall into below `if` block if the Update event represents:
	// (1) NominatedNode info is added
	// (2) NominatedNode info is updated
	// (3) NominatedNode info is removed
	if NominatedNodeName(oldPod) == "" && NominatedNodeName(newPodInfo.Pod) == "" {
		if nnn, ok := npm.nominatedPodToNode[oldPod.UID]; ok {
			// This is the only case we should continue reserving the NominatedNode
			nodeName = nnn
		}
	}
	// We update irrespective of the nominatedNodeName changed or not, to ensure
	// that pod pointer is updated.
	npm.delete(oldPod)
	npm.add(newPodInfo, nodeName)
}

// NewPodNominator creates a nominator as a backing of framework.PodNominator.
// A podLister is passed in so as to check if the pod exists
// before adding its nominatedNode info.
func NewPodNominator(podLister listersv1.PodLister) framework.PodNominator {
	return &nominator{
		podLister:          podLister,
		nominatedPods:      make(map[string][]*framework.PodInfo),
		nominatedPodToNode: make(map[types.UID]string),
	}
}

// MakeNextPodFunc returns a function to retrieve the next pod from a given
// scheduling queue
func MakeNextPodFunc(queue SchedulingQueue) func() *framework.QueuedPodInfo {
	return func() *framework.QueuedPodInfo {
		podInfo, err := queue.Pop()
		if err == nil {
			klog.V(4).InfoS("About to try and schedule pod", "pod", klog.KObj(podInfo.Pod))
			return podInfo
		}
		klog.ErrorS(err, "Error while retrieving next pod from scheduling queue")
		return nil
	}
}

func podInfoKeyFunc(obj interface{}) (string, error) {
	return cache.MetaNamespaceKeyFunc(obj.(*framework.QueuedPodInfo).Pod)
}

// Checks if the Pod may become schedulable upon the event.
// This is achieved by looking up the global clusterEventMap registry.
func (p *PriorityQueue) podMatchesEvent(podInfo *framework.QueuedPodInfo, clusterEvent framework.ClusterEvent) bool {
	if clusterEvent.IsWildCard() {
		return true
	}

	for evt, nameSet := range p.clusterEventMap {
		// Firstly verify if the two ClusterEvents match:
		// - either the registered event from plugin side is a WildCardEvent,
		// - or the two events have identical Resource fields and *compatible* ActionType.
		//   Note the ActionTypes don't need to be *identical*. We check if the ANDed value
		//   is zero or not. In this way, it's easy to tell Update&Delete is not compatible,
		//   but Update&All is.
		evtMatch := evt.IsWildCard() ||
			(evt.Resource == clusterEvent.Resource && evt.ActionType&clusterEvent.ActionType != 0)

		// Secondly verify the plugin name matches.
		// Note that if it doesn't match, we shouldn't continue to search.
		if evtMatch && intersect(nameSet, podInfo.UnschedulablePlugins) {
			return true
		}
	}

	return false
}

func intersect(x, y sets.String) bool {
	if len(x) > len(y) {
		x, y = y, x
	}
	for v := range x {
		if y.Has(v) {
			return true
		}
	}
	return false
}
