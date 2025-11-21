/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	//-----------------------------------------------------------------------------
	crdinformers "github.com/kubewharf/godel-scheduler-api/pkg/client/informers/externalversions"
	schedulingv1a1 "github.com/kubewharf/godel-scheduler-api/pkg/apis/scheduling/v1alpha1"
	//------------------------------------------------------------------------------
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/klog/v2"
	"reflect"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

func (sched *Scheduler) onStorageClassAdd(obj interface{}) {
	sc, ok := obj.(*storagev1.StorageClass)
	if !ok {
		klog.ErrorS(nil, "Cannot convert to *storagev1.StorageClass", "obj", obj)
		return
	}

	// CheckVolumeBindingPred fails if pod has unbound immediate PVCs. If these
	// PVCs have specified StorageClass name, creating StorageClass objects
	// with late binding will cause predicates to pass, so we need to move pods
	// to active queue.
	// We don't need to invalidate cached results because results will not be
	// cached for pod that has unbound immediate PVCs.
	if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.StorageClassAdd, nil)
	}
}

func (sched *Scheduler) addNodeToCache(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		klog.ErrorS(nil, "Cannot convert to *v1.Node", "obj", obj)
		return
	}

	nodeInfo := sched.SchedulerCache.AddNode(node)
	klog.V(3).InfoS("Add event for node", "node", klog.KObj(node))
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.NodeAdd, preCheckForNode(nodeInfo))
}

func (sched *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*v1.Node)
	if !ok {
		klog.ErrorS(nil, "Cannot convert oldObj to *v1.Node", "oldObj", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node)
	if !ok {
		klog.ErrorS(nil, "Cannot convert newObj to *v1.Node", "newObj", newObj)
		return
	}

	nodeInfo := sched.SchedulerCache.UpdateNode(oldNode, newNode)
	// Only requeue unschedulable pods if the node became more schedulable.
	if event := nodeSchedulingPropertiesChange(newNode, oldNode); event != nil {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(*event, preCheckForNode(nodeInfo))
	}
}

func (sched *Scheduler) deleteNodeFromCache(obj interface{}) {
	var node *v1.Node
	switch t := obj.(type) {
	case *v1.Node:
		node = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			klog.ErrorS(nil, "Cannot convert to *v1.Node", "obj", t.Obj)
			return
		}
	default:
		klog.ErrorS(nil, "Cannot convert to *v1.Node", "obj", t)
		return
	}
	klog.V(3).InfoS("Delete event for node", "node", klog.KObj(node))
	// NOTE: Updates must be written to scheduler cache before invalidating
	// equivalence cache, because we could snapshot equivalence cache after the
	// invalidation and then snapshot the cache itself. If the cache is
	// snapshotted before updates are written, we would update equivalence
	// cache with stale information which is based on snapshot of old cache.
	if err := sched.SchedulerCache.RemoveNode(node); err != nil {
		klog.ErrorS(err, "Scheduler cache RemoveNode failed")
	}
}

// addPodToSchedulingQueue 是一个事件处理函数，用于响应 Pod 的 "Add" 事件。
// 当一个未调度的 Pod 被创建或被当前调度器选中负责调度时，此函数会被调用。
// 它的主要作用是将新创建的 Pod 添加到调度器的调度队列中，等待后续的调度流程处理。
func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	// 将传入的 interface{} 对象断言为 *v1.Pod 类型。
	// 这个 obj 通常来自于 Pod Informer 的 Add 事件。
	pod := obj.(*v1.Pod)

	// 记录一条日志，表明有一个未调度的 Pod 被添加到了调度队列。
	// 日志级别为 V(3)，属于较详细的调试信息。
	klog.V(3).InfoS("Add event for unscheduled pod", "pod", klog.KObj(pod))

	// 调用调度队列的 Add 方法，将 Pod 添加到队列中。
	// 这通常意味着 Pod 会被放入 activeQ，使其进入可调度的候选列表。
	if err := sched.SchedulingQueue.Add(pod); err != nil {
		// 如果添加到队列失败，记录错误日志。
		// 这可能是因为队列已满、Pod 本身存在格式错误等。
		utilruntime.HandleError(fmt.Errorf("unable to queue %T: %v", obj, err))
	}
	// 如果添加成功，函数静默返回，Pod 已在队列中等待调度。
}

func (sched *Scheduler) updatePodInSchedulingQueue(oldObj, newObj interface{}) {
	oldPod, newPod := oldObj.(*v1.Pod), newObj.(*v1.Pod)
	// Bypass update event that carries identical objects; otherwise, a duplicated
	// Pod may go through scheduling and cause unexpected behavior (see #96071).
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}

	isAssumed, err := sched.SchedulerCache.IsAssumedPod(newPod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to check whether pod %s/%s is assumed: %v", newPod.Namespace, newPod.Name, err))
	}
	if isAssumed {
		return
	}

	if err := sched.SchedulingQueue.Update(oldPod, newPod); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to update %T: %v", newObj, err))
	}
}

func (sched *Scheduler) deletePodFromSchedulingQueue(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = obj.(*v1.Pod)
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, sched))
			return
		}
	default:
		utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
		return
	}
	klog.V(3).InfoS("Delete event for unscheduled pod", "pod", klog.KObj(pod))
	if err := sched.SchedulingQueue.Delete(pod); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to dequeue %T: %v", obj, err))
	}
	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		// This shouldn't happen, because we only accept for scheduling the pods
		// which specify a scheduler name that matches one of the profiles.
		klog.ErrorS(err, "Unable to get profile", "pod", klog.KObj(pod))
		return
	}
	fwk.RejectWaitingPod(pod.UID)
}

func (sched *Scheduler) addPodToCache(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.ErrorS(nil, "Cannot convert to *v1.Pod", "obj", obj)
		return
	}
	klog.V(3).InfoS("Add event for scheduled pod", "pod", klog.KObj(pod))

	if err := sched.SchedulerCache.AddPod(pod); err != nil {
		klog.ErrorS(err, "Scheduler cache AddPod failed", "pod", klog.KObj(pod))
	}

	sched.SchedulingQueue.AssignedPodAdded(pod)
}

func (sched *Scheduler) updatePodInCache(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		klog.ErrorS(nil, "Cannot convert oldObj to *v1.Pod", "oldObj", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod)
	if !ok {
		klog.ErrorS(nil, "Cannot convert newObj to *v1.Pod", "newObj", newObj)
		return
	}

	// A Pod delete event followed by an immediate Pod add event may be merged
	// into a Pod update event. In this case, we should invalidate the old Pod, and
	// then add the new Pod.
	if oldPod.UID != newPod.UID {
		sched.deletePodFromCache(oldObj)
		sched.addPodToCache(newObj)
		return
	}

	// NOTE: Updates must be written to scheduler cache before invalidating
	// equivalence cache, because we could snapshot equivalence cache after the
	// invalidation and then snapshot the cache itself. If the cache is
	// snapshotted before updates are written, we would update equivalence
	// cache with stale information which is based on snapshot of old cache.
	if err := sched.SchedulerCache.UpdatePod(oldPod, newPod); err != nil {
		klog.ErrorS(err, "Scheduler cache UpdatePod failed", "oldPod", klog.KObj(oldPod), "newPod", klog.KObj(newPod))
	}

	sched.SchedulingQueue.AssignedPodUpdated(newPod)
}

func (sched *Scheduler) deletePodFromCache(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) {
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			klog.ErrorS(nil, "Cannot convert to *v1.Pod", "obj", t.Obj)
			return
		}
	default:
		klog.ErrorS(nil, "Cannot convert to *v1.Pod", "obj", t)
		return
	}
	klog.V(3).InfoS("Delete event for scheduled pod", "pod", klog.KObj(pod))
	// NOTE: Updates must be written to scheduler cache before invalidating
	// equivalence cache, because we could snapshot equivalence cache after the
	// invalidation and then snapshot the cache itself. If the cache is
	// snapshotted before updates are written, we would update equivalence
	// cache with stale information which is based on snapshot of old cache.
	if err := sched.SchedulerCache.RemovePod(pod); err != nil {
		klog.ErrorS(err, "Scheduler cache RemovePod failed", "pod", klog.KObj(pod))
	}

	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.AssignedPodDelete, nil)
}

// assignedPod selects pods that are assigned (scheduled and running).
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// responsibleForPod returns true if the pod has asked to be scheduled by the given scheduler.
func responsibleForPod(pod *v1.Pod, profiles profile.Map) bool {
	return profiles.HandlesSchedulerName(pod.Spec.SchedulerName)
}

// addAllEventHandlers 是一个辅助函数，用于在测试和调度器主流程中，
// 为各种 Kubernetes 资源的 Informer 添加事件处理函数。
// 这些处理函数负责监听资源变化（如 Pod、Node 等的增删改），并相应地更新调度器的内部缓存和调度队列。
//
// 参数说明：
// - sched: 指向调度器实例的指针，处理函数会调用其方法来更新状态。
// - informerFactory: 共享的 Informer 工厂，用于获取标准 Kubernetes API 资源的 Informer。
// - dynInformerFactory: 动态共享 Informer 工厂，用于获取 CRD 等自定义资源的 Informer。
// - gvkMap: 一个映射，定义了哪些 GVK (GroupVersionKind) 资源需要监听以及需要监听的动作类型 (Add, Update, Delete)。
func addAllEventHandlers(
	sched *Scheduler,
	informerFactory informers.SharedInformerFactory,
	dynInformerFactory dynamicinformer.DynamicSharedInformerFactory,
	crdInformerFactory crdinformers.SharedInformerFactory,
	gvkMap map[framework.GVK]framework.ActionType,
) {
	// --- 1. 为已调度的 Pod 添加事件处理器 ---
	// 监听 Pod 变化，仅处理已分配节点的 Pod (assignedPod)。
	// 这些事件会更新调度器的 Pod 缓存 (pod cache)，用于快速查询已调度 Pod 的状态。
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					// 仅处理已分配节点的 Pod
					return assignedPod(t)
				case cache.DeletedFinalStateUnknown:
					// 处理被删除的 Pod，即使其最终状态未知。
					// 由于对象可能已过期，不检查其是否已分配，直接尝试清理。
					if _, ok := t.Obj.(*v1.Pod); ok {
						return true
					}
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, sched))
					return false
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addPodToCache,      // Pod 添加时，将其加入缓存
				UpdateFunc: sched.updatePodInCache,   // Pod 更新时，更新缓存中的记录
				DeleteFunc: sched.deletePodFromCache, // Pod 删除时，从缓存中移除
			},
		},
	)

	// --- 2. 为未调度的 Pod 添加事件处理器 ---
	// 监听 Pod 变化，仅处理未分配节点且当前调度器负责调度的 Pod。
	// 这些事件会更新调度器的调度队列 (scheduling queue)。
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					// 仅处理未分配节点且当前调度器配置 (Profiles) 负责调度的 Pod
					return !assignedPod(t) && responsibleForPod(t, sched.Profiles)
				case cache.DeletedFinalStateUnknown:
					// 处理被删除的 Pod，检查其是否是当前调度器负责的未调度 Pod。
					// 由于对象可能已过期，不检查其是否已分配，只检查负责关系。
					if pod, ok := t.Obj.(*v1.Pod); ok {
						return responsibleForPod(pod, sched.Profiles)
					}
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, sched))
					return false
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addPodToSchedulingQueue,      // Pod 添加时，将其加入调度队列
				UpdateFunc: sched.updatePodInSchedulingQueue,   // Pod 更新时，更新调度队列中的记录
				DeleteFunc: sched.deletePodFromSchedulingQueue, // Pod 删除时，从调度队列中移除
			},
		},
	)

	// --- 3. 为 Node 添加事件处理器 ---
	// 监听 Node 变化，无论 Pod 是否已分配，Node 的变化都可能影响调度决策。
	// 这些事件会更新调度器的 Node 缓存 (node cache)。
	informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addNodeToCache,      // Node 添加时，将其加入缓存
			UpdateFunc: sched.updateNodeInCache,   // Node 更新时，更新缓存中的记录
			DeleteFunc: sched.deleteNodeFromCache, // Node 删除时，从缓存中移除
		},
	)

	//-------------------------------------------------------------------------
	// ==================== Scheduler CRD 事件处理器 ====================
	// 监听 Godel Scheduler 自身配置的变更（如调度策略更新）
	crdInformerFactory.Scheduling().V1alpha1().Schedulers().Informer().AddEventHandler(
		// 使用 FilteringResourceEventHandler 只处理与当前调度器实例相关的事件
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *schedulingv1a1.Scheduler:
					// 添加日志打印 Scheduler CRD 的名称和当前调度器的名称
					klog.InfoS("比较调度器名称", "schedulerCRDName", t.Name, "currentSchedulerName", sched.Name)
					return t.Name == sched.Name // 只处理名称匹配的 Scheduler CRD
				case cache.DeletedFinalStateUnknown:
					// 处理删除事件中缓存可能不一致的情况
					if scheduler, ok := t.Obj.(*schedulingv1a1.Scheduler); ok {
						// 添加日志打印 DeletedFinalStateUnknown 中的 Scheduler 名称和当前调度器的名称
						klog.InfoS("比较调度器名称", "schedulerCRDName", scheduler.Name, "currentSchedulerName", sched.Name)
						return scheduler.Name == sched.Name
					}
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1alpha1.Scheduler in %T", obj, sched))
					return false
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				UpdateFunc: sched.onSchedulerUpdate, // Scheduler CRD 更新时触发配置重载
			},
		},
	)
	//-----------------------------------------------------------------------------------------------

	//buildEvtResHandler := func(at framework.ActionType, gvk framework.GVK, shortGVK string) cache.ResourceEventHandlerFuncs {
	//	funcs := cache.ResourceEventHandlerFuncs{}
	//	if at&framework.Add != 0 {
	//		evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Add, Label: fmt.Sprintf("%vAdd", shortGVK)}
	//		funcs.AddFunc = func(_ interface{}) {
	//			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, nil)
	//		}
	//	}
	//	if at&framework.Update != 0 {
	//		evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Update, Label: fmt.Sprintf("%vUpdate", shortGVK)}
	//		funcs.UpdateFunc = func(_, _ interface{}) {
	//			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, nil)
	//		}
	//	}
	//	if at&framework.Delete != 0 {
	//		evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Delete, Label: fmt.Sprintf("%vDelete", shortGVK)}
	//		funcs.DeleteFunc = func(_ interface{}) {
	//			sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, nil)
	//		}
	//	}
	//	return funcs
	//}
	//
	//for gvk, at := range gvkMap {
	//	switch gvk {
	//	case framework.Node, framework.Pod:
	//		// Do nothing.
	//	case framework.CSINode:
	//		informerFactory.Storage().V1().CSINodes().Informer().AddEventHandler(
	//			buildEvtResHandler(at, framework.CSINode, "CSINode"),
	//		)
	//	case framework.CSIDriver:
	//		informerFactory.Storage().V1().CSIDrivers().Informer().AddEventHandler(
	//			buildEvtResHandler(at, framework.CSIDriver, "CSIDriver"),
	//		)
	//	case framework.CSIStorageCapacity:
	//		informerFactory.Storage().V1beta1().CSIStorageCapacities().Informer().AddEventHandler(
	//			buildEvtResHandler(at, framework.CSIStorageCapacity, "CSIStorageCapacity"),
	//		)
	//	case framework.PersistentVolume:
	//		// MaxPDVolumeCountPredicate: since it relies on the counts of PV.
	//		//
	//		// PvAdd: Pods created when there are no PVs available will be stuck in
	//		// unschedulable queue. But unbound PVs created for static provisioning and
	//		// delay binding storage class are skipped in PV controller dynamic
	//		// provisioning and binding process, will not trigger events to schedule pod
	//		// again. So we need to move pods to active queue on PV add for this
	//		// scenario.
	//		//
	//		// PvUpdate: Scheduler.bindVolumesWorker may fail to update assumed pod volume
	//		// bindings due to conflicts if PVs are updated by PV controller or other
	//		// parties, then scheduler will add pod back to unschedulable queue. We
	//		// need to move pods to active queue on PV update for this scenario.
	//		informerFactory.Core().V1().PersistentVolumes().Informer().AddEventHandler(
	//			buildEvtResHandler(at, framework.PersistentVolume, "Pv"),
	//		)
	//	case framework.PersistentVolumeClaim:
	//		// MaxPDVolumeCountPredicate: add/update PVC will affect counts of PV when it is bound.
	//		informerFactory.Core().V1().PersistentVolumeClaims().Informer().AddEventHandler(
	//			buildEvtResHandler(at, framework.PersistentVolumeClaim, "Pvc"),
	//		)
	//	case framework.StorageClass:
	//		if at&framework.Add != 0 {
	//			informerFactory.Storage().V1().StorageClasses().Informer().AddEventHandler(
	//				cache.ResourceEventHandlerFuncs{
	//					AddFunc: sched.onStorageClassAdd,
	//				},
	//			)
	//		}
	//		if at&framework.Update != 0 {
	//			informerFactory.Storage().V1().StorageClasses().Informer().AddEventHandler(
	//				cache.ResourceEventHandlerFuncs{
	//					UpdateFunc: func(_, _ interface{}) {
	//						sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.StorageClassUpdate, nil)
	//					},
	//				},
	//			)
	//		}
	//	case framework.Service:
	//		// ServiceAffinity: affected by the selector of the service is updated.
	//		// Also, if new service is added, equivalence cache will also become invalid since
	//		// existing pods may be "captured" by this service and change this predicate result.
	//		informerFactory.Core().V1().Services().Informer().AddEventHandler(
	//			buildEvtResHandler(at, framework.Service, "Service"),
	//		)
	//	default:
	//		// Tests may not instantiate dynInformerFactory.
	//		if dynInformerFactory == nil {
	//			continue
	//		}
	//		// GVK is expected to be at least 3-folded, separated by dots.
	//		// <kind in plural>.<version>.<group>
	//		// Valid examples:
	//		// - foos.v1.example.com
	//		// - bars.v1beta1.a.b.c
	//		// Invalid examples:
	//		// - foos.v1 (2 sections)
	//		// - foo.v1.example.com (the first section should be plural)
	//		if strings.Count(string(gvk), ".") < 2 {
	//			klog.ErrorS(nil, "incorrect event registration", "gvk", gvk)
	//			continue
	//		}
	//		// Fall back to try dynamic informers.
	//		gvr, _ := schema.ParseResourceArg(string(gvk))
	//		dynInformer := dynInformerFactory.ForResource(*gvr).Informer()
	//		dynInformer.AddEventHandler(
	//			buildEvtResHandler(at, gvk, strings.Title(gvr.Resource)),
	//		)
	//		go dynInformer.Run(sched.StopEverything)
	//	}
	//}
}

func nodeSchedulingPropertiesChange(newNode *v1.Node, oldNode *v1.Node) *framework.ClusterEvent {
	if nodeSpecUnschedulableChanged(newNode, oldNode) {
		return &queue.NodeSpecUnschedulableChange
	}
	if nodeAllocatableChanged(newNode, oldNode) {
		return &queue.NodeAllocatableChange
	}
	if nodeLabelsChanged(newNode, oldNode) {
		return &queue.NodeLabelChange
	}
	if nodeTaintsChanged(newNode, oldNode) {
		return &queue.NodeTaintChange
	}
	if nodeConditionsChanged(newNode, oldNode) {
		return &queue.NodeConditionChange
	}

	return nil
}

func nodeAllocatableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable)
}

func nodeLabelsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.GetLabels(), newNode.GetLabels())
}

func nodeTaintsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints)
}

func nodeConditionsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	strip := func(conditions []v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	return !reflect.DeepEqual(strip(oldNode.Status.Conditions), strip(newNode.Status.Conditions))
}

func nodeSpecUnschedulableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable
}

func preCheckForNode(nodeInfo *framework.NodeInfo) queue.PreEnqueueCheck {
	// In addition to the checks in kubelet (pkg/kubelet/lifecycle/predicate.go#GeneralPredicates),
	// the following logic appends a taint/toleration check.
	// TODO: verify if kubelet should also apply the taint/toleration check, and then unify the
	// logic with kubelet and move to a shared place.
	//
	// Note: the following checks doesn't take preemption into considerations, in very rare
	// cases (e.g., node resizing), "pod" may still fail a check but preemption helps. We deliberately
	// chose to ignore those cases as unschedulable pods will be re-queued eventually.
	return func(pod *v1.Pod) bool {
		if len(noderesources.Fits(pod, nodeInfo, feature.DefaultFeatureGate.Enabled(features.PodOverhead))) != 0 {
			return false
		}

		// Ignore parsing errors for backwards compatibility.
		matches, _ := nodeaffinity.GetRequiredNodeAffinity(pod).Match(nodeInfo.Node())
		if !matches {
			return false
		}

		if !nodename.Fits(pod, nodeInfo) {
			return false
		}

		if !nodeports.Fits(pod, nodeInfo) {
			return false
		}

		_, isUntolerated := v1helper.FindMatchingUntoleratedTaint(nodeInfo.Node().Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			// PodToleratesNodeTaints is only interested in NoSchedule and NoExecute taints.
			return t.Effect == v1.TaintEffectNoSchedule || t.Effect == v1.TaintEffectNoExecute
		})
		return !isUntolerated
	}
}
