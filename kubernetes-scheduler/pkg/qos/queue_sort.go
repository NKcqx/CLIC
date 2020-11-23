package qos

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/v1/pod"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	framework "k8s.io/kubernetes/pkg/scheduler/framework/v1alpha1"
)

// 测试一个简单的插件的实现逻辑，基于qos，也就是服务质量

/*
	保证如下顺序，copy from https://github.com/kubernetes-sigs/scheduler-plugins/tree/master/pkg/qos
	只作为基本的测试
	Guaranteed (requests == limits)
	Burstable (requests < limits)
	BestEffort (requests and limits not set)
 */

// 插件名称，用于注册和配置
const Name = "QOSSort"

// 插件定义
type Sort struct{}

// 初始化方法
func New(_ runtime.Object, _ framework.FrameworkHandle) (framework.Plugin, error) {
	return &Sort{}, nil
}

var _ framework.QueueSortPlugin = &Sort{}

// 需要实现以下两个方法
// Plugin的Name方法
func (pl *Sort) Name() string {
	return Name
}

// QueueSortPlugin中的Less方法
func (pl *Sort) Less(pInfo1, pInfo2 *framework.QueuedPodInfo) bool {
	p1 := pod.GetPodPriority(pInfo1.Pod)
	p2 := pod.GetPodPriority(pInfo2.Pod)
	return (p1 > p2) || (p1 == p2 && compQOS(pInfo1.Pod, pInfo2.Pod))
}

// 实现比较的基本逻辑
func compQOS(p1, p2 *v1.Pod) bool {
	p1QOS, p2QOS := v1qos.GetPodQOS(p1), v1qos.GetPodQOS(p2)

	if p1QOS == v1.PodQOSGuaranteed {
		return true
	}
	if p1QOS == v1.PodQOSBurstable {
		return p2QOS != v1.PodQOSGuaranteed
	}
	return p2QOS == v1.PodQOSBestEffort
}


