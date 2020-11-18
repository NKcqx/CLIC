package main

import (
	"kubernetes-scheduler/pkg/qos"
	"math/rand"
	"os"
	"time"

	"github.com/spf13/pflag"

	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version" // for version metric registration
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

// 自定义调度插件的启动类
func main() {
	rand.Seed(time.Now().UnixNano())

	// 注册调度插件，需要两个参数，一个是插件名，一个是一个 PluginFactory（一个func）
	command := app.NewSchedulerCommand(
		app.WithPlugin(qos.Name, qos.New),
	)

	pflag.CommandLine.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	logs.InitLogs()
	defer logs.FlushLogs()

	if err := command.Execute(); err != nil {
		os.Exit(1)
	}
}
