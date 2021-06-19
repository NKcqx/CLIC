# 微服务部署手册，按照下面的顺序部署系统依赖的服务

## 微服务部署

所有平台部署均使用CRD的方式，本仓库维护了基本的部署文件

 版本更新

更新对应deployment的镜像即可
kubectl set image deployment/clic-job-center job-template=edwardtang/job-center:micro-service-rebuild --record

sudo swapoff -a
sudo systemctl daemon-reload
sudo systemctl restart kubelet
sudo systemctl restart kube-proxy


kubernetes的服务发现（dns）经常出现问题，原因未知？

如果 Node 上安装的 Docker 版本大于 1.12，那么 Docker 会把默认的 iptables FORWARD 策略改为 DROP。这会引发 Pod 网络访问的问题。解决方法则在每个 Node 上面运行 iptables -P FORWARD ACCEPT，比如


echo "ExecStartPost=/sbin/iptables -P FORWARD ACCEPT" >> /etc/systemd/system/docker.service.d/exec_start.conf
systemctl daemon-reload
systemctl restart docker

重启pod
kubectl get pod coredns-74ff55c5b-s7qkb -n kube-system -o yaml | kubectl replace --force -f -

## 平台环境部署

推荐使用 operator的方式部署




