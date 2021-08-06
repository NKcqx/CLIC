#!/bin/bash

# 先执行k8sinstall

# 关闭防火墙以及交换区
sudo swapoff -a
sudo ufw disable

# 初始化api-server，这里的地址应该是master的ip地址
sudo kubeadm init --apiserver-advertise-address=10.176.24.160

# 复制配置到用户目录
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

# 安装dns插件weave net插件
sudo sysctl net.bridge.bridge-nf-call-iptables=1
export kubever=$(kubectl version | base64 | tr -d '\n')
kubectl apply -f "https://cloud.weave.works/k8s/net?k8s-version=$kubever"

