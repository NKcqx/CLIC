#!/bin/bash

# 本脚本包含k8s的基本环境，需要在所有节点运行
# 安装docker
sudo apt-get update
sudo apt-get install -y docker.io
sudo gpasswd -a ${USER} docker

newgrp docker

# 添加k8s的源
vim /etc/apt/sources.list.d/kubernetes.list 
# 添加一行
deb http://mirrors.ustc.edu.cn/kubernetes/apt kubernetes-xenial main

sudo apt-get update && sudo apt-get install -y docker.io kubelet kubernetes-cni kubeadm

# 上述命令会报错，需要修改key，替换key为上面显示的key的后8位
gpg --keyserver keyserver.ubuntu.com --recv-keys BA07F4FB
gpg --export --armor BA07F4FB | sudo apt-key add -

kubeadm config images list

# 从阿里云拉取镜像，这里的版本需要和上面保持一致
images=(  # 下面的镜像应该去除"k8s.gcr.io/"的前缀，版本换成上面获取到的版本
    kube-apiserver:v1.20.4
    kube-controller-manager:v1.20.4
    kube-scheduler:v1.20.4
    kube-proxy:v1.20.4
    pause:3.2
    etcd:3.4.13-0
    coredns:1.7.0
)

for imageName in ${images[@]} ; do
    docker pull registry.cn-hangzhou.aliyuncs.com/google_containers/$imageName
    docker tag registry.cn-hangzhou.aliyuncs.com/google_containers/$imageName k8s.gcr.io/$imageName
    docker rmi registry.cn-hangzhou.aliyuncs.com/google_containers/$imageName
done
