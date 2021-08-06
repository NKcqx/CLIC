#!/bin/bash

# 本脚本包含k8s的基本环境，需要在所有节点运行
# 安装docker
sudo apt-get update
sudo apt-get install -y docker.io
sudo gpasswd -a ${USER} docker

newgrp docker

# 安装k8s
cat <<EOF > /etc/apt/sources.list.d/kubernetes.list
deb http://mirrors.ustc.edu.cn/kubernetes/apt kubernetes-xenial main
EOF

apt-get update && apt-get install -y docker.io kubelet kubernetes-cni kubeadm

# 替换key的后8位
gpg --keyserver keyserver.ubuntu.com --recv-keys BA07F4FB
gpg --export --armor BA07F4FB | sudo apt-key add -

kubeadm config images list

# 从阿里云拉取镜像，这里的版本需要和上面保持一致
images=(  # 下面的镜像应该去除"k8s.gcr.io/"的前缀，版本换成上面获取到的版本
    kube-apiserver:v1.18.8
    kube-controller-manager:v1.18.8
    kube-scheduler:v1.18.8
    kube-proxy:v1.18.8
    pause:3.2
    etcd:3.4.3-0
    coredns:1.6.7
)

for imageName in ${images[@]} ; do
    docker pull registry.cn-hangzhou.aliyuncs.com/google_containers/$imageName
    docker tag registry.cn-hangzhou.aliyuncs.com/google_containers/$imageName k8s.gcr.io/$imageName
    docker rmi registry.cn-hangzhou.aliyuncs.com/google_containers/$imageName
done