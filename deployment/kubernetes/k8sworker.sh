#!/bin/bash

# 先执行k8sinstall

# 关闭防火墙以及交换区
sudo swapoff -a
sudo ufw disable

# 加入集群，这里的地址是master的地址，token是执行master的token
sudo kubeadm join 10.176.24.160:6443 --token pnr2v7.bcojthd92v8e4uov \
--discovery-token-ca-cert-hash sha256:91401bd2b5bd43a13b132658813c1e8f157295bbad99ce167d76d0ab2f340a35

mkdir .kube
# 复制api-server配置文件到本地，由于权限，去其他服务器
sudo scp /etc/kubernetes/admin.conf tzw@10.176.24.162:$HOME/.kube/config
# 本机
sudo cp -i $HOME/.kube/config /etc/kubernetes/admin.conf
echo "export KUBECONFIG=$HOME/.kube/config" >> ~/.bash_profile
source ~/.bash_profile