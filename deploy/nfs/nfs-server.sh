#!/bin/bash

# nfs server
sudo apt install nfs-kernel-server
sudo mkdir -p /nfs/data/
sudo chmod -R 777 /nfs/data/
sudo vim /etc/exports # 需要手动编辑并添加nfs的挂载目录
# /nfs/data *(rw,no_root_squash,sync)
sudo exportfs -a
sudo systemctl restart nfs-kernel-server