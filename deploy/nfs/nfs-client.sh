#!/bin/bash

sudo apt-get install nfs-common
sudo mkdir -p /nfs/data/
sudo chmod -R 777 /nfs/data/
sudo mount 192.168.10.3:/nfs/data/ /nfs/data/