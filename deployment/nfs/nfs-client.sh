#!/bin/bash

sudo apt-get install nfs-common
sudo mkdir -p /nfs/data/
sudo chmod -R 777 /nfs/data/
sudo mount 10.176.24.160:/nfs/data/ /nfs/data/