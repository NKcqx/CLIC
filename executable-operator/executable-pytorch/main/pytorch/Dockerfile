# 暂时只能将环境手动打包
# 需要先安装conda-pack，conda install -c conda-forge conda-pack
# 再使用conda pack -n CLIC打包成tar压缩文件
FROM continuumio/miniconda3:4.9.2
WORKDIR /CLIC_Pytorch
COPY /executable-basic-python/* .
COPY /executable-pytorch/* .
#COPY /executable-pytorch/resources/* .
RUN mkdir CLIC-Conda && tar -zxvf CLIC.tar.gz -C CLIC-Conda
RUN ["/bin/bash", "-c", "source CLIC-Conda/bin/activate"]
WORKDIR /CLIC_Pytorch/pytorch