# 暂时只能将环境手动打包
# 需要先安装conda-pack，conda install -c conda-forge conda-pack
# 再使用conda pack -n CLIC打包成tar压缩文件
FROM continuumio/miniconda3:4.9.2
WORKDIR /CLIC_Pytorch
COPY /executable-basic-python/* ./
COPY /executable-pytorch/* ./
RUN mkdir CLIC-Conda && tar -zxvf CLIC.tar.gz -C CLIC-Conda && rm -rf CLIC.tar.gz
RUN echo 'export PYTHONPATH=$PYTHONPATH:/CLIC_Pytorch' >> /root/.bashrc \
 && echo 'source /CLIC_Pytorch/CLIC-Conda/bin/activate' >> /root/.bashrc
WORKDIR /CLIC_Pytorch/pytorch

#CMD ["/bin/bash","-c", "source /CLIC_Pytorch/CLIC-Conda/bin/activate"]
#SHELL ["/bin/bash", "-c"]
#RUN source CLIC-Conda/bin/activate \
# && conda init bash \
# && source /root/.bashrc
#RUN ["conda", "init", "bash"]
#RUN ["conda", "activate", "CLIC-Conda"]
#RUN ["/bin/bash", "-c", "source /root/.bashrc"]
