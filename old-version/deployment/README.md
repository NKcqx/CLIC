# 部署和上线文档

本文档用于收敛所有部署环境和开发测试流程的文档

## 部署文档 （不断更新中）

部署环境包含几个部分，Kubernetes环境、存储和计算所依赖的环境（spark、hdfs等）、CLIC环境（master等）。
部署环境尽量要求脚本化和声明式，降低运维成本

<b>注意，下面运行的脚本并未实现完全的脚本化，只是为了方便统一收敛到一个文件上，实际的执行过程都需要人为地干预</b>

未来几个问题需要解决：

- 减少nfs的依赖，jar包自动打包成image并推送到远端registry，由各个节点拉取
- 配置文件和程序解耦，系统的所有配置文件均通过configmap挂载，避免修改配置就需要重新打包
- 脚本化和自动化，包括各个安装脚本，helm自动化等
- 分布式环境均通过operator的方式进行安装，

### Kubernetes环境

1. 在master节点部署Kubernetes的master，执行kubernetes的k8smaster.sh脚本（手动依次执行，目前脚本有问题需要人为干预）
2. 在各个非master节点上部署其他节点，执行kubernetes中的k8sworker.sh脚本（手动依次执行，目前脚本有问题需要人为干预）

为了做到环境隔离，创建一个namespace，下面所有的部署均放到namespace中。由于历史原因，本系统的环境均安装在argo的namespace中

```shell script
kubectl create namespace argo
```

### Nfs环境

目前nfs的环境是为了存储一些用户信息和jar包方便测试，比如udf的class文件、小文本的数据等。长期来看，nfs只会存储一些系统必要的数据

TODO: 数据等迁移，减少对于nfs的依赖，这是个长期工作，目前为了测试方便，短期内仍然通过nfs存储一些数据信息
- 所有数据文件统一迁移到hdfs中存储
- 用户信息（比如udf等）通过shell提交，然后保存在redis等统计的元数据存储上
- 所有生成的文本（workflow的yaml文件等），也通过redis存储进行读写

1. 需要设置nfs的服务器和客户端，并设置共享路径，分别在nfs-server上执行nfs-server.sh 以及在其他nfs-client上执行nfs-client.sh
<b>部署nfs挂载路径的时候，记录下nfs-server的参数 以及 对应的挂载目录</b>
2. 配置nfs的pv和pvc，文件在nfs目录下，注意修改pv-nfs中的路径
```shell script
kubectl create -f pv-nfs.yaml -n argo
kubectl create -f pvc-nfs.yaml -n argo
```

### Redis环境

redis用于存储一些元信息，目前主要是master的一些状态信息，未来和用户有关的信息也会存储在redis中。目前redis部署只使用一个单机

1. 创建redis的config map，其中保存redis的用户名和端口等信息，必要时可以修改，注意其中的pid文件是挂载在nfs上的
```shell script
kubectl create -f redis-config.yaml -n argo
```
2. 创建redis的deployment，为一个副本，为了方便访问，再创建一个service暴露给内部用户
```shell script
kubectl create -f redis-deploy.yaml -n argo
kubectl create -f redis-svc.yaml -n argo
```
3. 测试：进入redis的一个pod，然后连接到service上测试
```shell script
kubectl get pods -n argo | grep redis # 查看redis的pod名称
kubectl exec -it -n argo redis-fdb58c9c6-kxnhw -- /bin/sh # 进入该pod内部
redis-cli -h redis-svc -p 6379 -a 123456 # 连接到该svs上测试
```

### Hdfs环境

hdfs用于存储实际的数据，这里每一个物理节点都部署一个hdfs的数据节点。这里使用helm直接安装

1. 安装helm，每一个node都需要安装
```shell script
curl https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash
```

2. 直接使用helm安装hdfs，详细参数列表见README.md
```shell script
helm install hdfs ./hdfs -n argo \
	--set ingress.httpfs.enabled=true
```

3. 测试：测试方法两种，一种直接通过暴露出来的httpfs测试，一种进入pod内部测试
- httpfs测试
```shell script
# 暴露httpfs的service到localhost
kubectl port-forward -n argo hdfs-httpfs-5948cc587c-gqxcw    14000:14000
# 查看文件
curl -i -L  http://localhost:14000/v1/data?op=OPEN
# 创建文件夹
curl -i -X PUT  http://localhost:14000/webhdfs/v1/data?op=mkdirs\&user.name=root
# list文件夹
curl -i  http://localhost:14000/webhdfs/v1/data?op=liststatus\&user.name=root
# 上传本地文件
curl -i -X PUT -T ./largeTest.csv "http://localhost:14000/webhdfs/v1/data/largeTest.csv?op=CREATE&data=true&user.name=root" -H "Content-Type:application/octet-stream"
```
- dfs测试
```shell script
# 进入namenode内部
kubectl exec -n argo -it hdfs-namenode-0 /bin/bash
# 创建文件夹
hdfs dfs -makir /data/demo/data/count
# 上传文件
hdfs dfs -put /data/demo/data/count /test/info/demo/count
# list文件
hdfs dfs -ls /test/info/demo
# 删除文件
hdfs dfs -rm -r -skipTrash  /test/info/demo/count
```

### Spark环境

spark等是底层实际的执行引擎，
TODO ===> 为了配合底层的调度引擎，两种选择：
- 改成直接创建pod的方式，相当于拉平，这种实现可能需要依赖于spark operator的实现
- 目前的方式，会创建一个job提交spark集群任务，因此调度相当于归spark本身管理，而无法被kubernetes调度

1. 直接使用helm安装即可，详细参数见README.md
```shell script
helm install -n argo spark ./spark-v2 \
--set worker.replicaCount=3 \
--set image.tag=2.4.5 \
--set master.webPort=8081 
```
2. 测试: 进入spark集群的某一个pod，去提交一个测试任务
```shell script
# 进入k8s集群的spark的pod
kubectl exec -n argo -it spark-master-87b49cc46-rrvq2 /bin/bash
# 调用bin/spark-submit提交
bin/spark-submit \
    --master spark://spark-master-svc:7077 \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --name spark-pi \
    --conf spark.kubernetes.namespace=argo \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=5 \
    --conf spark.kubernetes.container.image=spark:v0 \
    local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar
```

### Clic-master环境

Clic-master是CLIC系统中调度和资源管理的核心，用于取代之前的argo workflow。

- 目前阶段：需要将clic-master打包成jar，然后放到jars目录下即可，只是为了方便测试，省去了多个image同步的问题
- 正常做法：TODO: 需要将clic-master打包成jar之后，然后构建image，并推送到远端的registry，再由远端分别拉取

1. 创建clic-master，其中clic-master的jar是挂载到nfs目录下的，未来这里可能需要改变
```shell script
kubectl create -f clic-master.yaml -n argo
```
2. 创建一个svc，暴露7777端口给用户和底层的pod
```shell script
kubectl create -f clic-master-svc.yaml -n argo
```

###clic-shell环境
clic-shell为用户提供一个与clic交互的方式，用户可以使用clic-shell去提交任务、查看任务状态等。
为用户提供的命令如下所示：
```shell script
clic  -submit <planName> <planDagPath>         #通过提交yaml的方式提交一个任务
clic  -submit <planJarPath>                    #通过提交jar的方式提交一个任务
clic  -list_task                               #获得所有任务的列表
clic  -task <taskName>                         #通过指定taskName获取相关任务的信息
clic  -task_stage <taskName>                   #通过指定taskName获取相关task的所有stage信息
clic  -stage <stageId>                         #通过指定stageId获取相关satge的信息
clic  -stage_result <stageId> [<lineNum>]      #通过指定stageId获取相关satge的结果，lineNum用于指定显示的行数，默认行数为10.
clic  -suspend_stage <stageId>                 #暂停某个指定是stage(包括所有依赖该stage的其他stage)
clic  -continue_stage <stageId>                #继续某个指定是stage(包括所有依赖该stage的其他stage)
clic  -version                                 #打印version信息
clic  -help                                    #打印使用信息
```
部署方式：
1. 目前阶段：需要将clic-shell模块打包成jar，放到jars目录下即可，然后在集群启动一个常驻pod作为交互端；
2. 将clic.sh放在用于交互端集群pod下的/bin目录下。
   注意：clic脚本在clic-shell模块的shell目录下，将其移动到相关目录下即可（pod可访问即可）。
3. 在clic-shell脚本可读的conf目录下放置master-info.config配置文件：
```shell script
#供clic-shell使用
# Clic-Master的thrift服务的ip
ip=clic-master-svc
# Clic-Master的thrift服务的port
port=7777
```
4.通过上面的命令去使用clic-shell查看信息。注意：需要clic-master等已部署完成。




## 问题修复

### 重启集群

有时候某个node可能存在not ready的情况，一般情况下，重启该节点的代理（kubelet）即可

```shell script
sudo swapoff -a
sudo systemctl daemon-reload
sudo systemctl restart kubelet
```

### 常见命令

```shell script
# 查看pod状态
kubectl get pods -n argo -o wide
# 查看日志
kubectl logs -n argo PODNAME
# 查看容器状态
kubectl describe pods -n argo PODNAME
# 进入pod内部查看
kubectl exec -it -n argo PODNAME -- /bin/sh
# 批量删除job
kubectl get jobs -n argo | grep Evicted | awk '{print $2}' | xargs kubectl delete jobs -n argo
```

## 上线文档 

### 前期
前期为了方便测试，上线的jar包并不直接打包成image，而是通过挂载目录的方式完成，实际的目录为/nfs/data/jars，并修改jar包的名称即可

### 后期
本地打包jar，并使用maven docker插件打包成image，同时提交给远程的私有registry。然后集群中的各个节点，分别拉取image即可
