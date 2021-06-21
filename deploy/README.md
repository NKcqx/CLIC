微服务部署手册，按照下面的顺序部署系统依赖的服务

## 前置部署

- 1.系统依赖nfs（或者其他分布式存储）进行部署，因此需要先部署nfs，再部署k8s访问的pv和pvc
   - nfs的部署脚本在nfs路径下
   - nfs的pv和pvc可以根据情况更改（比如挂载路径）
   - 需要在挂载路径下创建我们需要的文件夹：
     - 目前元信息存放位置：挂载目录为 /nfs/data
        - dag：/data/system/dags
        - 中间结果： /data/system/inter_files
     - 用户的udf和输入文件，建议存放到 /data/user
   
- 2.系统需要依赖k8s的高权限的service account的token去创建job、operator。下面直接获取了kube-system的admin的权限，
    实际并不太好，只是为了方便，最好自己根据需求创建对应的sa，然后获取token

  ```shell script
    # 获取最高权限的token，后面executor-center的configmap的配置token需要使用这一个token
    $(kubectl get secret $(kubectl get serviceaccount admin -n kube-system -o jsonpath='{.secrets[0].name}') -n kube-system -o jsonpath='{.data.token}' | base64 --decode )
  ```
  
- 3.系统底层需要不同的执行引擎，比如spark、flink、tensorflow，需要先在k8s上安装对应的环境的operator
    - 参考网络的解决方案，建议使用helm的方式

## 微服务部署

每一个微服务包含三个部分，分别是config、deployment、service。所有平台部署均使用CRD的方式，本仓库维护了基本的部署文件。
其中：config保存了spring的生产环境的配置文件的configmap、deployment是部署的文件、service是对外的服务。
依次执行每一个微服务应用的config、deployment、service即可。
- 1.创建config：`kubectl create -f xxx-config.yaml`
- 2.创建deployment: `kubectl create -f xxx.yaml`
- 3.创建service：`kubectl create -f xxx-svc.yaml`
- 当需要更改config的时候，直接通过`kubectl edit configmap xxx`来更新对应的配置

## 版本更新

更新对应deployment的镜像即可
kubectl set image deployment/clic-job-center job-template=edwardtang/job-center:micro-service-rebuild --record

## kubernetes的服务发现（dns）经常出现问题

原因：
- 1.如果k8s的版本较高，需要将coredns升级到新版本，见[链接](https://blog.csdn.net/heian_99/article/details/114950602)
- 2.如果Node上安装的Docker版本大于1.12，那么Docker 会把默认的 iptables FORWARD 策略改为 DROP。
    这会引发 Pod 网络访问的问题。解决方法则在每个 Node 上面运行 iptables -P FORWARD ACCEPT:
    
    一劳永逸的方法
   ```shell script
    echo "ExecStartPost=/sbin/iptables -P FORWARD ACCEPT" >> /etc/systemd/system/docker.service.d/exec_start.conf
    systemctl daemon-reload
    systemctl restart docker
    ```
    如何docker正在运行，或者直接执行：`/sbin/iptables -P FORWARD ACCEPT`，然后按照上面命令重启docker
- 3.iptable需要设置，见[链接](https://imroc.cc/post/202105/why-enable-bridge-nf-call-iptables/)
- 4.检查防火墙是否关闭：
    ```shell script
    sudo swapoff -a
    sudo ufw disable
    ```

## 常用
- 重启pod的方法
kubectl get pod coredns-74ff55c5b-s7qkb -n kube-system -o yaml | kubectl replace --force -f -

## 平台环境部署

推荐使用 operator的方式部署

