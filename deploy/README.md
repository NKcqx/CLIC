# 微服务部署手册，按照下面的顺序部署系统依赖的服务


# 版本更新

更新对应deployment的镜像即可
kubectl set image deployment/clic-scheduler-center edwardtang/scheduler-center=edwardtang/scheduler-center:1.0--record


