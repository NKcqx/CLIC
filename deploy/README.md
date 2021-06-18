# 微服务部署手册，按照下面的顺序部署系统依赖的服务

## 微服务部署

所有平台部署均使用CRD的方式，本仓库维护了基本的部署文件

 版本更新

更新对应deployment的镜像即可
kubectl set image deployment/clic-job-center job-template=edwardtang/job-center:micro-service-rebuild --record

## 平台环境部署

推荐使用 operator的方式部署




