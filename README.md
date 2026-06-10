# 云计算技术课程设计 · group17

> 学号 2023112594　姓名 陶秋源　班级 计算机科学与技术04班
> 华为云 CCE + SWR + OBS + ELB ｜ Region: cn-north-4

基于华为云 CCE 搭建 Kubernetes 平台，部署「Flask + Redis」两层 Web 应用，并在其上运行
Spark 大数据分析作业；附加完成监控（Prometheus+Grafana）、CI/CD、分布式 AI 训练（PyTorch DDP）。

## 目录结构

```
.
├── backend/                 # 任务1 Flask 后端（多阶段 Dockerfile，自选包 requests）
├── frontend/                # 任务1 Nginx 前端（index.html 含学号姓名）
├── docker-compose.yml       # 任务1 本地联调
├── k8s/                     # 任务2-6 K8s 清单
│   ├── 01-configmap.yaml ~ 06-backend-service.yaml   # 任务3 部署 + ELB
│   ├── 07/08 redis PVC      # 任务4 持久化
│   ├── 09~12 frontend       # 任务5 ConfigMap Volume 挂载
│   ├── 13-hpa.yaml          # 任务6 HPA 弹性伸缩
│   └── metrics-server.yaml  # HPA 依赖
├── spark/                   # 第二部分A：wordcount + 清洗 + SQL + 性能对比
├── monitoring/              # 附加题1：kube-prometheus-stack values
├── ai/                      # 附加题3：PyTorch DDP 训练（单机 vs 2 worker）
├── .github/workflows/       # 附加题2：CI/CD 流水线
└── scripts/                 # OBS 上传、HPA 压测等辅助脚本
```

## 关键信息

| 项目 | 值 |
|---|---|
| Region | cn-north-4 |
| SWR 组织 | group17 |
| OBS 桶 | group17（s3a://group17/data/douban_movies.csv） |
| 后端 ELB | http://1.94.238.112/api/ping |
| 镜像前缀 | swr.cn-north-4.myhuaweicloud.com/group17/ |

详见实验报告 PDF。
