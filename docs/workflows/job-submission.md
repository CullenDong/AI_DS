# 远程任务提交脚手架(SageMaker / SSM)

早期搭的远程执行脚手架,当前工作流(本地 duckdb + 堡垒机直连)未使用,保留备用。

## 文件

| 文件 | 角色 |
|---|---|
| `jobs/sagemaker_run/run_submit.py` | 提交 SageMaker 任务 |
| `jobs/sagemaker_run/run_entry.py` | 任务容器内入口 |
| `jobs/sagemaker_run/requirements.txt` | 任务环境依赖 |
| `jobs/ssm_run/ssm_submit.py` | 经 AWS SSM 在远程实例执行命令 |

## 何时用

- 单机内存/磁盘扛不住(当前 1.2 亿行/天在 6GB 内可跑,尚未触顶)
- 需要靠近数据的区域执行(如 ap-southeast-1 直连 Redshift 免隧道瓶颈,全表日同步 ~6 分钟/天
  的隧道开销可省)
