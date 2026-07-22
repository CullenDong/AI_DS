# Redshift → 本地/S3 数据同步(SS03 cohort)

从生产 Redshift(`slot-machine` 库 `public.fct_bet_orders`)按天抽取数据,落地为日分区
parquet,并可镜像上传到 `s3://bituslabs-team-ai/SS03_raw_data/`。

## 文件

| 文件 | 角色 |
|---|---|
| `tools/db/redshift.py` | 连接层:**只读** backend(正则拦截写操作)+ paramiko SSH 堡垒机隧道 + `.env` 自动加载 + 连接缓存 |
| `jobs/sync_ss03.py` | 全表按天同步 → 本地 `data/ss03/dt=YYYY-MM-DD/part.parquet`(断点续跑) |
| `jobs/sync_ss03_s3.py` | 全表按天同步 → S3(先本地后上传,`--upload-only` / `--purge-local`) |
| `jobs/sync_ss03_cohorts.py` | **配置驱动的 cohort 同步**(主力入口):`COHORTS` 列表定义 folder/日期/过滤,每个 cohort 一个 S3 folder |
| `jobs/sync_ss03_ai_groups.py` | 并行第二任务示例:独立隧道端口 5434 |

## 环境

`.env`(gitignored):`REDSHIFT_USER` / `REDSHIFT_PASSWORD` / `BASTION_KEY_PATH=~/.ssh/oceanhunter-prod-bastion-ec2.pem`。
默认 host `production-redshift-cluster...ap-southeast-1:5439`,堡垒机 `13.215.212.244`。
依赖:`redshift_connector paramiko python-dotenv boto3 pyarrow`;S3 写权限走 `~/.aws/credentials`。

## 新 cohort 标准流程

1. **发现查询**(用户口头描述 → 确切 ID,绝不直接信记忆):
   `SELECT partition_ab[0]::varchar, math_table_id, count(*) ... GROUP BY 1,2`,把同组存在的
   数学表列给用户确认排除项。SUPER 列做 `LIKE` 必须 `::varchar`,等值比较可不转。
2. `COHORTS` 加一项:`folder`(命名=组+表+日期,如 `AI_group_4.2-4.22`)、`start/end`(含两端)、
   `extra_where`(在 `BASE_WHERE` 之上,保留用户 SQL 语义)。
3. 后台运行 `python3 jobs/sync_ss03_cohorts.py`。断点续跑:S3 上已有的天跳过;Redshift 懒连接。
4. 每天:查询(UTC 日切片)→ 本地 parquet → 上传 → **字节校验**。
5. 收尾核验:每 folder 天数 / 本地 vs S3 字节 / 行数汇总,汇报表格。

## 布局

```
s3://bituslabs-team-ai/SS03_raw_data/<cohort_folder>/dt=YYYY-MM-DD/part.parquet
本地镜像 data/ss03/<cohort_folder>/dt=.../part.parquet   (data/ gitignored)
```

## 已同步 cohort(截至 2026-07)

| folder | 条件 | 行数 |
|---|---|---|
| `AI_group_4.2-4.22` | AI组 `jojpin-9mokha-rexQug`,全部数学表 | 496K |
| `AB_TEST_B_normal_zero_95_kai_6.10-7.8` | `4f1a46ca-…` + `normal_zero_95_kai` | 2.32M |
| `AB_TEST_A_normal_Zero_BG97_Saitekika_BGadj_6.10-7.8` | `4a04df21-…` + BG97 表 | 1.57M |
| `AI_group_v3.2_5.22-6.16` / `AI_group_v4_6.18-7.9` | AI组 Pre/Post v4 | 579K / 414K |
| `default_normal_zero_6.10-7.5` | default组 `wytsuj-fothap-5Qixda` + `normal_zero`(排除 `normal_kakuteiB`) | ~4.7M |

## 坑

- **并行任务必须用不同本地隧道端口**(5433/5434/5435…),直接构造 `RedshiftBackend(..., local_port=N)`。
- 全表一天 ~670K 行 / ~6 分钟(隧道瓶颈);过滤 cohort 秒~分钟级。多 cohort 回填按小时计,务必后台。
- 秘钥安全:`.env` 不进库、密码不进对话记录。
