# Workflows 索引

每个工作流一份文档,覆盖仓库内全部 Python 模块。

| 文档 | 工作流 | 主要文件 |
|---|---|---|
| [redshift-s3-sync.md](redshift-s3-sync.md) | Redshift → 本地/S3 数据同步(SS03 cohort) | `tools/db/redshift.py`, `jobs/sync_ss03*.py` |
| [ss03-analysis.md](ss03-analysis.md) | SS03 日报 / before-after / AB 策略与数学表对比 | `jobs/ss03_*.py` |
| [superace-bgfg-inference.md](superace-bgfg-inference.md) | Super Ace BG/FG 推断(倍率+停顿法,旧投注级数据) | `tools/superace/sa_fg_*.py` |
| [fortune-gems-distribution.md](fortune-gems-distribution.md) | Fortune Gems 倍率分布(Normal/Extra 分离) | `tools/fortune_gems/fg_month_distribution.py` |
| [game-top20-reports.md](game-top20-reports.md) | 游戏运营 Top20 Excel 报告(含联网研究) | `tools/reports/build_*_report.py` |
| [ds-agent.md](ds-agent.md) | Claude 数据科学 Agent 脚手架 | `agent/`, `tools/agent_tools/registry.py` 等 |
| [notebook-kb.md](notebook-kb.md) | Notebook 知识库(索引+LLM摘要) | `notebook_kb/` |
| [job-submission.md](job-submission.md) | 远程任务提交脚手架(SageMaker / SSM) | `jobs/sagemaker_run/`, `jobs/ssm_run/` |

相关 skill(供 Claude Code 自动调用):`.claude/skills/redshift-s3-sync`、`.claude/skills/game-bet-eda`。
