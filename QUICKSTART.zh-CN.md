# KVFlow 快速上手（中文）

KVFlow 是一套“通用 Agent 开发工作流”：把一句需求变成可审计的开发过程——注册项目 →
选择模板 → 编译计划 → 派发 Worker → Manager 评审 → 集成 → 记录知识 → 出报告。
核心不包含任何 KVStock/量化/路径/端口/模型名的硬编码，在没有 KVStock 的机器上也能跑通。

## 1. 安装（把插件接到 DSH 宿主）

```powershell
# 查看当前的宿主集成状态
kvflow host status --profile desktop

# 安装：写入 profile 的 bundle + cordis 行，自动备份并校验宿主能否合成配置
kvflow install --profile desktop `
  --python C:\path\to\python.exe `
  --python-path C:\path\to\kvflow\src `
  --home C:\Users\<你>\.kvflow

# 先看会改什么（不写任何文件）
kvflow install --profile desktop --dry-run

# 升级 / 卸载（卸载会恢复 profile 文件，可回滚）
kvflow upgrade  --profile desktop --python ... --python-path ...
kvflow uninstall --profile desktop
```

安装后重启 DSH（KVFlow 不会去动正在运行的宿主进程），宿主里会出现：

- 工具：`kvflow_current_project`、`kvflow_projects`、`kvflow_project_status`、
  `kvflow_start`、`kvflow_runs`、`kvflow_status`、`kvflow_result`、`kvflow_control`、
  `kvflow_templates`、`kvflow_knowledge`
- 命令：`/flow <需求>`
- 备份在 `<profile>\.kvflow-install-backup-<时间戳>`，安装失败会自动回滚。

## 2. 注册一个项目

```powershell
kvflow project onboard --path C:\work\my-app --write --template feature
kvflow project doctor            # 体检：目录、配置、模板、预算、授权摘要
kvflow project show my-app
```

`onboard` 会读取真实文件（语言、测试框架、可写目录），生成项目内 `kvflow.project.json`，
由你批准后才注册；可写根与受保护路径重叠时会被直接拒绝。

注册的 profile 就是该项目的“证明方式”，执行时按你批准的原样 argv 运行。两条实战经验：

- 工具链不在 PATH 时（例如随项目分发的 Node），在 profile 里**固定绝对路径**：
  `argv=["C:/.../node.exe","--test"]`；允许清单仍然生效，路径不存在会被直接拒绝。
- Node 22 下 `node --test test/` 会把目录当成模块去 require 并以 `MODULE_NOT_FOUND` 失败，
  而 `node --test`（自动发现）才会真正跑测试；注册 profile 时用后者。

## 3. 预览与执行

```powershell
kvflow plan "给 src/calc.py 增加 multiply(a,b) 并用已注册测试证明" --project my-app
kvflow run  "给 src/calc.py 增加 multiply(a,b) 并用已注册测试证明" --project my-app
kvflow runs  --limit 10
kvflow result job_xxxxxxxx
```

- 模板：`feature` / `bugfix` / `refactor` / `docs_or_data`
- 模型档：`deepseek_only`（默认）、`hybrid`（Manager/评审用 Codex CLI + Worker 用 DeepSeek）
- 预算：`small` / `standard` / `large`，金额单位为微元（1 元 = 1,000,000 微元），
  全链路 GLOBAL → PROJECT → JOB 记账，超限即拒绝并记录。

## 4. 暂停 / 恢复 / 取消

```powershell
kvflow status job_xxxxxxxx
kvflow pause  job_xxxxxxxx
kvflow resume job_xxxxxxxx
kvflow cancel job_xxxxxxxx
```

## 5. 可选适配器（例如 KVStock）
适配器默认全部关闭，必须显式启用，且只读：

```powershell
kvflow adapter list
kvflow adapter enable kvstock --root C:\path\to\kvstock `
  --source '{"key":"journal","kind":"journal","path":"agent_os/data/x.sqlite3","allowed_prefixes":["STATUS"],"lifecycle_prefix":"STATUS"}'
kvflow adapter status
kvflow adapter read journal --prefix STATUS
```

未启用时任何读取都会报错，并在错误里给出启用命令；启用后也不能写入被适配的项目。

## 6. 备份与恢复

```powershell
kvflow backup --note "before upgrade"
kvflow backup verify <备份文件>
kvflow restore <备份文件> --destination C:\path\to\target
```

## 7. 出错时先看什么

```powershell
kvflow doctor            # 运行时自检（数据库、模板、模型档、凭证可用性）
kvflow project doctor    # 项目自检
kvflow result <job_id>   # 一次运行的全部证据：节点、回执、评审、集成、预算
```

KVFlow 报告任何未完成都有明确原因（缺失回执、评审未通过、集成被拒等），
不会把“部分完成”写成通过。
