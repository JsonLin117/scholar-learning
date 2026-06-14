# Practice: Skills & Slash Commands — ODM 供應鏈 Skill 設計

## 場景
設計一套 Claude Code Skills 給 ODM 供應鏈公司的 Data Engineering 團隊。
包含三個 skill：
1. **pipeline-check** — 自動檢查 Airflow DAG 狀態和 Delta Lake 表健康度（reference skill, Claude 自動觸發）
2. **deploy-pipeline** — 部署 data pipeline 到 production（task skill, 只能手動 /deploy-pipeline）
3. **schema-review** — 審查 SQL schema 變更（context:fork, 注入當前 git diff）

## 練習重點
- 正確使用 frontmatter 欄位（特別是觸發控制）
- 動態 context 注入（`!`command``）
- Supporting files 組織
- context: fork 隔離
- allowed-tools / disallowed-tools 安全設計

## 驗證
`solution.py` 用 Python 解析 SKILL.md frontmatter 並驗證設計規則：
- deploy 類 skill 必須 disable-model-invocation
- 自動背景 skill 不能有 AskUserQuestion
- description 不超過 1536 字元
