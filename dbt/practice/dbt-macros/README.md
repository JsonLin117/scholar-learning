# dbt Macros 練習

## 場景
ODM 供應鏈 dbt 專案中的 Macro 系統模擬。

## 練習內容
1. **MiniJinjaEngine**：模擬 Jinja 模板引擎的編譯流程（`{{ }}`、`{% for %}`、`{% if %}`、`{% set %}`）
2. **MacroRegistry**：macro 定義、查找、調用、dispatch（跨平台適配）
3. **PackageSystem**：packages.yml 解析、namespace 隔離、override 搜尋順序
4. **ODM Macro Library**：5 個實用 macro（物料號標準化、SK 生成、UOM 轉換、PIVOT、限制 dev 資料量）
5. **Anti-Pattern Detection**：偵測 macro 反模式（過度巢狀、macro 中 ref()、缺 execute guard）
6. **Readability vs DRY 決策器**：根據重複次數/平台數/複雜度判斷是否應抽 macro

## 執行
```bash
cd /Users/json/Projects/scholar-learning
JAVA_HOME=/opt/homebrew/opt/openjdk@17 .venv/bin/python3 dbt/practice/dbt-macros/solution.py
```
