# CCA 模擬考題練習 — 6 情境全練

**日期：** 2026-06-22
**科目：** Cert-Claude-Architect（Topic 16/16 — 最終回）
**場景：** CCA 考試全真模擬（6 Scenarios × 5 Questions = 30 題）

## 🏭 情境描述

Claude Certified Architect — Foundations 考試隨機抽 4/6 個情境出題。
本練習覆蓋全部 6 個情境，每個情境 5 題，共 30 題。
每題 4 選 1，模擬真實考試格式。

題目設計原則：
- 涵蓋所有 5 個 Domain（D1-D5）的交叉出題
- 重點覆蓋高頻考點：Hook vs Prompt、tool_choice 三種模式、subagent context 隔離、-p flag、Batch API、nullable schema、escalation 反模式
- 包含常見陷阱：不存在的功能、over-engineering、self-reported confidence

## 💡 解題思路

每題分析四個維度：
1. **Root Cause**：問題的真正根因是什麼？
2. **Proportionality**：最低成本最高槓桿的解法
3. **Determinism**：關鍵路徑需要確定性（Hook）還是概率性（Prompt）足夠
4. **Match API to Use Case**：同步 vs 批次、real-time vs latency-tolerant

## 🔧 實作重點

- 30 題完整模擬考 + 逐題解析
- 自動評分 + Domain 分析
- 弱點偵測（哪個 Domain 錯最多）
- 全部題目可重複練習

## 📊 SA 延伸思考

CCA 考試的知識體系直接映射到 ODM 供應鏈的 AI Agent 架構設計：
- Customer Support Agent → 供應鏈客訴自動分類+升級
- Multi-Agent → 跨部門（採購/倉庫/產線）agent 協作
- Data Extraction → SAP 文件自動化解析
- CI/CD → Data Pipeline 自動 review
