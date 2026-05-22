# Microservices vs Monolith 實作練習

**日期：** 2026-05-23  
**科目：** System-Design  
**場景：** ODM 供應鏈 — SAP Core 周邊的架構選型與 Strangler Fig 遷移

## 🏭 情境描述

緯穎這類 ODM 廠通常有一個不可動搖的 SAP Core：SO/PO、MRP、財務、庫存正式承諾都在裡面。周邊會長出客戶 Portal、供應鏈 Control Tower、AI 需求預測、物流通知等自建系統。SA 的問題不是「要不要微服務」，而是：哪些能力留在 Monolith，哪些用 Modular Monolith 起步，哪些真的值得拆成 Microservices。

## 💡 解題思路

用同一個「CSP 急單履約」需求，分別模擬三種架構：

1. **Monolith / SAP Core**：建立訂單、扣庫存、生成 MRP signal 在同一個 ACID 邊界內完成。
2. **Modular Monolith**：仍是單一部署，但 Order / Inventory / Shipping 只能透過 public API 互動，用 module boundary guard 防止直接跨表修改。
3. **Microservices**：每個服務有自己的資料與延遲/失敗風險，跨服務一致性用事件與 Saga compensation 收斂。

最後用 **Strangler Fig Router** 展示如何把 Customer Portal 流量從 legacy monolith 漸進切到新服務，而不是 Big Bang rewrite。

## 🔧 實作重點

- `ArchitectureDecision`: 用團隊規模、domain 成熟度、擴展壓力、一致性需求產生架構建議。
- `SapCoreMonolith`: 單一 transaction，失敗時 rollback。
- `ModularMonolith`: public API + boundary guard，保留 in-process call 與 ACID 優勢。
- `MicroserviceSaga`: 用事件、補償交易與模擬 network latency 展示分散式複雜度。
- `StranglerRouter`: 按 capability + traffic percentage 漸進遷移。

## 📊 SA 延伸思考

ODM 的合理路線通常是：**SAP Core 不拆**；新周邊平台先用 **Modular Monolith** 建立 DDD 邊界；只有當特定 capability 出現獨立擴展、獨立部署、團隊自治需求時才拆成 **Microservices**。微服務是組織與運維成熟後的結果，不是起點。
