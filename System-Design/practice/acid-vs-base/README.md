# ACID vs BASE 實作練習

**日期：** 2026-05-11
**科目：** System-Design
**場景：** ODM 供應鏈 — 跨系統訂單履約的一致性邊界設計

## 🏭 情境描述

緯穎這類 ODM 廠要處理客戶急單：SAP ERP 建立銷售/採購承諾、MES/SFCS 鎖定產線庫存、TMS/物流系統預約出貨。這些系統不可能共享同一個資料庫交易，所以需要判斷：哪些步驟必須 ACID，哪些步驟可以用 BASE + Saga Pattern 達到最終一致。

## 💡 解題思路

把「單一資料庫內扣庫存 + 建訂單」做成 ACID transaction；再把「跨 ERP / Inventory / Payment / Shipping」做成 Saga Orchestration。每個服務只保證自己的本地 ACID，跨服務失敗時用補償交易 rollback 已完成的商業動作。

## 🔧 實作重點

1. **ACID transaction**：用 copy-on-write transaction 模擬 atomic commit / rollback。
2. **BASE / Saga**：每個服務獨立提交，失敗時倒序執行 compensating transaction。
3. **最終一致性**：用 event queue + read model 展示 dashboard 可能短暫讀到舊資料。

## 📊 SA 延伸思考

SA 不應該追求「全部強一致」。ERP/付款/正式庫存承諾要 ACID；跨工廠 dashboard、Control Tower、物流狀態可以接受分鐘級延遲。正確做法是畫出一致性邊界：邊界內用 local transaction，邊界外用事件、補償、冪等與可觀測性治理。
