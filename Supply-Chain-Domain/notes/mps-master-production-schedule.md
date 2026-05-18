# MPS (Master Production Schedule) Notes — 2026-05-19

## 一句話
MPS 把客戶 Forecast+PO 翻譯成「何時生產、生產什麼、生產多少」的計劃。

## MPS 計算公式
MPS = Forecast + Firm PO - On-hand - In-transit + Safety Stock → Planned Production

## Time Fence
- Frozen(本週~2週): 不改
- Slushy(2~6週): 需審批
- Free(6週+): 自由調

## SAP T-Code
- MD61/62/63: 計劃獨立需求 CRUD
- MD40: MPS Run（只處理 MPS 物料）
- MD04: Stock/Req List
- CO40: 計劃訂單→生產訂單

## ATP = MPS - 已承諾 PO
