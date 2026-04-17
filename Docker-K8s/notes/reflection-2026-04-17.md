# 反思 2026-04-17 — Docker 核心概念

**科目：** Docker-K8s（suppB）| **主題：** Docker Image / Container / Layer / Registry

---

## Senior DE 考題自評

**考題：** 把 PySpark ETL pipeline 容器化並部署到 K8s，說明 Dockerfile 關鍵設計考量與 Layer 順序重要性

**自評：** ⭐⭐⭐⭐⭐ (5/5)

**漏掉的細節（待補強）：**
- `pip install --no-cache-dir` — 移除 pip 快取，縮小 Image
- 明確設定 `WORKDIR /app`
- **Multi-stage build** — builder stage 裝編譯工具 → runtime stage 最小依賴（可縮小 50-70%）

---

## SA 挑戰：ODM 多 Pipeline 容器化

**情境：** 緯穎 3 個 pipeline（bom-diff / forecast-ingest / odt-report）有 pandas 版本衝突

**核心方案：**
- 每個 pipeline 獨立 Docker Image → 推到 Azure ACR
- 用 SHA digest 而非 tag pin 版本（Production 穩定性）
- K8s KubernetesPodOperator 獨立資源 requests/limits

**SA Trade-off：**
- ✅ 套件完全隔離、版本可控
- ⚠️ 3 個獨立 CI/CD build pipeline（複雜度增加）
- 💡 優化：共享 base Layer（pyspark-base），減少 ACR 儲存 + build 時間

---

## 今日 SC Daily 連結（ODM vs OEM vs OBM）

Docker ↔ ODM 供應鏈的連結：
- 緯穎多 CSP 客戶 = 多 BOM 格式 = 多套件版本需求 → Docker 容器化隔離是自然解法
- OCP 標準化 → 未來 BOM schema 標準化機會 → 減少 multi-format ETL 複雜度

---

## 明天要深挖

1. Multi-stage build 實際 Image 大小節省
2. docker-compose 本地多 pipeline 環境（Lesson 2）
3. Airflow KubernetesPodOperator 資源設定策略
