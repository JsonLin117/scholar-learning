# Docker 核心概念 實作練習

**日期：** 2026-04-17
**科目：** Docker-K8s（suppB）
**場景：** ODM 供應鏈 — 多客戶 BOM 解析 pipeline

---

## 🏭 情境描述

你是緯穎的 Data Engineer。每天需要處理來自不同 CSP 客戶（Google、Microsoft）的 BOM 差異報告，這兩個客戶的 BOM 格式不同，所需的 Python 套件版本也不同：

- Google BOM：CSV 格式，需要 pandas 2.0
- Microsoft BOM：Excel 格式，需要 pandas 1.5 + openpyxl

**問題**：如果直接在同一台機器上跑兩個 pipeline，套件版本衝突。

**解法**：用 Docker 把每個 pipeline 打包成獨立 Container，各自有自己的環境。

---

## 💡 解題思路

1. 為每個 BOM pipeline 寫一個 Dockerfile
2. 利用 **Layer Cache 最佳實踐**：先 COPY requirements.txt → pip install，再 COPY src/
3. 用**非 root user** 跑 Container（最小權限原則）
4. 密碼/API Key 用**環境變數注入**，不寫死
5. 提供 `docker-compose.yml` 方便本地同時跑兩個 pipeline

---

## 🔧 實作重點

### 1. Layer Cache 順序（最關鍵）
```dockerfile
# ✅ 正確順序：不常改的放前面（利用 cache）
COPY requirements.txt .      ← 不常改 → cache 命中
RUN pip install -r ...        ← 不常改 → cache 命中
COPY src/ .                  ← 常改   → 只有這層和之後重建

# ❌ 錯誤順序：每次改 code 都重裝套件（很慢）
COPY src/ .
RUN pip install -r ...
```

### 2. Non-root User 安全實踐
```dockerfile
RUN useradd -m appuser
USER appuser
```

### 3. Volume 掛載（資料持久化）
```bash
docker run -v $(pwd)/output:/app/output bom-parser-google:v1.0
```

---

## 📊 SA 延伸思考

**如果我是 SA，這個方案還能怎麼改進？**

1. **生產環境**：不用 `docker run` 手動跑，改用 K8s CronJob 定時執行
2. **CI/CD 整合**：GitHub Actions 自動 build image + push 到 Azure ACR
3. **Scale out**：如果需要同時處理 100 個客戶的 BOM，用 K8s Job 並行跑
4. **Multi-stage build**：builder stage 安裝編譯工具，runtime stage 只保留最小依賴 → Image 更小

**Trade-off**：
- Docker 本身：學習成本低，適合現在的規模（幾個 pipeline）
- Airflow KubernetesPodOperator：每個 DAG task 跑在獨立 Container → 未來的理想架構（Docker-K8s Lesson 10 會學）
