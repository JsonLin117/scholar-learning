"""
Docker 核心概念 - 實作練習
場景：緯穎 ODM 多客戶 BOM 解析 pipeline

這個腳本模擬兩個客戶的 BOM 解析器
在真實情境中，每個 parser 會被打包進獨立的 Docker Container

執行方式（使用 venv）：
cd /Users/json/Projects/scholar-learning
.venv/bin/python3 Docker-K8s/practice/docker-core-concepts/solution.py

對應的 Docker 化方式（示範，不實際執行）：
  docker build -t bom-parser-google:v1.0 -f Dockerfile.google .
  docker run -e CLIENT_NAME=google bom-parser-google:v1.0
"""

import os
import json
from dataclasses import dataclass, field
from typing import List, Dict


# ============================================================
# 模擬 BOM 資料結構
# ============================================================

@dataclass
class BOMItem:
    part_number: str
    description: str
    quantity: int
    unit: str
    source_format: str  # "google_csv" or "microsoft_excel"

@dataclass
class BOMReport:
    client: str
    revision: str
    items: List[BOMItem] = field(default_factory=list)
    
    def to_standard_format(self) -> Dict:
        """
        轉換成統一的內部格式（這是 Docker Adapter 模式的核心）
        不管客戶給什麼格式，最終都轉成同一個 schema
        """
        return {
            "client": self.client,
            "revision": self.revision,
            "item_count": len(self.items),
            "items": [
                {
                    "pn": item.part_number,
                    "desc": item.description,
                    "qty": item.quantity,
                    "unit": item.unit
                }
                for item in self.items
            ]
        }


# ============================================================
# 客戶 A（Google）的 BOM 解析器
# 在真實情境中這個 parser 跑在 google-bom-parser:v1.0 container
# ============================================================

class GoogleBOMParser:
    """
    模擬 Google BOM 的 CSV 格式解析
    實際上需要 pandas 2.0
    """
    
    def parse(self, raw_data: List[Dict]) -> BOMReport:
        """
        Google 的 CSV 格式：
        part_number, description, qty, unit
        """
        report = BOMReport(client="Google", revision="R2.1")
        
        for row in raw_data:
            # Google 的欄位是 lowercase
            item = BOMItem(
                part_number=row["part_number"],
                description=row["description"],
                quantity=int(row["qty"]),
                unit=row["unit"],
                source_format="google_csv"
            )
            report.items.append(item)
        
        print(f"[GoogleParser] 解析完成，共 {len(report.items)} 個 BOM item")
        return report


# ============================================================
# 客戶 B（Microsoft）的 BOM 解析器
# 在真實情境中這個 parser 跑在 msft-bom-parser:v1.0 container
# ============================================================

class MicrosoftBOMParser:
    """
    模擬 Microsoft BOM 的 Excel 格式解析
    實際上需要 pandas 1.5 + openpyxl
    （跟 Google 的套件版本衝突 → 這就是為什麼需要分開的 Container）
    """
    
    def parse(self, raw_data: List[Dict]) -> BOMReport:
        """
        Microsoft 的格式：欄位名稱是 PartNo, Description, Quantity, UOM
        (跟 Google 的欄位名稱不同，這就是 Adapter 模式要解決的問題)
        """
        report = BOMReport(client="Microsoft", revision="v3.0.2")
        
        for row in raw_data:
            # Microsoft 的欄位是不同的命名
            item = BOMItem(
                part_number=row["PartNo"],          # 注意：不同的欄位名稱
                description=row["Description"],
                quantity=int(row["Quantity"]),
                unit=row["UOM"],                    # Unit of Measure
                source_format="microsoft_excel"
            )
            report.items.append(item)
        
        print(f"[MicrosoftParser] 解析完成，共 {len(report.items)} 個 BOM item")
        return report


# ============================================================
# Docker Layer Cache 最佳實踐展示
# ============================================================

def show_dockerfile_best_practices():
    """
    展示 Dockerfile Layer 順序的最佳實踐
    關鍵：不常改的放前面（利用 Layer cache），常改的放後面
    """
    
    bad_dockerfile = """
# ❌ 壞的 Dockerfile：每次改 code 都要重裝套件（很慢）
FROM python:3.11-slim
COPY src/ ./src/              # Layer 3：常改 ← 如果這層改了，之後的都要重建
RUN pip install -r requirements.txt  # Layer 4：很慢，每次都要重跑
"""

    good_dockerfile = """
# ✅ 好的 Dockerfile：利用 Layer cache
FROM python:3.11-slim         # Layer 1：base image（很少換）
WORKDIR /app                  # Layer 2：設定目錄（很少換）
COPY requirements.txt .       # Layer 3：requirements（不常改）
RUN pip install -r requirements.txt  # Layer 4：只有 requirements 改才重跑
COPY src/ ./src/              # Layer 5：程式碼（常改）→ 只有這層重建
RUN useradd -m appuser        # Layer 6：安全設定
USER appuser
CMD ["python", "src/pipeline.py"]
"""
    
    print("=" * 60)
    print("Docker Layer Cache 最佳實踐")
    print("=" * 60)
    print(bad_dockerfile)
    print(good_dockerfile)
    print()
    print("原則：把「越不常改的」放越前面")
    print("  不常改：base image, 系統套件, pip requirements")
    print("  常改：你的程式碼 (src/)")
    print()
    print("效果：改一行 code → 只有最後 2 個 layer 重建")
    print("      (而不是從頭重裝所有套件)")


# ============================================================
# 環境變數注入（安全實踐）
# ============================================================

def show_env_var_injection():
    """
    展示如何安全地從環境變數讀取設定
    在 Docker 中：docker run -e DB_HOST=xxx -e DB_PASSWORD=$SECRET
    永遠不要把密碼寫死在程式碼或 Dockerfile 裡！
    """
    
    # ✅ 正確：從環境變數讀取
    db_host = os.environ.get("DB_HOST", "localhost")  # 預設值供開發使用
    db_password = os.environ.get("DB_PASSWORD", "")
    client_name = os.environ.get("CLIENT_NAME", "unknown")
    
    if not db_password:
        print("⚠️  提醒：DB_PASSWORD 未設定（開發環境可以接受，生產環境必須設定）")
    
    print(f"[Config] Client: {client_name} | DB: {db_host}")
    print("[Config] 密碼從環境變數讀取，不硬編碼 ✅")


# ============================================================
# 主程式
# ============================================================

def main():
    print("=" * 60)
    print("緯穎 ODM BOM 解析 Pipeline - Docker 容器化示範")
    print("=" * 60)
    print()
    
    # 模擬 Google 的 BOM 資料（CSV 格式）
    google_raw = [
        {"part_number": "CPU-8380-001", "description": "Intel Xeon 8380", "qty": "2", "unit": "EA"},
        {"part_number": "MEM-DDR5-32G", "description": "DDR5 32GB DIMM",  "qty": "16", "unit": "EA"},
        {"part_number": "SSD-NVMe-4T",  "description": "NVMe SSD 4TB",    "qty": "4", "unit": "EA"},
    ]
    
    # 模擬 Microsoft 的 BOM 資料（Excel 格式，欄位名稱不同）
    microsoft_raw = [
        {"PartNo": "CPU-8380-002", "Description": "Intel Xeon Platinum 8380", "Quantity": "2", "UOM": "Each"},
        {"PartNo": "MEM-64G-ECC",  "Description": "64GB ECC DDR5",           "Quantity": "8", "UOM": "Each"},
        {"PartNo": "GPU-A100",     "Description": "NVIDIA A100 80GB",         "Quantity": "8", "UOM": "Each"},
    ]
    
    # 用各自的 Parser 解析（不同客戶，不同格式）
    google_parser = GoogleBOMParser()
    google_bom = google_parser.parse(google_raw)
    
    microsoft_parser = MicrosoftBOMParser()
    msft_bom = microsoft_parser.parse(microsoft_raw)
    
    # 轉成統一的內部格式（Adapter 模式的核心）
    print()
    print("--- 轉換成統一內部格式 ---")
    google_standard = google_bom.to_standard_format()
    msft_standard = msft_bom.to_standard_format()
    
    print(f"Google BOM：{json.dumps(google_standard, indent=2, ensure_ascii=False)}")
    print()
    print(f"Microsoft BOM：{json.dumps(msft_standard, indent=2, ensure_ascii=False)}")
    
    # 展示安全實踐
    print()
    show_env_var_injection()
    
    # 展示 Layer Cache 最佳實踐
    print()
    show_dockerfile_best_practices()
    
    # SA 總結
    print()
    print("=" * 60)
    print("SA 視角總結")
    print("=" * 60)
    print("""
現在的架構（Docker）：
  google-bom-parser Container → 各自的 Python 版本和套件
  msft-bom-parser Container   → 隔離，互不干擾

未來的理想架構（K8s）：
  Airflow KubernetesPodOperator
  → 每個 BOM 處理 Task 自動在 K8s 上起一個 Container
  → 完成後自動銷毀，資源利用最優化
  → 100 個客戶就起 100 個 Container 並行處理

Trade-off：
  Docker 本身：簡單，適合小規模（< 10 個 pipeline）
  K8s：複雜，但可以管理數百個 Container，值得在緯穎規模使用
""")


if __name__ == "__main__":
    main()
