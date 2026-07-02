# dbt Cloud Architecture — 多環境設計 + Branch 策略 + CI/CD

## 場景
ODM 公司（緯穎規模）的 dbt Cloud Enterprise 部署設計：
- 15 名 DE、5 名 BI Analyst、2 名 DBA、1 名 Team Lead
- 3 個 data source（SAP MM、MES、WMS）
- 需求：Dev/Staging/Prod 環境隔離、CI 自動驗證、RBAC 權限管理

## 練習內容
1. **Environment Manager**：三環境配置模擬器（Dev/Staging/Prod）
2. **Branch Strategy Simulator**：Git flow 模擬（feature→staging→main）
3. **CI Job Engine**：`state:modified+` 選擇器 + deferral + temp schema
4. **RBAC Designer**：Group→Permission Set→Project 權限指派
5. **Environment Variable Resolver**：`env_var()` 跨環境解析
6. **模擬考題**：5 題 dbt Architect 認證模擬
