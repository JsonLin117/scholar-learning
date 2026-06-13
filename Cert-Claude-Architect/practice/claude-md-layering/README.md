# CLAUDE.md 層級與模組化 — 練習

## 場景
模擬 Claude Code 的 CLAUDE.md 載入引擎，包含：

1. **四層 Scope 解析**：Managed Policy → User → Project → Local，含子目錄 on-demand
2. **目錄樹向上走**：從 cwd 往 filesystem root 找 CLAUDE.md
3. **@import 展開**：支援相對路徑、巢狀 import（max 4 hops）、循環偵測
4. **.claude/rules/ 條件載入**：YAML frontmatter `paths` glob 匹配
5. **claudeMdExcludes 過濾**：Monorepo 排除其他團隊規則
6. **CCA 模擬考題**：5 題 D3 CLAUDE.md 相關

## ODM 場景
一個 ODM 伺服器公司的 monorepo，DE/Frontend/Firmware 三個團隊共用，
每個團隊有不同的 coding standard 和工具規範。
