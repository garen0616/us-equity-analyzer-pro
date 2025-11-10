# 美股個股分析

以 Node.js + Express 建立的 SEC / Finnhub / OpenAI / FMP 整合服務，輸入股票代號與日期即可回溯近四季財報、整合分析師共識（含 FMP 目標價）、新聞情緒、動能評分與體質打分，並提供 LLM 投資結論。前端為純靜態頁面（`public/index.html`），後端 API 位於 `/api/analyze`。

## 需求

- Node.js 18+（本地開發使用 `npm run dev`）
- 必要 API key（放進 `.env`）：
  - `SEC_USER_AGENT`：SEC 強制要求，可填 `YourApp/1.0 (email@example.com)`
  - `FINNHUB_KEY`：取得推薦 / 財報 / 報價（作為 FMP 失敗時的備援）
  - `FMP_API_KEY`：Financial Modeling Prep Pro，優先提供即時價、歷史價、動能序列與分析師目標價
  - `OPENAI_API_KEY`：呼叫 LLM（預設模型 `gpt-5`，可用 `OPENAI_MODEL` 覆寫；前置摘要/新聞任務可透過 `OPENAI_MODEL_SECONDARY` 指定較小模型，預設 `gpt-4o-mini`）
- 推薦 API key：
  - `SEC_API_KEY`：提升 SEC API 速率
  - `ALPHAVANTAGE_KEY`：Price Target / 歷史價第三層備援

## 安裝與啟動

```bash
git clone https://github.com/garen0616/us-equity-analyzer-pro.git
cd us-equity-analyzer-pro
npm install

# 編輯 .env，至少填入 SEC / Finnhub / FMP / OpenAI 金鑰
cp .env.example .env  # 如需範本

# 本地啟動
PORT=5000 npm run dev
# → http://localhost:5000
```

## 自動化自我測試（NVDA 範例）

伺服器啟動後，可以用 `curl` 直接測試 API：

```bash
DATE=$(date -I)
curl -s -X POST http://localhost:5000/api/analyze \
  -H 'Content-Type: application/json' \
  -d "{\"ticker\":\"NVDA\",\"date\":\"$DATE\"}" \
  > /tmp/nvda_api_response.json
```

本次實測（2025-11-08）關鍵輸出：

- `quote.c = 188.15`（若輸入日期為未來或當日即取即時價；若為過去則依序使用 Finnhub / AlphaVantage / Yahoo 取得歷史收盤價）
- `price_target.targetMean = 229.67`（AlphaVantage 均價，系統已自動補齊高低區間）
- `analysis.action.rating = BUY`、`target_price = 225`、`stop_loss = 165`

前端頁面同時會顯示 ChatGPT 總結、財報時間線、體質詳解、動能/趨勢與新聞情緒，並在「ChatGPT 總結」卡片下方標示本次 OpenAI token 與估算 cost。SEC MD&A、新聞與分析師/FMP 資料都會先由較小模型進行摘要與欄位萃取，再交給 gpt-5 減少 Token 使用。可用瀏覽器打開 `http://localhost:5000` 驗證。

## 批次分析（Excel / CSV）

- 前端頁面底部的「批次分析」工作列可直接上傳 Excel/CSV；第一欄 `ticker`、第二欄 `date`（`YYYY-MM-DD`），舊版第三欄 `model` 仍相容但可留空。
- 伺服器會依序執行與 `/api/analyze` 相同的流程，並輸出 CSV，欄位為：Ticker、Date、Model、現價、分析師平均/共識目標價、ChatGPT 總結目標價、建議、類型（大型/小型股）、體質分數、新聞情緒、動能評分、趨勢燈號。
- 後端同時提供 `POST /api/batch`，multipart field 名稱為 `file`，可自動取得產出的 CSV。

## 部署到 Zeabur

1. 在 Zeabur 建立新專案，選擇 **Deploy from GitHub** 並連結 `us-equity-analyzer-pro`。
2. Build 設定：
   - Runtime: Node.js 20+
   - Install command: `npm install`
   - Build command: _(留空)_
   - Start command: `npm run start`
3. Environment variables（與 `.env` 相同）：
   - `PORT=3000`（Zeabur 會自動指定，保留即可）
   - `SEC_USER_AGENT=...`
   - `SEC_API_KEY=...`
  - `FINNHUB_KEY=...`
  - `FMP_API_KEY=...`
  - `ALPHAVANTAGE_KEY=...`（如有）
  - `OPENAI_API_KEY=...`
  - `OPENAI_MODEL=gpt-5`
  - `OPENAI_MODEL_SECONDARY=gpt-4o-mini`
4. 部署完成後，Zeabur 會提供公開 URL，即可透過瀏覽器使用。

## 有用腳本

- `npm run dev`：載入 `.env` 並啟動本地伺服器。
- `npm start`：生產模式啟動（Zeabur / 其他 PaaS 使用）。
- `npm run test:self`：呼叫 `/selftest`，驗證整體串接。
