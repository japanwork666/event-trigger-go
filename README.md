# Go 即時事件收集與行銷觸發服務

使用 Golang 與 Docker 實作的輕量級事件收集與行銷觸發服務。

本專案模擬 CDP（Customer Data Platform，客戶資料平台）的事件處理流程，包含 API 接收、非同步事件處理、行銷規則判斷、批次寫入與優雅關機。

## 專案功能

- **事件接收 API**：透過 HTTP 接收 JSON 格式的使用者行為事件。
- **非同步事件處理**：使用 Go Channel 與 Goroutine 分離事件接收及後續處理。
- **即時行銷規則**：當 EventName 為 AddToCart 且 Amount 大於 1000 時，排入非同步 Webhook Worker。
- **批次寫入**：每累積 1000 筆事件，或每隔 5 秒，模擬一次資料庫批次寫入。
- **冪等性處理**：使用 TraceID 在記憶體中辨識重複事件。
- **優雅關機**：收到 SIGINT 或 SIGTERM 時，停止接收請求，排空事件佇列，完成最後一批資料寫入及 Webhook 工作。
- **結構化日誌**：使用 Go 標準函式庫 slog 輸出 JSON 日誌。
- **Docker 多階段建置**：不需要在本機安裝 Go，也可以建置及執行服務。

## 技術架構

```text
使用者或外部系統
       |
       v
HTTP API（接收 JSON 事件）
       |
       v
事件 Channel
       |
       v
批次處理與行銷規則引擎
       |
       +--------------------+
       |                    |
       v                    v
模擬資料庫批次寫入      Webhook Channel
                            |
                            v
                      非同步 Worker
                            |
                            v
                      模擬 LINE 優惠券推播
```

目前所有處理元件都在同一個 Go 程序中執行，因此 Docker 只需要一個服務容器。

## 專案檔案

```text
event-trigger-go/
├── main.go
├── main_test.go
├── go.mod
├── Dockerfile
├── compose.yaml
├── .dockerignore
├── .gitignore
├── README.md
└── LICENSE
```

## 執行環境

- Docker Desktop
- Docker Compose

本機不需要安裝 Golang，所有建置與測試都可以透過 Docker 完成。

## 使用 Docker 啟動

在專案根目錄執行：

```bash
docker compose up --build -d
```

查看容器狀態：

```bash
docker compose ps
```

查看服務日誌：

```bash
docker compose logs -f event-service
```

## API 使用方式

### 1. 健康檢查

API：

`GET /health`

請求：

```bash
curl http://localhost:8080/health
```

回傳範例：

```json
{
  "status": "ok",
  "accepting": true,
  "accepted": 0,
  "duplicates": 0,
  "triggered": 0,
  "stored": 0
}
```

欄位說明：

- `status`：服務狀態。
- `accepting`：是否仍接受新事件。
- `accepted`：成功放入事件佇列的事件數量。
- `duplicates`：被判定為重複的事件數量。
- `triggered`：已排入 Webhook 佇列的任務數量。
- `stored`：已計入模擬批次寫入的事件數量。

以上計數器只存在記憶體中，服務重新啟動後會歸零。

### 2. 接收使用者事件

API：

`POST /events`

請求範例：

```bash
curl -i -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "TraceID": "trace-001",
    "UserID": "u123",
    "EventName": "AddToCart",
    "Amount": 1500,
    "Timestamp": 1690000000
  }'
```

事件格式：

```json
{
  "TraceID": "trace-001",
  "UserID": "u123",
  "EventName": "AddToCart",
  "Amount": 1500,
  "Timestamp": 1690000000
}
```

欄位說明：

- `TraceID`：事件唯一識別碼，用於辨識重複事件。
- `UserID`：使用者識別碼。
- `EventName`：使用者行為名稱。
- `Amount`：事件金額。
- `Timestamp`：Unix 時間戳記。

成功接收新事件時，API 回傳 HTTP 202 Accepted。

### 3. 行銷觸發規則

當事件符合以下條件時，會排入非同步 Webhook Worker：

```go
EventName == "AddToCart" && Amount > 1000
```

符合條件的事件會輸出模擬 LINE 優惠券推播日誌。

本專案不會真正呼叫 LINE API，也不會實際發送優惠券。

### 4. 批次寫入機制

批次處理符合以下任一條件時會執行模擬寫入：

- 累積 1000 筆事件。
- 每隔 5 秒執行一次批次檢查。

目前只輸出批次寫入日誌並更新計數器，尚未連接真正的資料庫。

## 冪等性設計（Idempotency）

本專案使用 TraceID 搭配記憶體中的 map，避免同一個程序重複接受相同 TraceID 的事件。

設計方式：

1. 接收事件時，先檢查 TraceID 是否存在。
2. 如果已存在，回傳 `duplicate_ignored`。
3. 如果不存在，先保留 TraceID，再將事件放入 Channel。
4. 使用 Mutex 保護共享資料，避免並發請求造成重複接收。

目前限制：

- 服務重啟後，去重紀錄會消失。
- 多個服務副本之間不會共享去重狀態。
- 尚未實作資料庫唯一索引或持久化冪等性。
- 真正的外部 Webhook 仍需要考慮重試造成的重複發送。

生產環境可使用資料庫唯一索引、Redis、持久化冪等鍵及可靠訊息佇列，進一步確保資料一致性。

## 優雅關機（Graceful Shutdown）

服務收到 SIGINT 或 SIGTERM 時，會嘗試執行以下流程：

1. 接收關閉訊號。
2. 停止接收新的事件。
3. 等待正在處理的 HTTP 請求完成。
4. 關閉事件 Channel。
5. 排空事件佇列。
6. 寫入最後一批尚未處理的事件。
7. 關閉 Webhook Channel。
8. 等待 Webhook Worker 完成剩餘工作。
9. 所有工作完成後，服務才正式結束。

本專案設定 30 秒的 Graceful Shutdown Timeout。

若遇到關機逾時或程序遭強制終止，仍無法保證所有記憶體中的事件都能完成處理。

## 自動化測試

本專案使用 Go 標準 testing 套件進行測試。

即使本機沒有安裝 Go，也可以透過 Docker 執行。

### 1. 格式化程式碼

```bash
docker run --rm \
  -v "$PWD":/src \
  -w /src \
  golang:1.23 \
  gofmt -w main.go main_test.go
```

### 2. 執行測試

```bash
docker run --rm \
  -v "$PWD":/src \
  -w /src \
  golang:1.23 \
  go test -race -v ./...
```

### 3. 測試項目

目前測試涵蓋：

- 正常事件可以成功接收。
- 相同 TraceID 不會重複進入事件佇列。
- 無效 JSON 會被拒絕。
- 低金額事件不觸發 Webhook。
- 高金額 AddToCart 事件觸發 Webhook。
- 金額剛好 1000 不觸發 Webhook。
- 100 個並發請求使用相同 TraceID，只接受一次。
- 關機時批次寫入剩餘事件。
- 累積 1000 筆事件時觸發批次寫入。
- Webhook Worker 能夠排空佇列。
- 100 個不同 TraceID 的並發事件可以正常接收。

### 4. 測試結果

使用 Docker 執行：

```bash
docker run --rm \
  -v "$PWD":/src \
  -w /src \
  golang:1.23 \
  go test -race -v ./...
```

測試結果：

```text
PASS
ok event-trigger-go
```

`-race` 用於偵測 Go 程式中的 Data Race（資料競爭）。

## Docker Graceful Shutdown 實測

本專案使用 Docker Compose 驗證優雅關機流程。

### 1. 啟動服務

```bash
docker compose up --build -d
```

### 2. 發送測試事件

```bash
curl -i -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '{
    "TraceID": "shutdown-test-001",
    "UserID": "u123",
    "EventName": "AddToCart",
    "Amount": 1500,
    "Timestamp": 1790000000
  }'
```

預期回應：

```json
{
  "status": "accepted"
}
```

### 3. 停止服務

```bash
docker compose stop
```

Docker 會向容器程序發送 SIGTERM。

### 4. 查看關機日誌

```bash
docker compose logs event-service
```

預期可以看到類似以下紀錄：

```text
shutdown signal received
event ingestion disabled
event queue closed, draining workers
batch writer drained
webhook worker drained
shutdown complete
```

最後確認：

```text
accepted=1
triggered=1
stored=1
```

代表測試事件已經完成模擬批次寫入及 Webhook 處理。

## 停止服務與清理 Docker 資源

停止服務但保留容器：

```bash
docker compose stop
```

重新啟動：

```bash
docker compose start
```

停止並移除本專案的容器與網路：

```bash
docker compose down
```

停止並移除本機建置的 Image：

```bash
docker compose down --rmi local
```

以上操作不會刪除本機原始碼，也不會自動刪除 GitHub 上的 Repository。

## 監控與日誌規劃

本專案使用 Go 標準函式庫 `log/slog` 輸出 JSON 結構化日誌。

目前日誌包含：

- HTTP Server 啟動資訊。
- Webhook 觸發紀錄。
- 模擬資料庫批次寫入紀錄。
- Worker 完成處理紀錄。
- Graceful Shutdown 流程紀錄。
- 服務關閉統計資訊。

正式環境應持續監控：

- API 請求量、延遲及錯誤率。
- 事件佇列長度與批次大小。
- Webhook 成功率、失敗數及重試次數。
- 批次寫入耗時與失敗率。
- 關機耗時及未完成工作數量。

日誌建議包含 TraceID、事件名稱、處理結果與耗時，方便追蹤單一事件的生命週期。

## 生產環境改善方向

若要將本專案升級為正式服務，建議進一步實作：

- **訊息佇列**：使用 Kafka 或 RabbitMQ，避免程序異常終止時遺失尚未持久化的事件。
- **資料庫**：使用 PostgreSQL 交易與批次寫入。
- **持久化冪等性**：將 TraceID 與事件處理狀態儲存至 Redis 或資料庫。
- **Webhook 重試**：加入逾時、重試、指數退避與死信佇列。
- **監控系統**：使用 Prometheus 與 Grafana 監控流量、錯誤率及佇列深度。
- **分散式追蹤**：使用 OpenTelemetry 串接事件處理生命週期。
- **效能測試**：進行壓力測試、故障注入及資料恢復測試。

## AI 協作開發流程

本專案使用 AI 協助開發，並由開發者負責程式碼整合、執行測試及驗證結果。

開發流程：

1. 使用 AI 分析作業需求及拆解功能。
2. 使用 AI 協助設計 Go Channel、Goroutine 與 Worker 架構。
3. 使用 AI 協助實作 HTTP Event Ingestion API。
4. 使用 AI 協助設計 Idempotency 與並發安全機制。
5. 使用 AI 協助實作 Batch Writer 與 Graceful Shutdown。
6. 使用 AI 協助撰寫單元測試及並發測試。
7. 使用 Docker 執行程式格式化、Race Detection 與測試。
8. 使用 Docker Compose 驗證服務啟動、事件接收及優雅關機。
9. 整理 README 並準備 GitHub 專案展示。

AI 負責協助程式設計與問題分析，最終程式碼及測試結果由開發者實際執行與確認。

## 專案限制

目前為輕量級 Demo 專案，並非完整的正式 CDP 系統。

- 資料庫寫入僅為模擬，沒有真正持久化。
- LINE Webhook 僅輸出模擬日誌，沒有實際呼叫 LINE API。
- 事件佇列與去重資料都存在記憶體中。
- 不支援多個服務副本之間的資料共享。
- 無法保證程序遭強制終止時記憶體內的事件不遺失。

本專案主要展示 Go 並發處理、事件路由、批次處理、冪等性、Docker 部署及 Graceful Shutdown 的實作能力。
