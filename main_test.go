package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func sendEventRequest(s *Service, payload []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(
		http.MethodPost,
		"/events",
		bytes.NewReader(payload),
	)
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	s.ingest(rec, req)
	return rec
}

// 測試 1：正常事件可以成功接收
func TestIngestAcceptsValidEvent(t *testing.T) {
	s := NewService()

	event := Event{
		TraceID:   "test-trace-001",
		UserID:    "u123",
		EventName: "AddToCart",
		Amount:    1500,
		Timestamp: time.Now().Unix(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	rec := sendEventRequest(s, body)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d; body=%s",
			rec.Code, rec.Body.String())
	}

	if got := s.accepted.Load(); got != 1 {
		t.Fatalf("expected accepted=1, got %d", got)
	}

	if got := len(s.events); got != 1 {
		t.Fatalf("expected 1 queued event, got %d", got)
	}
}

// 測試 2：相同 TraceID 不會重複進入事件佇列
func TestIngestIgnoresDuplicateTraceID(t *testing.T) {
	s := NewService()

	payload := []byte(`{
		"TraceID": "duplicate-001",
		"UserID": "u123",
		"EventName": "AddToCart",
		"Amount": 1500,
		"Timestamp": 1690000000
	}`)

	first := sendEventRequest(s, payload)
	second := sendEventRequest(s, payload)

	if first.Code != http.StatusAccepted {
		t.Fatalf("first request: expected 202, got %d", first.Code)
	}

	if second.Code != http.StatusAccepted {
		t.Fatalf("second request: expected 202, got %d", second.Code)
	}

	var response map[string]string
	if err := json.Unmarshal(second.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response["status"] != "duplicate_ignored" {
		t.Fatalf("expected duplicate_ignored, got %q", response["status"])
	}

	if got := s.accepted.Load(); got != 1 {
		t.Fatalf("expected accepted=1, got %d", got)
	}

	if got := s.duplicates.Load(); got != 1 {
		t.Fatalf("expected duplicates=1, got %d", got)
	}

	if got := len(s.events); got != 1 {
		t.Fatalf("expected 1 queued event, got %d", got)
	}
}

// 測試 3：無效 JSON 會被拒絕
func TestIngestRejectsInvalidJSON(t *testing.T) {
	s := NewService()

	rec := sendEventRequest(s, []byte(`{"TraceID":`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", rec.Code)
	}

	if got := s.accepted.Load(); got != 0 {
		t.Fatalf("expected accepted=0, got %d", got)
	}

	if got := len(s.events); got != 0 {
		t.Fatalf("expected empty event queue, got %d", got)
	}
}

// 測試 4：低金額事件不觸發 Webhook
func TestLowAmountEventDoesNotTriggerWebhook(t *testing.T) {
	s := NewService()

	payload := []byte(`{
		"TraceID": "low-amount-001",
		"UserID": "u456",
		"EventName": "AddToCart",
		"Amount": 500,
		"Timestamp": 1690000000
	}`)

	rec := sendEventRequest(s, payload)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected status 202, got %d", rec.Code)
	}

	if got := len(s.webhooks); got != 0 {
		t.Fatalf("expected no webhook before processing, got %d", got)
	}

	if got := s.accepted.Load(); got != 1 {
		t.Fatalf("expected accepted=1, got %d", got)
	}
}

// 測試 5：高金額 AddToCart 事件觸發 Webhook
func TestHighAmountAddToCartTriggersWebhook(t *testing.T) {
	s := NewService()

	s.events <- Event{
		TraceID:   "high-amount-001",
		UserID:    "u789",
		EventName: "AddToCart",
		Amount:    1500,
		Timestamp: time.Now().Unix(),
	}

	close(s.events)
	s.batchWriter()

	if got := len(s.webhooks); got != 1 {
		t.Fatalf("expected 1 webhook, got %d", got)
	}

	if got := s.triggered.Load(); got != 1 {
		t.Fatalf("expected triggered=1, got %d", got)
	}

	webhook := <-s.webhooks
	if webhook.TraceID != "high-amount-001" {
		t.Fatalf("unexpected webhook TraceID: %s", webhook.TraceID)
	}
}

// 測試 6：金額剛好 1000 不觸發 Webhook
func TestAmountExactly1000DoesNotTriggerWebhook(t *testing.T) {
	s := NewService()

	s.events <- Event{
		TraceID:   "threshold-001",
		UserID:    "u999",
		EventName: "AddToCart",
		Amount:    1000,
		Timestamp: time.Now().Unix(),
	}

	close(s.events)
	s.batchWriter()

	if got := len(s.webhooks); got != 0 {
		t.Fatalf("expected no webhook, got %d", got)
	}

	if got := s.triggered.Load(); got != 0 {
		t.Fatalf("expected triggered=0, got %d", got)
	}
}

// 測試 7：多個並發請求使用相同 TraceID，只接受一次
func TestConcurrentDuplicateTraceID(t *testing.T) {
	s := NewService()

	const requestCount = 100

	payload := []byte(`{
		"TraceID": "concurrent-duplicate-001",
		"UserID": "u123",
		"EventName": "AddToCart",
		"Amount": 1500,
		"Timestamp": 1690000000
	}`)

	var wg sync.WaitGroup
	var mu sync.Mutex

	successCount := 0
	duplicateCount := 0
	errorCount := 0

	start := make(chan struct{})

	for i := 0; i < requestCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			rec := sendEventRequest(s, payload)

			var response map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				mu.Lock()
				errorCount++
				mu.Unlock()
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if rec.Code != http.StatusAccepted {
				errorCount++
				return
			}

			switch response["status"] {
			case "accepted":
				successCount++
			case "duplicate_ignored":
				duplicateCount++
			default:
				errorCount++
			}
		}()
	}

	close(start)
	wg.Wait()

	if errorCount != 0 {
		t.Fatalf("expected no errors, got %d", errorCount)
	}

	if successCount != 1 {
		t.Fatalf("expected exactly 1 accepted request, got %d", successCount)
	}

	if duplicateCount != requestCount-1 {
		t.Fatalf("expected %d duplicates, got %d",
			requestCount-1, duplicateCount)
	}

	if got := s.accepted.Load(); got != 1 {
		t.Fatalf("expected accepted=1, got %d", got)
	}

	if got := s.duplicates.Load(); got != requestCount-1 {
		t.Fatalf("expected duplicates=%d, got %d",
			requestCount-1, got)
	}

	if got := len(s.events); got != 1 {
		t.Fatalf("expected exactly 1 queued event, got %d", got)
	}

	t.Logf(
		"並發測試通過：%d 個請求，接受 %d 筆，攔截 %d 筆重複事件",
		requestCount,
		successCount,
		duplicateCount,
	)
}

// 測試 8：關閉事件 Channel 後，最後一批資料必須寫入
func TestBatchWriterFlushesOnShutdown(t *testing.T) {
	s := NewService()

	const eventCount = 10

	for i := 0; i < eventCount; i++ {
		s.events <- Event{
			TraceID:   fmt.Sprintf("shutdown-%d", i),
			UserID:    "u123",
			EventName: "PageView",
			Amount:    100,
			Timestamp: time.Now().Unix(),
		}
	}

	close(s.events)
	s.batchWriter()

	if got := s.stored.Load(); got != eventCount {
		t.Fatalf("expected stored=%d, got %d", eventCount, got)
	}

	t.Logf("Shutdown batch flush passed: %d events stored", eventCount)
}

// 測試 9：累積 1000 筆事件時觸發批次寫入
func TestBatchWriterFlushesAtBatchSize(t *testing.T) {
	s := NewService()

	for i := 0; i < batchSize; i++ {
		s.events <- Event{
			TraceID:   fmt.Sprintf("batch-%d", i),
			UserID:    "u123",
			EventName: "PageView",
			Amount:    100,
			Timestamp: time.Now().Unix(),
		}
	}

	close(s.events)
	s.batchWriter()

	if got := s.stored.Load(); got != batchSize {
		t.Fatalf("expected stored=%d, got %d", batchSize, got)
	}

	t.Logf("Batch size test passed: %d events stored", batchSize)
}

// 測試 10：Webhook Worker 可以排空所有待處理任務
func TestWebhookWorkerDrainsQueue(t *testing.T) {
	s := NewService()

	const webhookCount = 10

	for i := 0; i < webhookCount; i++ {
		s.webhooks <- Event{
			TraceID:   fmt.Sprintf("webhook-%d", i),
			UserID:    "u123",
			EventName: "AddToCart",
			Amount:    1500,
			Timestamp: time.Now().Unix(),
		}
	}

	close(s.webhooks)

	done := make(chan struct{})

	go func() {
		s.webhookWorker()
		close(done)
	}()

	select {
	case <-done:
		// Worker 正常完成
	case <-time.After(5 * time.Second):
		t.Fatal("webhook worker did not drain queue in time")
	}

	if got := len(s.webhooks); got != 0 {
		t.Fatalf("expected empty webhook queue, got %d", got)
	}

	t.Logf("Webhook worker drained %d events", webhookCount)
}

// 測試 11：100 個不同事件並發請求都可以成功接收
func TestConcurrentUniqueEvents(t *testing.T) {
	s := NewService()

	const requestCount = 100

	var wg sync.WaitGroup
	var mu sync.Mutex

	successCount := 0
	errorCount := 0

	start := make(chan struct{})

	for i := 0; i < requestCount; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()
			<-start

			event := Event{
				TraceID:   fmt.Sprintf("unique-%d", i),
				UserID:    fmt.Sprintf("user-%d", i),
				EventName: "PageView",
				Amount:    100,
				Timestamp: time.Now().Unix(),
			}

			payload, err := json.Marshal(event)
			if err != nil {
				mu.Lock()
				errorCount++
				mu.Unlock()
				return
			}

			rec := sendEventRequest(s, payload)

			mu.Lock()
			defer mu.Unlock()

			if rec.Code == http.StatusAccepted {
				successCount++
			} else {
				errorCount++
			}
		}(i)
	}

	close(start)
	wg.Wait()

	if errorCount != 0 {
		t.Fatalf("expected no errors, got %d", errorCount)
	}

	if successCount != requestCount {
		t.Fatalf("expected %d accepted, got %d",
			requestCount, successCount)
	}

	if got := s.accepted.Load(); got != requestCount {
		t.Fatalf("expected accepted=%d, got %d", requestCount, got)
	}

	if got := len(s.events); got != requestCount {
		t.Fatalf("expected %d queued events, got %d",
			requestCount, got)
	}

	t.Logf("Concurrent unique events passed: %d accepted", successCount)
}
