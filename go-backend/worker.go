package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/encoding/protojson"
)

// WorkerConfig holds configurations for the background worker loaded from environment variables
type WorkerConfig struct {
	WorkerEnabled                 bool
	DevelopmentMode               bool
	XanoBaseURL                   string
	XanoAuthToken                 string
	BranchID                      int
	DeviceCode                    string
	PollIntervalSeconds           int
	NoJobSleepSeconds             int
	DisconnectedSleepSeconds      int
	HeartbeatIntervalSeconds      int
	ErrorBackoffInitialSeconds    int
	ErrorBackoffMaxSeconds        int
	DevelopmentRecipientAllowlist []string
}

// ClaimJob represents the job schema returned by Xano claim_due endpoint
type ClaimJob struct {
	ID              int64  `json:"id"`
	BranchID        int    `json:"branch_id"`
	RecipientPhone  string `json:"recipient_phone"`
	RecipientJID    string `json:"recipient_jid"`
	MessageSnapshot string `json:"message_snapshot"`
}

var (
	workerStarted  int32 // in-memory guard to prevent multiple instances of the worker goroutine
	workerConfig   WorkerConfig
	workerConfigMu sync.RWMutex
	globalClient   *whatsmeow.Client
)

func getWorkerConfig() WorkerConfig {
	workerConfigMu.RLock()
	defer workerConfigMu.RUnlock()
	return workerConfig
}

// UpdateWorkerConfig updates the worker configuration dynamically in memory
func UpdateWorkerConfig(token string, enabled bool, branchID int, deviceCode string) {
	workerConfigMu.Lock()
	workerConfig.XanoAuthToken = token
	workerConfig.WorkerEnabled = enabled
	if branchID > 0 {
		workerConfig.BranchID = branchID
	}
	if deviceCode != "" {
		workerConfig.DeviceCode = deviceCode
	}

	maskedToken := ""
	if workerConfig.XanoAuthToken != "" {
		maskedToken = "****"
		if len(workerConfig.XanoAuthToken) > 8 {
			maskedToken = workerConfig.XanoAuthToken[:4] + "..." + workerConfig.XanoAuthToken[len(workerConfig.XanoAuthToken)-4:]
		}
	}
	savedBranchID := workerConfig.BranchID
	savedDeviceCode := workerConfig.DeviceCode
	workerConfigMu.Unlock()

	log.Printf("[WorkerConfig Update] WORKER_ENABLED: %v, BRANCH_ID: %d, DEVICE_CODE: %s, Token: %s",
		enabled, savedBranchID, savedDeviceCode, maskedToken)

	if enabled && token != "" {
		workerConfigMu.RLock()
		client := globalClient
		workerConfigMu.RUnlock()
		startAutomationWorkerLoopIfReady(client)
	}
}

func startAutomationWorkerLoopIfReady(whatsAppClient *whatsmeow.Client) bool {
	config := getWorkerConfig()

	if !config.WorkerEnabled {
		log.Println("[Worker] Background automation worker disabled. WhatsConect will run as a manual desktop sender only.")
		return false
	}

	if config.XanoAuthToken == "" {
		log.Println("[Worker] Background automation worker enabled, but no Xano token is configured. Worker will not start.")
		return false
	}

	if whatsAppClient == nil {
		log.Println("[Worker] Background automation worker enabled, but WhatsApp client is not ready. Worker will not start.")
		return false
	}

	if !atomic.CompareAndSwapInt32(&workerStarted, 0, 1) {
		log.Println("[Worker] StartAutomationWorker called, but worker is already running. Ignoring start request.")
		return false
	}

	log.Println("[Worker] Starting background automation worker loop.")
	go runWorkerLoop(whatsAppClient)
	return true
}

// StartAutomationWorker initializes and starts the background worker loop if enabled
func StartAutomationWorker(whatsAppClient *whatsmeow.Client) {
	config := loadConfig()

	workerConfigMu.Lock()
	workerConfig = config
	globalClient = whatsAppClient
	workerConfigMu.Unlock()

	startAutomationWorkerLoopIfReady(whatsAppClient)
}

func loadConfig() WorkerConfig {
	config := WorkerConfig{
		WorkerEnabled:              getEnvBool("WORKER_ENABLED", false),
		DevelopmentMode:            getEnvBool("DEVELOPMENT_MODE", true),
		XanoBaseURL:                getEnvString("XANO_BASE_URL", "https://xqoc-ewo0-x3u2.s2.xano.io/api:lY50ALPv"),
		XanoAuthToken:              getEnvString("XANO_AUTH_TOKEN", ""),
		BranchID:                   getEnvInt("BRANCH_ID", 4),
		DeviceCode:                 getEnvString("DEVICE_CODE", "BRANCH4-MAIN-PC"),
		PollIntervalSeconds:        getEnvInt("POLL_INTERVAL_SECONDS", 60),
		NoJobSleepSeconds:          getEnvInt("NO_JOB_SLEEP_SECONDS", 60),
		DisconnectedSleepSeconds:   getEnvInt("DISCONNECTED_SLEEP_SECONDS", 120),
		HeartbeatIntervalSeconds:   getEnvInt("HEARTBEAT_INTERVAL_SECONDS", 60),
		ErrorBackoffInitialSeconds: getEnvInt("ERROR_BACKOFF_INITIAL_SECONDS", 60),
		ErrorBackoffMaxSeconds:     getEnvInt("ERROR_BACKOFF_MAX_SECONDS", 600),
	}

	allowlistRaw := getEnvString("DEVELOPMENT_RECIPIENT_ALLOWLIST", "60109648647")
	var allowlist []string
	for _, s := range strings.Split(allowlistRaw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			allowlist = append(allowlist, s)
		}
	}
	config.DevelopmentRecipientAllowlist = allowlist
	return config
}

// LogConfig prints the worker configurations with authorization token masked for security
func (c WorkerConfig) LogConfig() {
	maskedToken := ""
	if c.XanoAuthToken != "" {
		maskedToken = "****"
		if len(c.XanoAuthToken) > 8 {
			maskedToken = c.XanoAuthToken[:4] + "..." + c.XanoAuthToken[len(c.XanoAuthToken)-4:]
		}
	}
	log.Printf("[WorkerConfig] WORKER_ENABLED: %v, DEVELOPMENT_MODE: %v, XANO_BASE_URL: %s, XANO_AUTH_TOKEN: %s, BRANCH_ID: %d, DEVICE_CODE: %s, POLL_INTERVAL_SECONDS: %d, NO_JOB_SLEEP_SECONDS: %d, DISCONNECTED_SLEEP_SECONDS: %d, HEARTBEAT_INTERVAL_SECONDS: %d, ERROR_BACKOFF_INITIAL_SECONDS: %d, ERROR_BACKOFF_MAX_SECONDS: %d, DEVELOPMENT_RECIPIENT_ALLOWLIST: %v",
		c.WorkerEnabled, c.DevelopmentMode, c.XanoBaseURL, maskedToken, c.BranchID, c.DeviceCode, c.PollIntervalSeconds, c.NoJobSleepSeconds, c.DisconnectedSleepSeconds, c.HeartbeatIntervalSeconds, c.ErrorBackoffInitialSeconds, c.ErrorBackoffMaxSeconds, c.DevelopmentRecipientAllowlist)
}

func runWorkerLoop(whatsAppClient *whatsmeow.Client) {
	lastHeartbeat := time.Time{}
	lastClaim := time.Time{}
	var consecutiveNoJobs int
	var lastNoJobLog time.Time
	var lastWaStatus string = "UNKNOWN"
	var backoffDuration time.Duration

	httpClient := &http.Client{
		Timeout: 15 * time.Second,
	}

	for {
		now := time.Now()
		config := getWorkerConfig()

		// 1. Determine current WhatsApp connection state
		var waStatus string
		var waJID string
		var waPushName string

		if whatsAppClient == nil {
			waStatus = "ERROR"
		} else {
			hasDeviceIdentity := whatsAppClient.Store != nil && whatsAppClient.Store.ID != nil
			isConnected := whatsAppClient.IsConnected()

			if isConnected {
				waStatus = "CONNECTED"
			} else {
				if hasDeviceIdentity {
					waStatus = "DISCONNECTED"
				} else {
					waStatus = "LOGGED_OUT"
				}
			}

			if whatsAppClient.Store != nil {
				waPushName = whatsAppClient.Store.PushName
				if whatsAppClient.Store.ID != nil {
					waJID = whatsAppClient.Store.ID.String()
				}
			}
		}

		statusChanged := waStatus != lastWaStatus
		if statusChanged && lastWaStatus != "UNKNOWN" {
			log.Printf("[Worker] WhatsApp connection status changed from '%s' to '%s'. Performing immediate heartbeat.", lastWaStatus, waStatus)
		}

		// Only run polling/heartbeat if worker is enabled and has a XanoAuthToken
		if config.WorkerEnabled && config.XanoAuthToken != "" {
			// 2. Perform Heartbeat
			heartbeatInterval := time.Duration(config.HeartbeatIntervalSeconds) * time.Second
			if waStatus != "CONNECTED" {
				heartbeatInterval = time.Duration(config.DisconnectedSleepSeconds) * time.Second
			}

			if now.Sub(lastHeartbeat) >= heartbeatInterval || statusChanged || lastHeartbeat.IsZero() {
				if statusChanged || lastHeartbeat.IsZero() {
					log.Printf("[Worker] Performing heartbeat check. WhatsApp status: %s", waStatus)
				}
				err := sendHeartbeat(httpClient, config, waStatus, waJID, waPushName)
				if err != nil {
					log.Printf("[Worker] Heartbeat failed: %v", err)
					backoffDuration = calculateBackoff(backoffDuration, config)
					lastHeartbeat = now // Avoid infinite busy loop retries
					sleepAndLog("Error backoff (heartbeat error)", backoffDuration)
					continue
				} else {
					if statusChanged || lastHeartbeat.IsZero() {
						log.Println("[Worker] Heartbeat succeeded.")
					}
					backoffDuration = 0 // Reset backoff on success
					lastHeartbeat = now
					lastWaStatus = waStatus // Update only after successful heartbeat
				}
			}

			// 3. Claim and Process Jobs (Only when WhatsApp is CONNECTED)
			if waStatus == "CONNECTED" && backoffDuration == 0 {
				claimInterval := 5 * time.Second
				if consecutiveNoJobs == 1 {
					claimInterval = time.Duration(config.PollIntervalSeconds) * time.Second
				} else if consecutiveNoJobs >= 2 {
					claimInterval = time.Duration(config.NoJobSleepSeconds) * time.Second
				}

				if now.Sub(lastClaim) >= claimInterval {
					job, err := claimDueJob(httpClient, config)
					if err != nil {
						log.Printf("[Worker] Claim due job failed: %v", err)
						backoffDuration = calculateBackoff(backoffDuration, config)
						lastClaim = now // Avoid infinite busy loop retries
						sleepAndLog("Error backoff (claim_due error)", backoffDuration)
						continue
					}

					if job == nil {
						consecutiveNoJobs++
						lastClaim = now
						if consecutiveNoJobs == 1 || now.Sub(lastNoJobLog) >= 60*time.Second {
							log.Println("[Worker] No due jobs found.")
							lastNoJobLog = now
						}
					} else {
						consecutiveNoJobs = 0
						lastClaim = now
						log.Printf("[Worker] Claimed job ID: %d. Recipient: %s", job.ID, job.RecipientPhone)

						// Process claimed job
						err = processClaimedJob(httpClient, whatsAppClient, config, job)
						if err != nil {
							log.Printf("[Worker] Process job %d failed: %v", job.ID, err)
							backoffDuration = calculateBackoff(backoffDuration, config)
							sleepAndLog("Error backoff (process job error)", backoffDuration)
							continue
						} else {
							backoffDuration = 0 // Reset backoff on success
						}
					}
				}
			}
		} else {
			if now.Sub(lastNoJobLog) >= 60*time.Second || lastNoJobLog.IsZero() {
				log.Println("[Worker] Waiting for valid XanoAuthToken configuration to start automation loop...")
				lastNoJobLog = now
			}
		}

		// 4. Calculate next sleep time to save resources
		nextSleep := calculateNextSleep(lastHeartbeat, lastClaim, consecutiveNoJobs, waStatus, config, backoffDuration)
		sleepAndLog("Loop wait", nextSleep)
	}
}

func calculateBackoff(current time.Duration, config WorkerConfig) time.Duration {
	initial := time.Duration(config.ErrorBackoffInitialSeconds) * time.Second
	max := time.Duration(config.ErrorBackoffMaxSeconds) * time.Second

	if current == 0 {
		return initial
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

func calculateNextSleep(lastHeartbeat, lastClaim time.Time, consecutiveNoJobs int, waStatus string, config WorkerConfig, backoffDuration time.Duration) time.Duration {
	if backoffDuration > 0 {
		return backoffDuration
	}

	if !config.WorkerEnabled || config.XanoAuthToken == "" {
		idleSeconds := config.NoJobSleepSeconds
		if idleSeconds < 5 {
			idleSeconds = 5
		}
		if idleSeconds > 60 {
			idleSeconds = 60
		}
		return time.Duration(idleSeconds) * time.Second
	}

	now := time.Now()
	var nextSleep time.Duration

	if waStatus == "CONNECTED" {
		// heartbeat next event
		nextHeartbeat := lastHeartbeat.Add(time.Duration(config.HeartbeatIntervalSeconds) * time.Second)
		timeToHeartbeat := nextHeartbeat.Sub(now)
		if timeToHeartbeat < 0 {
			timeToHeartbeat = 0
		}

		// claim next event
		claimInterval := 5 * time.Second
		if consecutiveNoJobs == 1 {
			claimInterval = time.Duration(config.PollIntervalSeconds) * time.Second
		} else if consecutiveNoJobs >= 2 {
			claimInterval = time.Duration(config.NoJobSleepSeconds) * time.Second
		}
		nextClaim := lastClaim.Add(claimInterval)
		timeToClaim := nextClaim.Sub(now)
		if timeToClaim < 0 {
			timeToClaim = 0
		}

		// Sleep the minimum of both to check at the earliest event
		if timeToHeartbeat < timeToClaim {
			nextSleep = timeToHeartbeat
		} else {
			nextSleep = timeToClaim
		}
	} else {
		// DISCONNECTED/LOGGED_OUT/ERROR: only heartbeat check
		nextHeartbeat := lastHeartbeat.Add(time.Duration(config.DisconnectedSleepSeconds) * time.Second)
		timeToHeartbeat := nextHeartbeat.Sub(now)
		if timeToHeartbeat < 0 {
			timeToHeartbeat = 0
		}
		nextSleep = timeToHeartbeat
	}

	// Safety clamp to prevent busy loop (min 1 second)
	if nextSleep < 1*time.Second {
		nextSleep = 1 * time.Second
	}
	return nextSleep
}

func sleepAndLog(reason string, duration time.Duration) {
	log.Printf("[Worker] Sleeping for %v. Reason: %s", duration, reason)
	time.Sleep(duration)
}

func sendHeartbeat(httpClient *http.Client, config WorkerConfig, waStatus, waJID, waPushName string) error {
	urlStr := fmt.Sprintf("%s/sender_heartbeat", strings.TrimSuffix(config.XanoBaseURL, "/"))

	runtimeMode := "PRODUCTION"
	if config.DevelopmentMode {
		runtimeMode = "DEVELOPMENT"
	}

	reqBody := map[string]any{
		"branch_id":          config.BranchID,
		"device_code":        config.DeviceCode,
		"runtime_mode":       runtimeMode,
		"service_status":     "ONLINE",
		"whatsapp_status":    waStatus,
		"whatsapp_jid":       waJID,
		"whatsapp_push_name": waPushName,
		"app_version":        "dev-0.1",
		"last_error":         nil,
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(reqBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if config.XanoAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+config.XanoAuthToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func claimDueJob(httpClient *http.Client, config WorkerConfig) (*ClaimJob, error) {
	urlStr := fmt.Sprintf("%s/claim_due", strings.TrimSuffix(config.XanoBaseURL, "/"))
	reqBody := map[string]any{
		"branch_id":   config.BranchID,
		"device_code": config.DeviceCode,
		"max_jobs":    1,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(reqBytes))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if config.XanoAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+config.XanoAuthToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Try parsing as array
	var jobs []ClaimJob
	if err := json.Unmarshal(bodyBytes, &jobs); err == nil {
		if len(jobs) > 0 {
			return &jobs[0], nil
		}
		return nil, nil
	}

	// Try parsing as a single object
	var job ClaimJob
	if err := json.Unmarshal(bodyBytes, &job); err == nil && job.ID > 0 {
		return &job, nil
	}

	// Try parsing standard data wrapped responses (e.g. data.jobs[0], data[0], jobs[0])
	var mapResp map[string]any
	if err := json.Unmarshal(bodyBytes, &mapResp); err == nil {
		// Check for {"data": {"jobs": [...]}} or {"data": [...]}
		if dataVal, ok := mapResp["data"]; ok {
			if dataMap, ok := dataVal.(map[string]any); ok {
				if jobsVal, ok := dataMap["jobs"]; ok {
					jobsBytes, _ := json.Marshal(jobsVal)
					var subJobs []ClaimJob
					if err := json.Unmarshal(jobsBytes, &subJobs); err == nil && len(subJobs) > 0 {
						return &subJobs[0], nil
					}
				}
			}
			dataBytes, _ := json.Marshal(dataVal)
			var subJobs []ClaimJob
			if err := json.Unmarshal(dataBytes, &subJobs); err == nil && len(subJobs) > 0 {
				return &subJobs[0], nil
			}
		}
		// Check for {"jobs": [...]}
		if jobsVal, ok := mapResp["jobs"]; ok {
			jobsBytes, _ := json.Marshal(jobsVal)
			var subJobs []ClaimJob
			if err := json.Unmarshal(jobsBytes, &subJobs); err == nil && len(subJobs) > 0 {
				return &subJobs[0], nil
			}
		}
	}

	return nil, nil
}

func processClaimedJob(httpClient *http.Client, whatsAppClient *whatsmeow.Client, config WorkerConfig, job *ClaimJob) error {
	// 1. Check Development Mode Safety allowlist
	if config.DevelopmentMode {
		allowed := false
		cleanPhone := cleanPhoneNumber(job.RecipientPhone)
		for _, allowNum := range config.DevelopmentRecipientAllowlist {
			if cleanPhoneNumber(allowNum) == cleanPhone && cleanPhone != "" {
				allowed = true
				break
			}
		}

		if !allowed {
			log.Printf("[Worker] SAFETY BLOCK: Recipient phone %s is not in DEVELOPMENT_RECIPIENT_ALLOWLIST. Blocking send and marking failed.", job.RecipientPhone)
			err := markJobFailed(httpClient, config, job.ID, "RECIPIENT_NOT_IN_DEVELOPMENT_ALLOWLIST", "Recipient is blocked by development allowlist.", false)
			if err != nil {
				return fmt.Errorf("allowlist safety mark_failed failed: %v", err)
			}
			return nil
		}
	}

	// 2. Dispatch via whatsmeow
	log.Printf("[Worker] Dispatching message for job %d to %s", job.ID, job.RecipientJID)
	whatsappMsgID, err := sendWhatsAppMessage(whatsAppClient, job.RecipientJID, job.MessageSnapshot)
	if err != nil {
		log.Printf("[Worker] WhatsApp send failed for job %d: %v", job.ID, err)
		markErr := markJobFailed(httpClient, config, job.ID, "WHATSAPP_SEND_FAILED", err.Error(), true)
		if markErr != nil {
			return fmt.Errorf("send failed: %v; mark_failed also failed: %v", err, markErr)
		}
		return nil
	}

	// 3. Mark success
	log.Printf("[Worker] WhatsApp send succeeded. Message ID: %s. Marking sent in Xano...", whatsappMsgID)
	err = markJobSent(httpClient, config, job.ID, whatsappMsgID)
	if err != nil {
		return fmt.Errorf("mark_sent failed: %v", err)
	}

	log.Printf("[Worker] Job %d successfully processed and marked sent in Xano.", job.ID)
	return nil
}

func sendWhatsAppMessage(whatsAppClient *whatsmeow.Client, chatJIDStr string, text string) (string, error) {
	if whatsAppClient == nil {
		return "", fmt.Errorf("whatsmeow client is nil")
	}

	targetJID, err := types.ParseJID(chatJIDStr)
	if err != nil {
		return "", fmt.Errorf("invalid JID: %v", err)
	}

	var msg waProto.Message
	msg.Conversation = &text

	resp, err := whatsAppClient.SendMessage(context.Background(), targetJID, &msg)
	if err != nil {
		return "", err
	}

	// Emit chatSync event so local Electron window captures the message sent in background
	protoBytes, _ := protojson.Marshal(&msg)
	selfMsg := map[string]any{
		"key": map[string]any{
			"remoteJid": targetJID.String(),
			"fromMe":    true,
			"id":        resp.ID,
		},
		"messageTimestamp": resp.Timestamp.Unix(),
		"pushName":         "",
		"message":          json.RawMessage(protoBytes),
	}
	emitEvent("chatSync", []any{selfMsg})

	return resp.ID, nil
}

func markJobSent(httpClient *http.Client, config WorkerConfig, jobID int64, whatsappMessageID string) error {
	urlStr := fmt.Sprintf("%s/mark_sent", strings.TrimSuffix(config.XanoBaseURL, "/"))
	reqBody := map[string]any{
		"job_id":              jobID,
		"branch_id":           config.BranchID,
		"device_code":         config.DeviceCode,
		"whatsapp_message_id": whatsappMessageID,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(reqBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if config.XanoAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+config.XanoAuthToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func markJobFailed(httpClient *http.Client, config WorkerConfig, jobID int64, errorCode, errorMessage string, retryable bool) error {
	urlStr := fmt.Sprintf("%s/mark_failed", strings.TrimSuffix(config.XanoBaseURL, "/"))
	reqBody := map[string]any{
		"job_id":        jobID,
		"branch_id":     config.BranchID,
		"device_code":   config.DeviceCode,
		"error_code":    errorCode,
		"error_message": errorMessage,
		"retryable":     retryable,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer(reqBytes))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	if config.XanoAuthToken != "" {
		req.Header.Set("Authorization", "Bearer "+config.XanoAuthToken)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func cleanPhoneNumber(phone string) string {
	var sb strings.Builder
	for _, r := range phone {
		if r >= '0' && r <= '9' {
			sb.WriteRune(r)
		}
	}
	s := sb.String()
	if strings.HasPrefix(s, "0") && len(s) > 1 {
		s = "60" + s[1:]
	}
	return s
}

func getEnvString(key, defaultValue string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return defaultValue
	}
	b, err := strconv.ParseBool(val)
	if err != nil {
		return defaultValue
	}
	return b
}

func getEnvInt(key string, defaultValue int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return defaultValue
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}
	return i
}
