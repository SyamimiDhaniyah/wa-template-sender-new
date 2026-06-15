package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	client      *whatsmeow.Client
	profileId   = "default"
	appDataPath string
	qrState     = struct {
		sync.RWMutex
		latestCode    string
		watcherActive bool
	}{}
	historyState = struct {
		sync.Mutex
		oldestByChat             map[string]*types.MessageInfo
		lastFullHistoryRequestAt time.Time
	}{oldestByChat: make(map[string]*types.MessageInfo)}
)

type APIResponse struct {
	Ok      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
	Message string `json:"message,omitempty"`
	Data    any    `json:"data,omitempty"`
}

func sendJSON(w http.ResponseWriter, data any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func rememberHistoryAnchor(info *types.MessageInfo) {
	if info == nil || info.Chat.IsEmpty() || info.ID == "" || info.Timestamp.IsZero() {
		return
	}
	key := info.Chat.String()
	historyState.Lock()
	defer historyState.Unlock()
	current := historyState.oldestByChat[key]
	if current == nil || info.Timestamp.Before(current.Timestamp) {
		copyInfo := *info
		historyState.oldestByChat[key] = &copyInfo
	}
}

func getHistoryAnchor(chat types.JID) *types.MessageInfo {
	if chat.IsEmpty() {
		return nil
	}
	historyState.Lock()
	defer historyState.Unlock()
	current := historyState.oldestByChat[chat.String()]
	if current == nil {
		return nil
	}
	copyInfo := *current
	return &copyInfo
}

func buildHistoryMessageInfo(chatJID string, senderJID string, id string, fromMe bool, timestamp uint64) *types.MessageInfo {
	if chatJID == "" || id == "" || timestamp == 0 {
		return nil
	}
	chat, err := types.ParseJID(chatJID)
	if err != nil {
		return nil
	}
	sender := types.EmptyJID
	if senderJID != "" {
		if parsedSender, err := types.ParseJID(senderJID); err == nil {
			sender = parsedSender
		}
	}
	if sender.IsEmpty() && !fromMe {
		sender = chat
	}
	return &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chat,
			Sender:   sender,
			IsFromMe: fromMe,
			IsGroup:  chat.Server == types.GroupServer,
		},
		ID:        types.MessageID(id),
		Timestamp: time.Unix(int64(timestamp), 0),
	}
}

func requestRecentFullHistory(ctx context.Context) (bool, string, error) {
	if client == nil {
		return false, "", fmt.Errorf("Client not initialized")
	}
	historyState.Lock()
	if !historyState.lastFullHistoryRequestAt.IsZero() && time.Since(historyState.lastFullHistoryRequestAt) < 45*time.Second {
		historyState.Unlock()
		return false, "recent full history request already in progress", nil
	}
	historyState.lastFullHistoryRequestAt = time.Now()
	historyState.Unlock()

	requestID := fmt.Sprintf("whatsconect_%d", time.Now().UnixNano())
	fromTimestamp := uint64(time.Now().AddDate(0, 0, -7).Unix())
	durationDays := uint32(7)
	fullDaysLimit := uint32(7)
	fullSizeLimit := uint32(128)
	storageQuota := uint32(512)
	inlineInitial := true
	recentDaysLimit := uint32(7)
	onDemandReady := true
	completeOnDemandReady := true

	msg := &waProto.Message{
		ProtocolMessage: &waProto.ProtocolMessage{
			Type: waProto.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE.Enum(),
			PeerDataOperationRequestMessage: &waProto.PeerDataOperationRequestMessage{
				PeerDataOperationRequestType: waProto.PeerDataOperationRequestType_FULL_HISTORY_SYNC_ON_DEMAND.Enum(),
				FullHistorySyncOnDemandRequest: &waProto.PeerDataOperationRequestMessage_FullHistorySyncOnDemandRequest{
					RequestMetadata: &waProto.FullHistorySyncOnDemandRequestMetadata{
						RequestID:       proto.String(requestID),
						BusinessProduct: proto.String("WhatsConect"),
					},
					HistorySyncConfig: &waCompanionReg.DeviceProps_HistorySyncConfig{
						FullSyncDaysLimit:             proto.Uint32(fullDaysLimit),
						FullSyncSizeMbLimit:           proto.Uint32(fullSizeLimit),
						StorageQuotaMb:                proto.Uint32(storageQuota),
						InlineInitialPayloadInE2EeMsg: proto.Bool(inlineInitial),
						RecentSyncDaysLimit:           proto.Uint32(recentDaysLimit),
						OnDemandReady:                 proto.Bool(onDemandReady),
						CompleteOnDemandReady:         proto.Bool(completeOnDemandReady),
					},
					FullHistorySyncOnDemandConfig: &waProto.FullHistorySyncOnDemandConfig{
						HistoryFromTimestamp: proto.Uint64(fromTimestamp),
						HistoryDurationDays:  proto.Uint32(durationDays),
					},
				},
			},
		},
	}
	_, err := client.SendPeerMessage(ctx, msg)
	if err != nil {
		return false, "", err
	}
	return true, "requested recent full history", nil
}

func setLatestQRCode(code string) {
	qrState.Lock()
	qrState.latestCode = code
	qrState.Unlock()
}

func getLatestQRCode() string {
	qrState.RLock()
	defer qrState.RUnlock()
	return qrState.latestCode
}

func clearLatestQRCode() {
	setLatestQRCode("")
}

func setQRWatcherActive(active bool) {
	qrState.Lock()
	qrState.watcherActive = active
	qrState.Unlock()
}

func isQRWatcherActive() bool {
	qrState.RLock()
	defer qrState.RUnlock()
	return qrState.watcherActive
}

func watchQRChannel(qrChan <-chan whatsmeow.QRChannelItem) {
	setQRWatcherActive(true)
	go func() {
		defer setQRWatcherActive(false)
		for evt := range qrChan {
			switch evt.Event {
			case "code":
				setLatestQRCode(evt.Code)
				emitEvent("qr", evt.Code)
			default:
				clearLatestQRCode()
				emitEvent("qrStatus", evt.Event)
				return
			}
		}
		clearLatestQRCode()
	}()
}

func handleLoginQR(w http.ResponseWriter, r *http.Request) {
	if client.IsConnected() && client.IsLoggedIn() {
		sendJSON(w, APIResponse{Ok: false, Error: "Already logged in"}, 400)
		return
	}

	if latestCode := getLatestQRCode(); latestCode != "" {
		sendJSON(w, APIResponse{Ok: true, Data: latestCode}, 200)
		return
	}

	if client.Store.ID != nil {
		if client.IsConnected() {
			client.Disconnect()
			time.Sleep(300 * time.Millisecond)
		}
		clearLatestQRCode()
		err := client.Connect()
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
		sendJSON(w, APIResponse{Ok: true, Message: "Reconnecting existing WhatsApp session", Data: map[string]any{
			"mode":      "reconnect",
			"connected": client.IsConnected(),
			"loggedIn":  client.IsLoggedIn(),
			"jid":       client.Store.ID,
		}}, 200)
		return
	}

	shouldRestartQRFlow := !client.IsConnected() || !isQRWatcherActive()
	if shouldRestartQRFlow {
		if client.IsConnected() {
			client.Disconnect()
		}
		clearLatestQRCode()
		qrChan, err := client.GetQRChannel(context.Background())
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: "Failed to start QR channel: " + err.Error()}, 500)
			return
		}
		watchQRChannel(qrChan)
		err = client.Connect()
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
	}

	deadline := time.Now().Add(45 * time.Second)
	for {
		if latestCode := getLatestQRCode(); latestCode != "" {
			sendJSON(w, APIResponse{Ok: true, Data: latestCode}, 200)
			return
		}
		if time.Now().After(deadline) {
			sendJSON(w, APIResponse{Ok: false, Error: "Timed out waiting for QR code from WhatsApp servers"}, 504)
			return
		} else {
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func handleLoginPairing(w http.ResponseWriter, r *http.Request) {
	phone := r.URL.Query().Get("phone")
	if phone == "" {
		sendJSON(w, APIResponse{Ok: false, Error: "Missing phone number"}, 400)
		return
	}

	if client.IsConnected() && client.IsLoggedIn() {
		sendJSON(w, APIResponse{Ok: false, Error: "Already logged in"}, 400)
		return
	}

	if !client.IsConnected() {
		err := client.Connect()
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
	}

	code, err := client.PairPhone(context.Background(), phone, true, whatsmeow.PairClientChrome, "WhatsConect")
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}

	sendJSON(w, APIResponse{Ok: true, Data: code}, 200)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, APIResponse{
		Ok: true,
		Data: map[string]any{
			"connected": client.IsConnected(),
			"loggedIn":  client.IsLoggedIn(),
			"pushName":  client.Store.PushName,
			"jid":       client.Store.ID,
		},
	}, 200)
}

func handleContacts(w http.ResponseWriter, r *http.Request) {
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		sendJSON(w, APIResponse{Ok: true, Data: map[string]any{}}, 200)
		return
	}
	contacts, err := client.Store.Contacts.GetAllContacts(context.Background())
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}
	sendJSON(w, APIResponse{Ok: true, Data: contacts}, 200)
}
func handleChats(w http.ResponseWriter, r *http.Request) {
	if client == nil || client.Store == nil || client.Store.ChatSettings == nil {
		sendJSON(w, APIResponse{Ok: true, Data: map[string]any{}}, 200)
		return
	}
	sendJSON(w, APIResponse{Ok: true, Data: client.Store.ChatSettings}, 200)
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	err := client.Logout(context.Background())
	if err != nil {
		// If logout fails, it might be already logged out or connection error, try disconnect anyway
		client.Disconnect()
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}
	client.Disconnect()
	sendJSON(w, APIResponse{Ok: true, Message: "Logged out"}, 200)
}

func handleDisconnect(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	client.Disconnect()
	sendJSON(w, APIResponse{Ok: true, Message: "Disconnected"}, 200)
}

func handleMessages(w http.ResponseWriter, r *http.Request) {
	if client == nil || !client.IsConnected() {
		sendJSON(w, APIResponse{Ok: false, Error: "WhatsApp is not connected"}, 400)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fullRequested := strings.EqualFold(r.URL.Query().Get("full"), "true")
	limit := 50
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		if parsed, err := strconv.Atoi(rawLimit); err == nil && parsed > 0 {
			limit = parsed
		}
	}
	if limit > 80 {
		limit = 80
	}

	if fullRequested {
		triggered, message, err := requestRecentFullHistory(ctx)
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
		sendJSON(w, APIResponse{Ok: true, Message: message, Data: map[string]any{
			"syncTriggered": triggered,
			"mode":          "full_recent",
		}}, 200)
		return
	}

	chatJID := strings.TrimSpace(r.URL.Query().Get("chatJid"))
	if chatJID == "" {
		triggered, message, err := requestRecentFullHistory(ctx)
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
		sendJSON(w, APIResponse{Ok: true, Message: message, Data: map[string]any{
			"syncTriggered": triggered,
			"mode":          "full_recent",
		}}, 200)
		return
	}

	chat, err := types.ParseJID(chatJID)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Invalid chat JID"}, 400)
		return
	}

	anchor := getHistoryAnchor(chat)
	if anchor == nil {
		triggered, message, err := requestRecentFullHistory(ctx)
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
		sendJSON(w, APIResponse{Ok: true, Message: message, Data: map[string]any{
			"syncTriggered": triggered,
			"mode":          "full_recent_no_anchor",
			"chatJid":       chat.String(),
		}}, 200)
		return
	}

	req := client.BuildHistorySyncRequest(anchor, limit)
	_, err = client.SendPeerMessage(ctx, req)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}
	sendJSON(w, APIResponse{Ok: true, Message: "requested chat history", Data: map[string]any{
		"syncTriggered": true,
		"mode":          "chat_on_demand",
		"chatJid":       chat.String(),
		"anchorId":      anchor.ID,
	}}, 200)
}

func handleSend(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	var req struct {
		ChatJID    string `json:"chatJid"`
		Text       string `json:"text"`
		Attachment *struct {
			Path     string `json:"path"`
			FileName string `json:"fileName"`
			MimeType string `json:"mimeType"`
			Kind     string `json:"kind"`
		} `json:"attachment"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 400)
		return
	}
	targetJID, err := types.ParseJID(req.ChatJID)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Invalid JID"}, 400)
		return
	}

	var msg waProto.Message
	if req.Attachment != nil && req.Attachment.Path != "" {
		var data []byte
		var err error
		if strings.HasPrefix(strings.ToLower(req.Attachment.Path), "http://") || strings.HasPrefix(strings.ToLower(req.Attachment.Path), "https://") {
			resp, fetchErr := http.Get(req.Attachment.Path)
			if fetchErr != nil {
				sendJSON(w, APIResponse{Ok: false, Error: "Failed to download attachment: " + fetchErr.Error()}, 500)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				sendJSON(w, APIResponse{Ok: false, Error: fmt.Sprintf("Failed to download attachment: HTTP %d", resp.StatusCode)}, 500)
				return
			}
			data, err = io.ReadAll(resp.Body)
		} else {
			data, err = os.ReadFile(req.Attachment.Path)
		}
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: "Failed to read attachment: " + err.Error()}, 500)
			return
		}
		kind := strings.ToLower(strings.TrimSpace(req.Attachment.Kind))
		if kind == "" {
			kind = "image"
		}
		mediaType := whatsmeow.MediaImage
		switch kind {
		case "video":
			mediaType = whatsmeow.MediaVideo
		case "audio":
			mediaType = whatsmeow.MediaAudio
		case "document":
			mediaType = whatsmeow.MediaDocument
		}

		uploaded, err := client.Upload(context.Background(), data, mediaType)
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: "Failed to upload media: " + err.Error()}, 500)
			return
		}

		switch kind {
		case "image":
			msg.ImageMessage = &waProto.ImageMessage{
				URL:           &uploaded.URL,
				DirectPath:    &uploaded.DirectPath,
				MediaKey:      uploaded.MediaKey,
				Mimetype:      &req.Attachment.MimeType,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    &uploaded.FileLength,
				Caption:       &req.Text,
			}
		case "video":
			msg.VideoMessage = &waProto.VideoMessage{
				URL:           &uploaded.URL,
				DirectPath:    &uploaded.DirectPath,
				MediaKey:      uploaded.MediaKey,
				Mimetype:      &req.Attachment.MimeType,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    &uploaded.FileLength,
				Caption:       &req.Text,
			}
		case "audio":
			msg.AudioMessage = &waProto.AudioMessage{
				URL:           &uploaded.URL,
				DirectPath:    &uploaded.DirectPath,
				MediaKey:      uploaded.MediaKey,
				Mimetype:      &req.Attachment.MimeType,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    &uploaded.FileLength,
			}
		default: // document
			msg.DocumentMessage = &waProto.DocumentMessage{
				URL:           &uploaded.URL,
				DirectPath:    &uploaded.DirectPath,
				MediaKey:      uploaded.MediaKey,
				Mimetype:      &req.Attachment.MimeType,
				FileEncSHA256: uploaded.FileEncSHA256,
				FileSHA256:    uploaded.FileSHA256,
				FileLength:    &uploaded.FileLength,
				Caption:       &req.Text,
				FileName:      &req.Attachment.FileName,
			}
		}
	} else {
		msg.Conversation = &req.Text
	}

	resp, err := client.SendMessage(context.Background(), targetJID, &msg)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}

	// Emit chatSync so the local Electron cache captures this sent message immediately
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

	sendJSON(w, APIResponse{Ok: true, Data: resp}, 200)
}

func handlePresence(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	var req struct {
		ChatJID string `json:"chatJid"`
		Status  string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 400)
		return
	}

	p := types.Presence(req.Status)
	err := client.SendPresence(context.Background(), p)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}
	sendJSON(w, APIResponse{Ok: true}, 200)
}

func handleRead(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	var req struct {
		ChatJID   string `json:"chatJid"`
		MessageID string `json:"messageId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 400)
		return
	}
	targetJID, err := types.ParseJID(req.ChatJID)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Invalid JID"}, 400)
		return
	}

	err = client.MarkRead(context.Background(), []types.MessageID{types.MessageID(req.MessageID)}, time.Now(), targetJID, targetJID)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}
	sendJSON(w, APIResponse{Ok: true}, 200)
}

func handleOnWhatsApp(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	var req struct {
		Phones []string `json:"phones"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 400)
		return
	}
	resp, err := client.IsOnWhatsApp(context.Background(), req.Phones)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
		return
	}
	sendJSON(w, APIResponse{Ok: true, Data: resp}, 200)
}

func emitEvent(eventType string, data any) {
	b, _ := json.Marshal(map[string]any{"type": eventType, "data": data})
	fmt.Println(string(b))
}

func eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Message:
		rememberHistoryAnchor(&v.Info)
		protoBytes, _ := protojson.Marshal(v.Message)
		// Transform to Baileys format
		parsed := map[string]any{
			"key": map[string]any{
				"remoteJid":   v.Info.Chat.String(),
				"fromMe":      v.Info.IsFromMe,
				"id":          v.Info.ID,
				"participant": v.Info.Sender.String(),
			},
			"messageTimestamp": v.Info.Timestamp.Unix(),
			"pushName":         v.Info.PushName,
			"message":          json.RawMessage(protoBytes),
		}
		emitEvent("chatSync", []any{parsed})
	case *events.Receipt:
	case *events.Presence:
		// Not implemented yet
	case *events.HistorySync:
		var parsedChats []any
		var parsedMessages []any
		for _, conv := range v.Data.GetConversations() {
			chatID := conv.GetID()
			if chatID == "" {
				chatID = conv.GetNewJID()
			}
			if chatID == "" {
				chatID = conv.GetOldJID()
			}
			if chatID == "" {
				chatID = conv.GetPnJID()
			}
			if chatID == "" {
				chatID = conv.GetLidJID()
			}
			if chatID != "" {
				parsedChats = append(parsedChats, map[string]any{
					"id":                    chatID,
					"jid":                   chatID,
					"pnJid":                 conv.GetPnJID(),
					"lidJid":                conv.GetLidJID(),
					"name":                  conv.GetName(),
					"displayName":           conv.GetDisplayName(),
					"subject":               conv.GetName(),
					"conversationTimestamp": conv.GetConversationTimestamp(),
					"lastMessageTimestamp":  conv.GetLastMsgTimestamp(),
					"unreadCount":           conv.GetUnreadCount(),
					"archive":               conv.GetArchived(),
					"pinned":                conv.GetPinned() > 0,
					"muteEndTime":           conv.GetMuteEndTime(),
				})
			}
			for _, msg := range conv.GetMessages() {
				info := msg.GetMessage()
				if info == nil {
					continue
				}
				fromMe := info.GetKey().GetFromMe()
				id := info.GetKey().GetID()
				participant := info.GetKey().GetParticipant()
				remoteJid := info.GetKey().GetRemoteJID()
				if remoteJid == "" {
					remoteJid = chatID
				}
				pushName := info.GetPushName()
				if pushName == "" {
					pushName = conv.GetDisplayName()
				}
				if pushName == "" {
					pushName = conv.GetName()
				}
				timestamp := info.GetMessageTimestamp()
				if anchor := buildHistoryMessageInfo(remoteJid, participant, id, fromMe, timestamp); anchor != nil {
					rememberHistoryAnchor(anchor)
				}

				protoBytes, _ := protojson.Marshal(info.GetMessage())

				parsed := map[string]any{
					"key": map[string]any{
						"remoteJid":   remoteJid,
						"fromMe":      fromMe,
						"id":          id,
						"participant": participant,
					},
					"messageTimestamp": timestamp,
					"pushName":         pushName,
					"message":          json.RawMessage(protoBytes),
				}
				parsedMessages = append(parsedMessages, parsed)
			}
		}

		tmpBase := time.Now().UnixNano()
		chatFile := filepath.Join(os.TempDir(), fmt.Sprintf("hi_sync_chats_%d.json", tmpBase))
		msgFile := filepath.Join(os.TempDir(), fmt.Sprintf("hi_sync_msgs_%d.json", tmpBase))
		if f, err := os.Create(chatFile); err == nil {
			json.NewEncoder(f).Encode(parsedChats)
			f.Close()
		} else {
			chatFile = ""
		}
		if f, err := os.Create(msgFile); err == nil {
			json.NewEncoder(f).Encode(parsedMessages)
			f.Close()
		} else {
			msgFile = ""
		}
		emitEvent("historySync", map[string]any{
			"chatsFile": chatFile,
			"file":      msgFile,
			"chats":     len(parsedChats),
			"messages":  len(parsedMessages),
		})
	case *events.Connected:
		emitEvent("status", "connected")
	case *events.LoggedOut:
		emitEvent("status", "logged_out")
	}
}

func handleMediaDownload(w http.ResponseWriter, r *http.Request) {
	if client == nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Client not initialized"}, 400)
		return
	}
	var req struct {
		Message json.RawMessage `json:"message"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 400)
		return
	}
	var protoMsg waProto.Message
	if err := protojson.Unmarshal([]byte(req.Message), &protoMsg); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Failed to parse message: " + err.Error()}, 400)
		return
	}
	data, err := client.DownloadAny(context.Background(), &protoMsg)
	if err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Download failed: " + err.Error()}, 500)
		return
	}
	sendJSON(w, APIResponse{Ok: true, Data: base64.StdEncoding.EncodeToString(data)}, 200)
}

type ConfigRequest struct {
	XanoAuthToken string `json:"xano_auth_token"`
	WorkerEnabled bool   `json:"worker_enabled"`
	BranchID      int    `json:"branch_id,omitempty"`
	DeviceCode    string `json:"device_code,omitempty"`
}

func handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		sendJSON(w, APIResponse{Ok: false, Error: "Method not allowed"}, http.StatusMethodNotAllowed)
		return
	}

	var req ConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Failed to parse body: " + err.Error()}, http.StatusBadRequest)
		return
	}

	UpdateWorkerConfig(req.XanoAuthToken, req.WorkerEnabled, req.BranchID, req.DeviceCode)

	sendJSON(w, APIResponse{Ok: true, Message: "Worker configuration updated successfully"}, http.StatusOK)
}

func main() {
	var err error
	if envProfile := strings.TrimSpace(os.Getenv("WHATSCONECT_PROFILE_ID")); envProfile != "" {
		profileId = envProfile
	}
	configDir, err := os.UserConfigDir()
	if err != nil || configDir == "" {
		log.Fatalf("Failed to resolve user config directory: %v", err)
	}
	appDataPath = filepath.Join(configDir, "whatsconect", "wa_profiles", profileId)
	if err := os.MkdirAll(appDataPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create app data directory: %v", err)
	}

	dbPath := filepath.Join(appDataPath, "store.db")
	dbLog := waLog.Stdout("Database", "WARN", true)

	// Open raw pure-go SQLite driver using glebarez/sqlite
	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=foreign_keys(1)")
	if err != nil {
		log.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		log.Fatalf("Failed to enable SQLite foreign keys: %v", err)
	}

	// Use specific SQLite dialect for WhatsMeow to prevent pragma strict check panics
	container := sqlstore.NewWithDB(db, "sqlite", dbLog)

	// Initialize database schema (Creates whatsmeow_device, etc.)
	err = container.Upgrade(context.Background())
	if err != nil {
		log.Fatalf("Failed to upgrade database: %v", err)
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	clientLog := waLog.Stdout("Client", "INFO", true)

	// Set device to appear as Chrome in WhatsApp Linked Devices
	chromePlatform := waCompanionReg.DeviceProps_CHROME
	store.DeviceProps.PlatformType = &chromePlatform
	osName := "WhatsConect"
	store.DeviceProps.Os = &osName

	client = whatsmeow.NewClient(deviceStore, clientLog)
	client.AddEventHandler(eventHandler)

	if client.Store.ID != nil {
		err = client.Connect()
		if err != nil {
			log.Fatal(err)
		}
	}

	http.HandleFunc("/api/login/qr", handleLoginQR)
	http.HandleFunc("/api/login/pair", handleLoginPairing)
	http.HandleFunc("/api/status", handleStatus)
	http.HandleFunc("/api/contacts", handleContacts)
	http.HandleFunc("/api/chats", handleChats)
	http.HandleFunc("/api/messages", handleMessages)
	http.HandleFunc("/api/send", handleSend)
	http.HandleFunc("/api/presence", handlePresence)
	http.HandleFunc("/api/read", handleRead)
	http.HandleFunc("/api/onwhatsapp", handleOnWhatsApp)
	http.HandleFunc("/api/disconnect", handleDisconnect)
	http.HandleFunc("/api/logout", handleLogout)
	http.HandleFunc("/api/media/download", handleMediaDownload)
	http.HandleFunc("/api/config", handleConfig)

	StartAutomationWorker(client)

	go func() {
		if err := http.ListenAndServe(":12345", nil); err != nil {
			log.Fatal(err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	client.Disconnect()
}
