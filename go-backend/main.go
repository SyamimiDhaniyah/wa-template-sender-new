package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
)

var (
	client      *whatsmeow.Client
	profileId   = "default"
	appDataPath string
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

func handleLoginQR(w http.ResponseWriter, r *http.Request) {
	if client.IsConnected() && client.IsLoggedIn() {
		sendJSON(w, APIResponse{Ok: false, Error: "Already logged in"}, 400)
		return
	}

	if client.Store.ID != nil {
		sendJSON(w, APIResponse{Ok: false, Error: "Session exists. Please logout first or just reconnect."}, 400)
		return
	}

	qrChan, _ := client.GetQRChannel(context.Background())
	if !client.IsConnected() {
		err := client.Connect()
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: err.Error()}, 500)
			return
		}
	}

	// We only wait for the first QR code string or a timeout
	select {
	case evt := <-qrChan:
		if evt.Event == "code" {
			sendJSON(w, APIResponse{Ok: true, Data: evt.Code}, 200)
			return
		} else {
			sendJSON(w, APIResponse{Ok: false, Error: "QR Login failed / timeout: " + evt.Event}, 500)
			return
		}
	case <-time.After(15 * time.Second):
		sendJSON(w, APIResponse{Ok: false, Error: "Timed out waiting for QR code from WhatsApp servers"}, 504)
		return
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

func handleMessages(w http.ResponseWriter, r *http.Request) {
	// For now, this just acknowledges the request.
	// Real history is streamed via HistorySync events to stdout.
	sendJSON(w, APIResponse{Ok: true, Message: "History sync managed via stdout events"}, 200)
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
		data, err := os.ReadFile(req.Attachment.Path)
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: "Failed to read attachment: " + err.Error()}, 500)
			return
		}
		uploaded, err := client.Upload(context.Background(), data, whatsmeow.MediaImage) // Default to image for now, can be improved
		if err != nil {
			sendJSON(w, APIResponse{Ok: false, Error: "Failed to upload media: " + err.Error()}, 500)
			return
		}

		kind := req.Attachment.Kind
		if kind == "" {
			kind = "image"
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
		var parsedMessages []any
		for _, conv := range v.Data.GetConversations() {
			for _, msg := range conv.GetMessages() {
				info := msg.GetMessage()
				if info == nil {
					continue
				}
				fromMe := info.GetKey().GetFromMe()
				id := info.GetKey().GetID()
				participant := info.GetKey().GetParticipant()
				remoteJid := info.GetKey().GetRemoteJID()
				pushName := info.GetPushName()
				timestamp := info.GetMessageTimestamp()

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

		tmpFile := filepath.Join(os.TempDir(), fmt.Sprintf("hi_sync_%d.json", time.Now().UnixNano()))
		f, err := os.Create(tmpFile)
		if err == nil {
			json.NewEncoder(f).Encode(parsedMessages)
			f.Close()
			emitEvent("historySync", map[string]any{"file": tmpFile})
		}
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

func main() {
	var err error
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
	http.HandleFunc("/api/logout", handleLogout)
	http.HandleFunc("/api/media/download", handleMediaDownload)

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
