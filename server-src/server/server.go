package server

import (
	"bufio"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"evodb/engine"
	"evodb/protocol"
)

// ── Console colors ────────────────────────────────────────────────────────────
const (
	reset   = "\033[0m"
	bold    = "\033[1m"
	dim     = "\033[2m"
	italic  = "\033[3m"
	purple  = "\033[35m"
	cyan    = "\033[36m"
	green   = "\033[32m"
	yellow  = "\033[33m"
	red     = "\033[31m"
	wine    = "\033[38;5;88m"
	gray    = "\033[38;5;240m"
	white   = "\033[97m"
	bgWine  = "\033[48;5;88m"
)

// ── Auth ──────────────────────────────────────────────────────────────────────
const (
	adminUser = "admin"
	adminPass = "evodb2025"
	cookieName = "evodb_session"
	sessionToken = "evodb_authenticated_session_v1"
)

type Server struct {
	dbPath    string
	addr      string
	db        *engine.DB
	startTime time.Time
	queries   atomic.Int64
}

func New(dbPath string, addr string) *Server {
	return &Server{dbPath: dbPath, addr: addr}
}

func (s *Server) Start() error {
	db, err := engine.Open(s.dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	s.db = db
	s.startTime = time.Now()

	s.printBanner()

	// Checkpoint goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			if err := s.db.Checkpoint(); err != nil {
				fmt.Printf(yellow+"  ⚠  Checkpoint: %v\n"+reset, err)
			}
		}
	}()

	// TCP server
	go func() {
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			fmt.Printf(red+bold+"  ✗ TCP error: %v\n"+reset, err)
			return
		}
		defer ln.Close()
		fmt.Printf(green+"  ✦ "+reset+white+bold+"TCP"+reset+gray+"  ──  "+reset+cyan+"%s"+reset+"\n", s.addr)
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go s.handleConn(conn)
		}
	}()

	httpAddr := strings.Replace(s.addr, "7777", "7778", 1)
	if httpAddr == s.addr {
		httpAddr = ":7778"
	}

	fmt.Printf(green+"  ✦ "+reset+white+bold+"HTTP"+reset+gray+" ──  "+reset+cyan+"%s"+reset+"\n", httpAddr)
	fmt.Printf(gray+"  ─────────────────────────────────────────\n"+reset)
	fmt.Printf(wine+bold+"  ◈  EvoDB está listo para recibir consultas\n"+reset)
	fmt.Printf(gray+"  ─────────────────────────────────────────\n\n"+reset)

	// HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/login",      s.handleLogin)
	mux.HandleFunc("/logout",     s.handleLogout)
	mux.HandleFunc("/ping",       s.requireAuth(s.statusPage))
	mux.HandleFunc("/api/status", s.requireAuth(s.apiStatus))
	mux.HandleFunc("/query",      s.requireAuth(s.httpQuery))
	mux.HandleFunc("/api/table/", s.requireAuth(s.apiTableOps))
	mux.HandleFunc("/",           s.requireAuth(s.statusPage))

	return http.ListenAndServe(httpAddr, mux)
}

func (s *Server) printBanner() {
	fmt.Print("\n")
	fmt.Printf(wine+bold+"  ╔══════════════════════════════════════════╗\n"+reset)
	fmt.Printf(wine+bold+"  ║"+reset+white+bold+"        E V O D B  —  Database Server      "+wine+bold+"║\n"+reset)
	fmt.Printf(wine+bold+"  ╚══════════════════════════════════════════╝\n"+reset)
	fmt.Print("\n")
	fmt.Printf(gray+"  %-10s"+reset+cyan+" %s\n"+reset, "◆ DB", s.dbPath)
	fmt.Printf(gray+"  %-10s"+reset+cyan+" %s\n"+reset, "◆ Addr", s.addr)
	fmt.Print("\n")
}

// ── Auth middleware ───────────────────────────────────────────────────────────
func (s *Server) isAuthenticated(r *http.Request) bool {
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(sessionToken)) == 1
}

func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.isAuthenticated(r) {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		next(w, r)
	}
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.isAuthenticated(r) {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	errMsg := ""
	if r.Method == http.MethodPost {
		user := r.FormValue("username")
		pass := r.FormValue("password")
		validUser := subtle.ConstantTimeCompare([]byte(user), []byte(adminUser)) == 1
		validPass := subtle.ConstantTimeCompare([]byte(pass), []byte(adminPass)) == 1
		if validUser && validPass {
			http.SetCookie(w, &http.Cookie{
				Name:     cookieName,
				Value:    sessionToken,
				Path:     "/",
				HttpOnly: true,
				MaxAge:   86400 * 7,
			})
			http.Redirect(w, r, "/", http.StatusFound)
			return
		}
		errMsg = "Credenciales incorrectas"
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(s.loginPage(errMsg)))
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:   cookieName,
		Value:  "",
		Path:   "/",
		MaxAge: -1,
	})
	http.Redirect(w, r, "/login", http.StatusFound)
}

// ── API: table CRUD operations ────────────────────────────────────────────────
func (s *Server) apiTableOps(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	path := strings.TrimPrefix(r.URL.Path, "/api/table/")
	parts := strings.SplitN(path, "/", 2)
	table := parts[0]

	if table == "" {
		http.Error(w, `{"error":"missing table"}`, 400)
		return
	}

	switch r.Method {
	case http.MethodGet:
		// GET /api/table/:name — fetch all rows + schema
		rows, cols, err := s.db.Query(table, nil, nil, 0)
		if err != nil {
			w.Write([]byte(`{"error":"` + err.Error() + `"}`))
			return
		}
		schema, _ := s.db.GetSchema(table)
		colDefs := []map[string]interface{}{}
		for _, c := range schema {
			colDefs = append(colDefs, map[string]interface{}{
				"name": c.Name, "type": c.Type, "indexed": c.Indexed,
			})
		}
		rowData := []map[string]string{}
		for _, row := range rows {
			m := map[string]string{}
			for i, v := range row {
				if i < len(cols) {
					m[cols[i].Name] = v.String()
				}
			}
			rowData = append(rowData, m)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{
			"table":  table,
			"schema": colDefs,
			"rows":   rowData,
		})

	case http.MethodPost:
		// POST /api/table/:name — execute raw command
		body, _ := io.ReadAll(r.Body)
		var req struct{ Cmd string `json:"cmd"` }
		if err := json.Unmarshal(body, &req); err != nil || req.Cmd == "" {
			http.Error(w, `{"error":"invalid body"}`, 400)
			return
		}
		s.queries.Add(1)
		result := s.execute(req.Cmd)
		json.NewEncoder(w).Encode(map[string]string{"result": result})

	default:
		http.Error(w, `{"error":"method not allowed"}`, 405)
	}
}

// ── API status JSON ───────────────────────────────────────────────────────────
func (s *Server) apiStatus(w http.ResponseWriter, r *http.Request) {
	tables := s.db.ListTables()
	tableStats := []map[string]interface{}{}
	for _, t := range tables {
		count, _ := s.db.CountRows(t, "", nil)
		cols, _ := s.db.GetSchema(t)
		colNames := []string{}
		for _, c := range cols {
			colNames = append(colNames, c.Name)
		}
		tableStats = append(tableStats, map[string]interface{}{
			"name": t, "rows": count, "columns": colNames,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "online",
		"uptime":  time.Since(s.startTime).String(),
		"queries": s.queries.Load(),
		"tables":  tableStats,
		"db":      s.dbPath,
	})
}

// ── HTTP query endpoint ───────────────────────────────────────────────────────
func (s *Server) httpQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	var req struct {
		Cmd string `json:"cmd"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Cmd == "" {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	s.queries.Add(1)
	response := s.execute(req.Cmd)
	w.Header().Set("Content-Type", "application/json")
	w.Write(buildResponse(response))
}

func buildResponse(result string) []byte {
	b, _ := json.Marshal(result)
	return append(append([]byte(`{"result":`), b...), '}')
}

// ── TCP handler ───────────────────────────────────────────────────────────────
func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	addr := conn.RemoteAddr().String()
	fmt.Printf(green+"  [+] "+reset+cyan+"%-22s"+reset+gray+" conectado\n"+reset, addr)
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	writer := bufio.NewWriter(conn)
	sendLine := func(msg string) {
		writer.WriteString(msg + "\n")
		writer.Flush()
	}
	sendLine("EVODB 1.0 ready")
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		s.queries.Add(1)
		response := s.execute(line)
		sendLine(response)
	}
	fmt.Printf(yellow+"  [-] "+reset+gray+"%-22s"+reset+dim+" desconectado\n"+reset, addr)
}

// ── Execute query ─────────────────────────────────────────────────────────────
func (s *Server) execute(raw string) string {
	cmd, err := protocol.Parse(raw)
	if err != nil {
		return "ERR " + err.Error()
	}
	switch cmd.Type {
	case protocol.CmdForge:
		if err := s.db.ForgeTable(cmd.Table, cmd.Columns); err != nil {
			return "ERR " + err.Error()
		}
		return fmt.Sprintf("OK table %q forged", cmd.Table)
	case protocol.CmdPush:
		if err := s.db.PushRow(cmd.Table, cmd.Values); err != nil {
			return "ERR " + err.Error()
		}
		return "OK 1 row pushed"
	case protocol.CmdUpsert:
		if err := s.db.UpsertRow(cmd.Table, cmd.KeyCol, cmd.Values); err != nil {
			return "ERR " + err.Error()
		}
		return "OK upserted"
	case protocol.CmdPull:
		rows, cols, err := s.db.Query(cmd.Table, cmd.Conditions, cmd.OrderBy, cmd.Limit)
		if err != nil {
			return "ERR " + err.Error()
		}
		return formatRows(rows, cols)
	case protocol.CmdCount:
		var filterVal *engine.Value
		if cmd.WhereCol != "" {
			filterVal = &cmd.WhereVal
		}
		n, err := s.db.CountRows(cmd.Table, cmd.WhereCol, filterVal)
		if err != nil {
			return "ERR " + err.Error()
		}
		return fmt.Sprintf("OK %d", n)
	case protocol.CmdBurn:
		n, err := s.db.BurnRows(cmd.Table, cmd.WhereCol, cmd.WhereVal)
		if err != nil {
			return "ERR " + err.Error()
		}
		return fmt.Sprintf("OK %d row(s) burned", n)
	case protocol.CmdReforge:
		n, err := s.db.ReforgeRows(cmd.Table, cmd.WhereCol, cmd.WhereVal, cmd.SetCol, cmd.SetVal)
		if err != nil {
			return "ERR " + err.Error()
		}
		return fmt.Sprintf("OK %d row(s) reforged", n)
	case protocol.CmdDrop:
		if err := s.db.DropTable(cmd.Table); err != nil {
			return "ERR " + err.Error()
		}
		return fmt.Sprintf("OK table %q dropped", cmd.Table)
	case protocol.CmdTables:
		tables := s.db.ListTables()
		if len(tables) == 0 {
			return "OK (no tables)"
		}
		return "OK " + strings.Join(tables, ", ")
	case protocol.CmdSchema:
		cols, err := s.db.GetSchema(cmd.Table)
		if err != nil {
			return "ERR " + err.Error()
		}
		var parts []string
		for _, col := range cols {
			entry := fmt.Sprintf("%s %s", col.Name, col.Type)
			if col.Indexed {
				entry += " INDEX"
			}
			parts = append(parts, entry)
		}
		return "OK " + strings.Join(parts, ", ")
	case protocol.CmdIndex:
		if err := s.db.AddIndex(cmd.Table, cmd.KeyCol); err != nil {
			return "ERR " + err.Error()
		}
		return fmt.Sprintf("OK index added on %s.%s", cmd.Table, cmd.KeyCol)
	default:
		return "ERR unknown command"
	}
}

func formatRows(rows []engine.Row, cols []engine.Column) string {
	if len(rows) == 0 {
		return "OK []"
	}
	var sb strings.Builder
	sb.WriteString("OK [")
	for ri, row := range rows {
		if ri > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("{")
		for i, v := range row {
			if i > 0 {
				sb.WriteString(",")
			}
			colName, _ := json.Marshal(cols[i].Name)
			sb.Write(colName)
			sb.WriteString(":")
			if cols[i].Type == engine.TypeJSON {
				sb.WriteString(v.String())
			} else {
				b, _ := json.Marshal(v.String())
				sb.Write(b)
			}
		}
		sb.WriteString("}")
	}
	sb.WriteString("]")
	return sb.String()
}

// ── Login page HTML ───────────────────────────────────────────────────────────
func (s *Server) loginPage(errMsg string) string {
	errHTML := ""
	if errMsg != "" {
		errHTML = `<div class="error-msg">` + errMsg + `</div>`
	}
	return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>EvoDB — Login</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg:       #0a0a0b;
  --surface:  #111114;
  --border:   #1e1e24;
  --wine:     #6b0f1a;
  --wine-dim: rgba(107,15,26,0.18);
  --wine-glow:rgba(107,15,26,0.45);
  --red:      #c0392b;
  --text:     #e8e8ed;
  --muted:    #52525e;
  --mono:     'Space Mono', monospace;
  --sans:     'Syne', sans-serif;
}
*, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }
html, body {
  height: 100%;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  display: flex;
  align-items: center;
  justify-content: center;
}
body::before {
  content: '';
  position: fixed;
  inset: 0;
  background:
    radial-gradient(ellipse 60% 50% at 50% 0%, rgba(107,15,26,0.15) 0%, transparent 70%),
    repeating-linear-gradient(0deg, transparent, transparent 39px, rgba(255,255,255,0.015) 40px),
    repeating-linear-gradient(90deg, transparent, transparent 39px, rgba(255,255,255,0.015) 40px);
  pointer-events: none;
  z-index: 0;
}
.wrap {
  position: relative;
  z-index: 1;
  width: 100%;
  max-width: 400px;
  padding: 20px;
}
.brand {
  text-align: center;
  margin-bottom: 40px;
}
.brand-logo {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 64px; height: 64px;
  background: var(--wine-dim);
  border: 1px solid var(--wine);
  border-radius: 16px;
  margin-bottom: 16px;
  box-shadow: 0 0 30px var(--wine-glow);
}
.brand-logo svg { width: 32px; height: 32px; fill: #c0392b; }
.brand h1 {
  font-family: var(--mono);
  font-size: 1.6rem;
  font-weight: 700;
  letter-spacing: -1px;
  color: var(--text);
}
.brand h1 span { color: #c0392b; }
.brand p {
  margin-top: 6px;
  font-size: 0.8rem;
  color: var(--muted);
  font-family: var(--mono);
  letter-spacing: 2px;
  text-transform: uppercase;
}
.card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 36px;
  box-shadow: 0 20px 60px rgba(0,0,0,0.5);
}
.field {
  margin-bottom: 20px;
}
label {
  display: block;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--muted);
  font-family: var(--mono);
  margin-bottom: 8px;
}
input {
  width: 100%;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 16px;
  font-family: var(--mono);
  font-size: 0.95rem;
  color: var(--text);
  outline: none;
  transition: border-color 0.2s, box-shadow 0.2s;
}
input:focus {
  border-color: var(--wine);
  box-shadow: 0 0 0 3px var(--wine-dim);
}
.error-msg {
  background: rgba(192,57,43,0.12);
  border: 1px solid rgba(192,57,43,0.3);
  color: #e74c3c;
  border-radius: 8px;
  padding: 10px 14px;
  font-size: 0.82rem;
  font-family: var(--mono);
  margin-bottom: 20px;
  text-align: center;
}
button {
  width: 100%;
  background: var(--wine);
  border: none;
  border-radius: 8px;
  padding: 14px;
  font-family: var(--mono);
  font-size: 0.9rem;
  font-weight: 700;
  color: #fff;
  cursor: pointer;
  letter-spacing: 2px;
  text-transform: uppercase;
  transition: background 0.2s, box-shadow 0.2s, transform 0.1s;
  margin-top: 8px;
  box-shadow: 0 4px 20px var(--wine-glow);
}
button:hover { background: #8b1020; box-shadow: 0 6px 30px var(--wine-glow); }
button:active { transform: scale(0.98); }
.footer-note {
  text-align: center;
  margin-top: 24px;
  font-size: 0.72rem;
  color: var(--muted);
  font-family: var(--mono);
}
</style>
</head>
<body>
<div class="wrap">
  <div class="brand">
    <div class="brand-logo">
      <svg viewBox="0 0 24 24"><path d="M20 6H4c-1.1 0-2 .9-2 2v8c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm-5 7H9v-2h6v2zm3-4H6V7h12v2z"/></svg>
    </div>
    <h1>EVO<span>DB</span></h1>
    <p>Database Server</p>
  </div>
  <div class="card">
    ` + errHTML + `
    <form method="POST" action="/login">
      <div class="field">
        <label>Usuario</label>
        <input type="text" name="username" placeholder="admin" autocomplete="username" autofocus>
      </div>
      <div class="field">
        <label>Contraseña</label>
        <input type="password" name="password" placeholder="••••••••" autocomplete="current-password">
      </div>
      <button type="submit">Acceder</button>
    </form>
  </div>
  <p class="footer-note">EvoDB — Acceso restringido</p>
</div>
</body>
</html>`
}

// ── Dashboard HTML ────────────────────────────────────────────────────────────
func (s *Server) statusPage(w http.ResponseWriter, r *http.Request) {
	tables := s.db.ListTables()
	uptime := time.Since(s.startTime)
	hours := int(uptime.Hours())
	minutes := int(uptime.Minutes()) % 60
	seconds := int(uptime.Seconds()) % 60

	totalRows := 0
	type tableInfo struct {
		Name string
		Rows int
		Cols []engine.Column
	}
	tableData := []tableInfo{}
	for _, t := range tables {
		count, _ := s.db.CountRows(t, "", nil)
		cols, _ := s.db.GetSchema(t)
		totalRows += count
		tableData = append(tableData, tableInfo{Name: t, Rows: count, Cols: cols})
	}

	tableListItems := ""
	for _, t := range tableData {
		tableListItems += fmt.Sprintf(
			`<div class="tbl-item" onclick="loadTable('%s')" id="titem-%s">
				<span class="tbl-icon">⬡</span>
				<span class="tbl-name">%s</span>
				<span class="tbl-rows">%d</span>
			</div>`, t.Name, t.Name, t.Name, t.Rows)
	}
	if tableListItems == "" {
		tableListItems = `<div class="no-tables">Sin tablas</div>`
	}

	uptimeStr := fmt.Sprintf("%02dh %02dm %02ds", hours, minutes, seconds)
	queryCount := s.queries.Load()

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, dashboardHTML,
		s.dbPath,
		len(tables),
		totalRows,
		uptimeStr,
		queryCount,
		tableListItems,
		s.dbPath,
	)
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>EvoDB Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap" rel="stylesheet">
<style>
:root {
  --bg:        #09090b;
  --surface:   #0f0f12;
  --surface2:  #141418;
  --border:    #1c1c22;
  --border2:   #26262e;
  --wine:      #6b0f1a;
  --wine-b:    #8b1020;
  --wine-dim:  rgba(107,15,26,0.15);
  --wine-glow: rgba(107,15,26,0.4);
  --red:       #c0392b;
  --red-bright:#e74c3c;
  --text:      #e8e8ed;
  --text2:     #a0a0aa;
  --muted:     #4a4a56;
  --green:     #27ae60;
  --mono:      'Space Mono', monospace;
  --sans:      'Syne', sans-serif;
}
*, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }
html { height: 100%; }
body {
  min-height: 100%;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  display: flex;
  flex-direction: column;
}

/* ── Sidebar ── */
.layout { display: flex; height: 100vh; overflow: hidden; }
.sidebar {
  width: 240px;
  min-width: 240px;
  background: var(--surface);
  border-right: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.sidebar-brand {
  padding: 24px 20px 20px;
  border-bottom: 1px solid var(--border);
}
.brand-row {
  display: flex;
  align-items: center;
  gap: 10px;
}
.brand-icon {
  width: 36px; height: 36px;
  background: var(--wine-dim);
  border: 1px solid var(--wine);
  border-radius: 8px;
  display: flex; align-items: center; justify-content: center;
  box-shadow: 0 0 16px var(--wine-glow);
}
.brand-icon svg { width: 18px; height: 18px; fill: var(--red); }
.brand-name {
  font-family: var(--mono);
  font-size: 1rem;
  font-weight: 700;
  letter-spacing: -0.5px;
}
.brand-name span { color: var(--red); }
.brand-db {
  margin-top: 8px;
  font-family: var(--mono);
  font-size: 0.68rem;
  color: var(--muted);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.sidebar-section {
  padding: 16px 12px 8px;
}
.sidebar-label {
  font-size: 0.6rem;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--muted);
  font-family: var(--mono);
  padding: 0 8px;
  margin-bottom: 8px;
}
.tbl-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  cursor: pointer;
  transition: background 0.15s;
  border: 1px solid transparent;
}
.tbl-item:hover { background: var(--surface2); border-color: var(--border2); }
.tbl-item.active { background: var(--wine-dim); border-color: var(--wine); }
.tbl-icon { font-size: 0.7rem; color: var(--muted); }
.tbl-name { flex: 1; font-family: var(--mono); font-size: 0.82rem; color: var(--text); }
.tbl-rows {
  font-family: var(--mono);
  font-size: 0.65rem;
  background: var(--surface2);
  color: var(--text2);
  padding: 2px 6px;
  border-radius: 4px;
  border: 1px solid var(--border2);
}
.tbl-item.active .tbl-rows { background: var(--wine-dim); color: var(--red); border-color: var(--wine); }
.no-tables {
  padding: 12px 10px;
  font-size: 0.78rem;
  color: var(--muted);
  font-family: var(--mono);
  font-style: italic;
}

.sidebar-footer {
  margin-top: auto;
  padding: 16px 12px;
  border-top: 1px solid var(--border);
}
.logout-btn {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;
  padding: 8px 10px;
  background: none;
  border: 1px solid var(--border2);
  border-radius: 6px;
  color: var(--text2);
  font-family: var(--mono);
  font-size: 0.75rem;
  cursor: pointer;
  text-decoration: none;
  transition: all 0.15s;
}
.logout-btn:hover { border-color: var(--wine); color: var(--red); background: var(--wine-dim); }

/* ── Main ── */
.main {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.topbar {
  padding: 16px 28px;
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: var(--surface);
}
.topbar-title {
  font-size: 1rem;
  font-weight: 600;
  color: var(--text);
}
.topbar-title span { color: var(--muted); font-weight: 400; margin-left: 6px; }
.status-dot {
  display: flex;
  align-items: center;
  gap: 6px;
  font-family: var(--mono);
  font-size: 0.72rem;
  color: var(--green);
}
.status-dot::before {
  content: '';
  width: 7px; height: 7px;
  background: var(--green);
  border-radius: 50%;
  box-shadow: 0 0 8px var(--green);
  animation: pulse 2s infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.4; }
}

/* ── Stats ── */
.stats-row {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 1px;
  background: var(--border);
  border-bottom: 1px solid var(--border);
}
.stat {
  padding: 20px 24px;
  background: var(--surface);
}
.stat-label {
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--muted);
  font-family: var(--mono);
}
.stat-val {
  font-family: var(--mono);
  font-size: 1.5rem;
  font-weight: 700;
  margin-top: 4px;
  color: var(--text);
}
.stat-val.wine { color: var(--red); }

/* ── Content area ── */
.content {
  flex: 1;
  overflow: auto;
  padding: 28px;
  background:
    radial-gradient(ellipse 80% 40% at 50% 0%, rgba(107,15,26,0.06) 0%, transparent 60%);
}

/* ── Welcome screen ── */
.welcome {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  text-align: center;
  color: var(--muted);
}
.welcome-icon {
  font-size: 3rem;
  margin-bottom: 16px;
  opacity: 0.3;
}
.welcome h2 {
  font-family: var(--mono);
  font-size: 0.9rem;
  color: var(--muted);
  font-weight: 400;
}

/* ── Table view ── */
.table-view { display: none; }
.table-view.visible { display: block; }
.tv-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 20px;
}
.tv-title {
  display: flex;
  align-items: center;
  gap: 10px;
}
.tv-title h2 {
  font-family: var(--mono);
  font-size: 1.1rem;
  color: var(--text);
}
.tv-count {
  font-family: var(--mono);
  font-size: 0.7rem;
  background: var(--wine-dim);
  color: var(--red);
  padding: 3px 8px;
  border-radius: 4px;
  border: 1px solid var(--wine);
}
.tv-actions { display: flex; gap: 8px; }

.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 8px 14px;
  border-radius: 6px;
  font-family: var(--mono);
  font-size: 0.75rem;
  font-weight: 700;
  cursor: pointer;
  border: 1px solid;
  transition: all 0.15s;
  text-transform: uppercase;
  letter-spacing: 1px;
}
.btn-primary {
  background: var(--wine);
  border-color: var(--wine-b);
  color: #fff;
  box-shadow: 0 2px 12px var(--wine-glow);
}
.btn-primary:hover { background: var(--wine-b); box-shadow: 0 4px 20px var(--wine-glow); }
.btn-ghost {
  background: transparent;
  border-color: var(--border2);
  color: var(--text2);
}
.btn-ghost:hover { border-color: var(--wine); color: var(--red); background: var(--wine-dim); }
.btn-danger {
  background: transparent;
  border-color: rgba(192,57,43,0.3);
  color: var(--red-bright);
}
.btn-danger:hover { background: rgba(192,57,43,0.1); border-color: var(--red); }

/* ── Data table ── */
.data-table-wrap {
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}
.data-table {
  width: 100%;
  border-collapse: collapse;
  font-family: var(--mono);
  font-size: 0.8rem;
}
.data-table th {
  padding: 11px 16px;
  text-align: left;
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--muted);
  background: var(--surface2);
  border-bottom: 1px solid var(--border);
  white-space: nowrap;
}
.data-table td {
  padding: 10px 16px;
  border-bottom: 1px solid var(--border);
  color: var(--text2);
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.data-table tr:last-child td { border-bottom: none; }
.data-table tr:hover td { background: var(--surface2); color: var(--text); }
.td-actions { display: flex; gap: 6px; }
.td-btn {
  padding: 3px 8px;
  font-family: var(--mono);
  font-size: 0.65rem;
  border-radius: 4px;
  cursor: pointer;
  border: 1px solid;
  transition: all 0.12s;
  background: transparent;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}
.td-btn-edit { border-color: var(--border2); color: var(--text2); }
.td-btn-edit:hover { border-color: var(--wine); color: var(--red); }
.td-btn-del { border-color: rgba(192,57,43,0.2); color: rgba(231,76,60,0.6); }
.td-btn-del:hover { border-color: var(--red); color: var(--red-bright); background: rgba(192,57,43,0.1); }

.empty-table {
  text-align: center;
  padding: 48px;
  color: var(--muted);
  font-family: var(--mono);
  font-size: 0.8rem;
}

/* ── Query terminal ── */
.query-terminal {
  margin-top: 24px;
  border: 1px solid var(--border);
  border-radius: 10px;
  overflow: hidden;
  background: var(--surface);
}
.qt-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 16px;
  background: var(--surface2);
  border-bottom: 1px solid var(--border);
}
.qt-label {
  font-family: var(--mono);
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--muted);
}
.qt-body { padding: 12px; display: flex; gap: 8px; }
.qt-input {
  flex: 1;
  background: var(--bg);
  border: 1px solid var(--border2);
  border-radius: 6px;
  padding: 10px 14px;
  font-family: var(--mono);
  font-size: 0.82rem;
  color: var(--text);
  outline: none;
  transition: border-color 0.2s;
}
.qt-input:focus { border-color: var(--wine); }
.qt-run {
  padding: 10px 16px;
  background: var(--wine);
  border: 1px solid var(--wine-b);
  border-radius: 6px;
  color: #fff;
  font-family: var(--mono);
  font-size: 0.75rem;
  font-weight: 700;
  cursor: pointer;
  text-transform: uppercase;
  letter-spacing: 1px;
  transition: all 0.15s;
  white-space: nowrap;
}
.qt-run:hover { background: var(--wine-b); }
.qt-result {
  margin: 0 12px 12px;
  padding: 12px 14px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 6px;
  font-family: var(--mono);
  font-size: 0.78rem;
  color: var(--text2);
  display: none;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 160px;
  overflow: auto;
}
.qt-result.ok { color: #27ae60; border-color: rgba(39,174,96,0.3); }
.qt-result.err { color: var(--red-bright); border-color: rgba(192,57,43,0.3); }
.qt-result.show { display: block; }

/* ── Modal ── */
.modal-bg {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.75);
  backdrop-filter: blur(4px);
  z-index: 100;
  align-items: center;
  justify-content: center;
}
.modal-bg.open { display: flex; }
.modal {
  background: var(--surface);
  border: 1px solid var(--border2);
  border-radius: 14px;
  padding: 28px;
  width: 100%;
  max-width: 480px;
  box-shadow: 0 24px 80px rgba(0,0,0,0.7), 0 0 0 1px var(--border);
  animation: slideUp 0.2s ease;
}
@keyframes slideUp {
  from { opacity: 0; transform: translateY(16px); }
  to   { opacity: 1; transform: translateY(0); }
}
.modal-title {
  font-family: var(--mono);
  font-size: 0.9rem;
  font-weight: 700;
  color: var(--text);
  margin-bottom: 20px;
  padding-bottom: 14px;
  border-bottom: 1px solid var(--border);
}
.modal-field { margin-bottom: 14px; }
.modal-field label {
  display: block;
  font-size: 0.65rem;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--muted);
  font-family: var(--mono);
  margin-bottom: 6px;
}
.modal-field input, .modal-field select {
  width: 100%;
  background: var(--bg);
  border: 1px solid var(--border2);
  border-radius: 6px;
  padding: 9px 12px;
  font-family: var(--mono);
  font-size: 0.85rem;
  color: var(--text);
  outline: none;
  transition: border-color 0.2s;
}
.modal-field input:focus, .modal-field select:focus { border-color: var(--wine); }
.modal-field select option { background: var(--surface2); }
.modal-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid var(--border);
}
.toast {
  position: fixed;
  bottom: 24px;
  right: 24px;
  background: var(--surface2);
  border: 1px solid var(--border2);
  border-radius: 8px;
  padding: 12px 18px;
  font-family: var(--mono);
  font-size: 0.78rem;
  color: var(--text);
  z-index: 200;
  transform: translateY(80px);
  opacity: 0;
  transition: all 0.3s;
  pointer-events: none;
}
.toast.show { transform: translateY(0); opacity: 1; }
.toast.ok { border-color: rgba(39,174,96,0.4); color: #2ecc71; }
.toast.err { border-color: rgba(192,57,43,0.4); color: var(--red-bright); }
</style>
</head>
<body>
<div class="layout">

  <!-- Sidebar -->
  <aside class="sidebar">
    <div class="sidebar-brand">
      <div class="brand-row">
        <div class="brand-icon">
          <svg viewBox="0 0 24 24"><path d="M20 6H4c-1.1 0-2 .9-2 2v8c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2zm-5 7H9v-2h6v2zm3-4H6V7h12v2z"/></svg>
        </div>
        <span class="brand-name">EVO<span>DB</span></span>
      </div>
      <div class="brand-db">%s</div>
    </div>
    <div class="sidebar-section">
      <div class="sidebar-label">Tablas</div>
      <div id="tableList">%s</div>
    </div>
    <div class="sidebar-footer">
      <a href="/logout" class="logout-btn">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor"><path d="M17 7l-1.41 1.41L18.17 11H8v2h10.17l-2.58 2.58L17 17l5-5zM4 5h8V3H4c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h8v-2H4V5z"/></svg>
        Cerrar sesión
      </a>
    </div>
  </aside>

  <!-- Main -->
  <main class="main">
    <div class="topbar">
      <div class="topbar-title">Dashboard <span>/ EvoDB</span></div>
      <div class="status-dot">Online</div>
    </div>

    <div class="stats-row">
      <div class="stat">
        <div class="stat-label">Tablas</div>
        <div class="stat-val wine">%d</div>
      </div>
      <div class="stat">
        <div class="stat-label">Registros</div>
        <div class="stat-val">%d</div>
      </div>
      <div class="stat">
        <div class="stat-label">Uptime</div>
        <div class="stat-val" style="font-size:1.1rem">%s</div>
      </div>
      <div class="stat">
        <div class="stat-label">Queries</div>
        <div class="stat-val">%d</div>
      </div>
    </div>

    <div class="content">
      <div class="welcome" id="welcomeScreen">
        <div class="welcome-icon">⬡</div>
        <h2>Selecciona una tabla para explorar</h2>
      </div>

      <div class="table-view" id="tableView">
        <div class="tv-header">
          <div class="tv-title">
            <h2 id="tvName">—</h2>
            <span class="tv-count" id="tvCount">0 rows</span>
          </div>
          <div class="tv-actions">
            <button class="btn btn-ghost" onclick="refreshTable()">↺ Refresh</button>
            <button class="btn btn-primary" onclick="openInsertModal()">+ Insert</button>
          </div>
        </div>

        <div class="data-table-wrap">
          <table class="data-table">
            <thead id="tvHead"></thead>
            <tbody id="tvBody"></tbody>
          </table>
        </div>

        <div class="query-terminal">
          <div class="qt-header">
            <span class="qt-label">◈ Terminal de consultas</span>
          </div>
          <div class="qt-body">
            <input class="qt-input" id="qtInput" placeholder="PULL users WHERE xp > 100" onkeydown="if(event.key==='Enter')runQuery()">
            <button class="qt-run" onclick="runQuery()">Run</button>
          </div>
          <div class="qt-result" id="qtResult"></div>
        </div>
      </div>
    </div>
  </main>
</div>

<!-- Insert/Edit Modal -->
<div class="modal-bg" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-title" id="modalTitle">Insertar fila</div>
    <div id="modalFields"></div>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeModal()">Cancelar</button>
      <button class="btn btn-primary" onclick="submitModal()" id="modalSubmit">Insertar</button>
    </div>
  </div>
</div>

<!-- Toast -->
<div class="toast" id="toast"></div>

<script>
let currentTable = null;
let currentSchema = [];
let currentRows = [];
let editRowIndex = null;

function showToast(msg, type='ok') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 2500);
}

async function loadTable(name) {
  document.querySelectorAll('.tbl-item').forEach(el => el.classList.remove('active'));
  const item = document.getElementById('titem-' + name);
  if (item) item.classList.add('active');

  currentTable = name;
  document.getElementById('welcomeScreen').style.display = 'none';
  document.getElementById('tableView').style.display = 'block';
  document.getElementById('tvName').textContent = name;

  try {
    const res = await fetch('/api/table/' + name);
    const data = await res.json();
    currentSchema = data.schema || [];
    currentRows = data.rows || [];
    renderTable();
    document.getElementById('qtInput').placeholder = 'PULL ' + name + ' LIMIT 10';
  } catch(e) {
    showToast('Error al cargar tabla', 'err');
  }
}

function renderTable() {
  const head = document.getElementById('tvHead');
  const body = document.getElementById('tvBody');
  document.getElementById('tvCount').textContent = currentRows.length + ' rows';

  if (!currentSchema.length) {
    head.innerHTML = '';
    body.innerHTML = '<tr><td colspan="99"><div class="empty-table">Sin esquema definido</div></td></tr>';
    return;
  }

  head.innerHTML = '<tr>' +
    currentSchema.map(c =>
      '<th>' + c.name + ' <small style="opacity:.5">' + c.type + (c.indexed ? ' ⬡' : '') + '</small></th>'
    ).join('') +
    '<th style="width:100px">Acciones</th></tr>';

  if (!currentRows.length) {
    body.innerHTML = '<tr><td colspan="' + (currentSchema.length+1) + '"><div class="empty-table">Sin registros</div></td></tr>';
    return;
  }

  body.innerHTML = currentRows.map((row, i) =>
    '<tr>' +
    currentSchema.map(c => '<td title="' + (row[c.name]||'') + '">' + (row[c.name]||'<span style="opacity:.3">null</span>') + '</td>').join('') +
    '<td><div class="td-actions">' +
    '<button class="td-btn td-btn-edit" onclick="openEditModal(' + i + ')">Edit</button>' +
    '<button class="td-btn td-btn-del" onclick="deleteRow(' + i + ')">Del</button>' +
    '</div></td></tr>'
  ).join('');
}

async function refreshTable() {
  if (currentTable) await loadTable(currentTable);
}

function openInsertModal() {
  editRowIndex = null;
  document.getElementById('modalTitle').textContent = 'Insertar fila — ' + currentTable;
  document.getElementById('modalSubmit').textContent = 'Insertar';
  const fields = currentSchema.map(c =>
    '<div class="modal-field">' +
    '<label>' + c.name + ' <span style="color:var(--wine);margin-left:4px">' + c.type + '</span></label>' +
    '<input id="mf_' + c.name + '" placeholder="' + c.name + '">' +
    '</div>'
  ).join('');
  document.getElementById('modalFields').innerHTML = fields;
  document.getElementById('modal').classList.add('open');
  if (currentSchema[0]) document.getElementById('mf_' + currentSchema[0].name)?.focus();
}

function openEditModal(i) {
  editRowIndex = i;
  const row = currentRows[i];
  document.getElementById('modalTitle').textContent = 'Editar fila — ' + currentTable;
  document.getElementById('modalSubmit').textContent = 'Guardar';
  const fields = currentSchema.map(c =>
    '<div class="modal-field">' +
    '<label>' + c.name + ' <span style="color:var(--wine);margin-left:4px">' + c.type + '</span></label>' +
    '<input id="mf_' + c.name + '" value="' + (row[c.name]||'') + '">' +
    '</div>'
  ).join('');
  document.getElementById('modalFields').innerHTML = fields;
  document.getElementById('modal').classList.add('open');
}

function closeModal() {
  document.getElementById('modal').classList.remove('open');
}

async function submitModal() {
  const vals = currentSchema.map(c => {
    const v = document.getElementById('mf_' + c.name)?.value || '';
    return JSON.stringify(v);
  });

  let cmd;
  if (editRowIndex !== null) {
    // Use UPSERT with first indexed col as key
    const keyCol = currentSchema.find(c => c.indexed) || currentSchema[0];
    if (!keyCol) { showToast('Sin columna clave', 'err'); return; }
    cmd = 'UPSERT ' + currentTable + ' KEY ' + keyCol.name + ' (' + vals.join(', ') + ')';
  } else {
    cmd = 'PUSH ' + currentTable + ' (' + vals.join(', ') + ')';
  }

  try {
    const res = await fetch('/api/table/' + currentTable, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cmd }),
    });
    const data = await res.json();
    if (data.result?.startsWith('ERR')) {
      showToast(data.result, 'err');
    } else {
      showToast(editRowIndex !== null ? 'Fila actualizada' : 'Fila insertada');
      closeModal();
      await refreshTable();
    }
  } catch(e) {
    showToast('Error de red', 'err');
  }
}

async function deleteRow(i) {
  const row = currentRows[i];
  const keyCol = currentSchema.find(c => c.indexed) || currentSchema[0];
  if (!keyCol) { showToast('Sin columna clave para eliminar', 'err'); return; }
  const keyVal = row[keyCol.name];
  if (!confirm('¿Eliminar fila donde ' + keyCol.name + ' = ' + keyVal + '?')) return;

  const cmd = 'BURN ' + currentTable + ' WHERE ' + keyCol.name + ' = ' + JSON.stringify(keyVal);
  try {
    const res = await fetch('/api/table/' + currentTable, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cmd }),
    });
    const data = await res.json();
    if (data.result?.startsWith('ERR')) {
      showToast(data.result, 'err');
    } else {
      showToast('Fila eliminada');
      await refreshTable();
    }
  } catch(e) {
    showToast('Error de red', 'err');
  }
}

async function runQuery() {
  const input = document.getElementById('qtInput');
  const result = document.getElementById('qtResult');
  const cmd = input.value.trim();
  if (!cmd) return;

  result.className = 'qt-result show';
  result.textContent = 'Ejecutando...';

  try {
    const res = await fetch('/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ cmd }),
    });
    const data = await res.json();
    const r = data.result || '';
    result.textContent = r;
    result.className = 'qt-result show ' + (r.startsWith('ERR') ? 'err' : 'ok');
    if (!r.startsWith('ERR')) await refreshTable();
  } catch(e) {
    result.textContent = 'Error de red';
    result.className = 'qt-result show err';
  }
}
</script>
</body>
</html>`
