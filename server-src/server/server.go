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

const (
	reset  = "\033[0m"
	bold   = "\033[1m"
	dim    = "\033[2m"
	cyan   = "\033[36m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	gray   = "\033[38;5;240m"
	white  = "\033[97m"
	wine   = "\033[38;5;88m"
)

const (
	adminUser    = "admin"
	adminPass    = "admin"
	cookieName   = "evodb_session"
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

	// Checkpoint every 5 minutes
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			if err := s.db.Checkpoint(); err != nil {
				fmt.Printf(yellow+"  ⚠  checkpoint: %v\n"+reset, err)
			}
		}
	}()

	// TCP server
	go func() {
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			fmt.Printf(red+bold+"  ✗ TCP: %v\n"+reset, err)
			return
		}
		defer ln.Close()
		fmt.Printf(green+"  ✓ "+reset+white+bold+"TCP"+reset+gray+"  →  "+reset+cyan+"%s\n"+reset, s.addr)
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go s.handleConn(conn)
		}
	}()

	// HTTP server on port+1
	httpAddr := strings.Replace(s.addr, "7777", "7778", 1)
	if httpAddr == s.addr {
		httpAddr = ":7778"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/login", s.handleLogin)
	mux.HandleFunc("/logout", s.handleLogout)
	mux.HandleFunc("/api/status", s.requireAuth(s.apiStatus))
	mux.HandleFunc("/query", s.requireAuth(s.httpQuery))
	mux.HandleFunc("/api/table/", s.requireAuth(s.apiTableOps))
	mux.HandleFunc("/", s.requireAuth(s.statusPage))

	fmt.Printf(green+"  ✓ "+reset+white+bold+"HTTP"+reset+gray+" →  "+reset+cyan+"%s\n"+reset, httpAddr)
	fmt.Printf(dim+"  ─────────────────────────────────────\n"+reset)

	return http.ListenAndServe(httpAddr, mux)
}

func (s *Server) printBanner() {
	fmt.Print("\n")
	fmt.Printf(wine+bold+"  ╔══════════════════════════════════════╗\n"+reset)
	fmt.Printf(wine+bold+"  ║"+reset+white+bold+"     E V O D B  —  Server v4        "+wine+bold+"║\n"+reset)
	fmt.Printf(wine+bold+"  ╚══════════════════════════════════════╝\n"+reset)
	fmt.Printf(gray+"  db    "+reset+cyan+"%s\n"+reset, s.dbPath)
	fmt.Printf(gray+"  addr  "+reset+cyan+"%s\n"+reset, s.addr)
	fmt.Print("\n")
}

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
	w.Write([]byte(loginPageHTML(errMsg)))
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (s *Server) apiTableOps(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/api/table/")
	table := strings.SplitN(path, "/", 2)[0]
	if table == "" {
		http.Error(w, `{"error":"missing table"}`, 400)
		return
	}
	switch r.Method {
	case http.MethodGet:
		rows, cols, err := s.db.Query(table, nil, nil, 0)
		if err != nil {
			w.Write([]byte(`{"error":"` + err.Error() + `"}`))
			return
		}
		schema, _ := s.db.GetSchema(table)
		colDefs := []map[string]interface{}{}
		for _, c := range schema {
			colDefs = append(colDefs, map[string]interface{}{
				"name": c.Name, "type": c.Type.String(), "indexed": c.Indexed,
			})
		}
		// Return typed row data: strings as strings, ints as numbers, bools as bools
		rowData := []map[string]interface{}{}
		for _, row := range rows {
			m := map[string]interface{}{}
			for i, v := range row {
				if i < len(cols) {
					m[cols[i].Name] = typedValue(v)
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
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Cmd string `json:"cmd"`
		}
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
	b, _ := json.Marshal(response)
	w.Write(append(append([]byte(`{"result":`), b...), '}'))
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	addr := conn.RemoteAddr().String()
	fmt.Printf(green+"  + "+reset+gray+"%s"+reset+" conectado\n", addr)
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
		sendLine(s.execute(line))
	}
	fmt.Printf(yellow+"  - "+reset+gray+"%s"+reset+" desconectado\n", addr)
}

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
			b, _ := json.Marshal(v.String())
			sb.Write(b)
		}
		sb.WriteString("}")
	}
	sb.WriteString("]")
	return sb.String()
}

// typedValue returns a Go value with proper type for JSON encoding in the Web UI.
func typedValue(v engine.Value) interface{} {
	switch v.Type {
	case engine.TypeInt:
		return v.IntVal
	case engine.TypeFloat:
		return v.FltVal
	case engine.TypeBool:
		return v.BoolVal
	case engine.TypeNull:
		return nil
	default:
		return v.StrVal
	}
}

func (s *Server) statusPage(w http.ResponseWriter, r *http.Request) {
	tables := s.db.ListTables()
	uptime := time.Since(s.startTime)
	h := int(uptime.Hours())
	m := int(uptime.Minutes()) % 60
	sec := int(uptime.Seconds()) % 60

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

	page := dashboardHTML
	page = strings.ReplaceAll(page, "{{DBPATH}}", s.dbPath)
	page = strings.ReplaceAll(page, "{{TABLELIST}}", tableListItems)
	page = strings.ReplaceAll(page, "{{TABLECOUNT}}", fmt.Sprintf("%d", len(tables)))
	page = strings.ReplaceAll(page, "{{ROWCOUNT}}", fmt.Sprintf("%d", totalRows))
	page = strings.ReplaceAll(page, "{{UPTIME}}", fmt.Sprintf("%02dh %02dm %02ds", h, m, sec))
	page = strings.ReplaceAll(page, "{{QUERIES}}", fmt.Sprintf("%d", s.queries.Load()))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(page))
}

func loginPageHTML(errMsg string) string {
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
<style>
:root{--bg:#f5f5f7;--surface:#fff;--border:#e2e2e8;--accent:#d32f2f;--accent-b:#b71c1c;--accent-dim:rgba(211,47,47,0.08);--text:#1a1a1e;--muted:#9898a6;--mono:'IBM Plex Mono',monospace;--sans:'DM Sans',sans-serif}
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--sans);display:flex;align-items:center;justify-content:center}
.wrap{width:100%;max-width:380px;padding:20px}
.brand{text-align:center;margin-bottom:28px}
.brand h1{font-family:var(--mono);font-size:1.6rem;font-weight:700;letter-spacing:-1px}
.brand h1 span{color:var(--accent)}
.brand p{margin-top:4px;font-size:.72rem;color:var(--muted);font-family:var(--mono);letter-spacing:2px;text-transform:uppercase}
.card{background:var(--surface);border:1.5px solid var(--border);border-radius:16px;padding:28px;box-shadow:0 4px 24px rgba(0,0,0,.06)}
.field{margin-bottom:16px}
label{display:block;font-size:.65rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:var(--mono);font-weight:600;margin-bottom:6px}
input{width:100%;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;padding:10px 13px;font-family:var(--mono);font-size:.9rem;color:var(--text);outline:none;transition:border-color .15s}
input:focus{border-color:var(--accent)}
.error-msg{background:rgba(211,47,47,.07);border:1.5px solid rgba(211,47,47,.2);color:var(--accent);border-radius:8px;padding:8px 12px;font-size:.8rem;margin-bottom:16px;text-align:center}
button{width:100%;background:var(--accent);border:1.5px solid var(--accent-b);border-radius:8px;padding:12px;font-family:var(--sans);font-size:.88rem;font-weight:700;color:#fff;cursor:pointer;margin-top:6px;transition:background .15s}
button:hover{background:var(--accent-b)}
</style>
</head>
<body>
<div class="wrap">
  <div class="brand"><h1>EVO<span>DB</span></h1><p>Database Server</p></div>
  <div class="card">
    ` + errHTML + `
    <form method="POST" action="/login">
      <div class="field"><label>Usuario</label><input type="text" name="username" autofocus></div>
      <div class="field"><label>Contraseña</label><input type="password" name="password"></div>
      <button type="submit">Acceder</button>
    </form>
  </div>
</div>
</body>
</html>`
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0">
<title>EvoDB Dashboard</title>
<style>
:root{
  --bg:#0e0e11;--surface:#17171c;--surface2:#1f1f26;--surface3:#26262f;
  --border:#2a2a35;--border2:#35353f;
  --accent:#e53935;--accent-b:#c62828;--accent-dim:rgba(229,57,53,.1);
  --text:#f0f0f5;--text2:#9898aa;--muted:#55556a;
  --green:#22c55e;--green-dim:rgba(34,197,94,.1);
  --indigo:#818cf8;
  --mono:'JetBrains Mono',monospace;--sans:'DM Sans',sans-serif;
  --radius:10px;--radius-lg:14px;
  --shadow:0 2px 8px rgba(0,0,0,.4);
}
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap');
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--sans);-webkit-font-smoothing:antialiased}

.layout{display:flex;height:100dvh;overflow:hidden}

.sidebar{
  width:240px;min-width:240px;background:var(--surface);
  border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden;
  transition:transform .25s ease;
}
.sidebar-brand{padding:18px 16px 14px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.brand-name{font-family:var(--mono);font-size:1rem;font-weight:700;letter-spacing:-0.5px}
.brand-name span{color:var(--accent)}
.brand-db{margin-top:4px;font-family:var(--mono);font-size:.6rem;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:180px}
.close-sidebar{display:none;background:none;border:none;color:var(--text2);font-size:1.1rem;cursor:pointer;padding:4px}
.sidebar-section{padding:12px 10px 8px;flex:1;overflow-y:auto}
.sidebar-label{font-size:.55rem;text-transform:uppercase;letter-spacing:2px;color:var(--muted);font-family:var(--mono);padding:0 6px;margin-bottom:6px;font-weight:600}
.tbl-item{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:var(--radius);cursor:pointer;border:1.5px solid transparent;margin-bottom:2px;transition:all .12s}
.tbl-item:hover{background:var(--surface2)}
.tbl-item.active{background:var(--accent-dim);border-color:rgba(229,57,53,.25)}
.tbl-icon{font-size:.55rem;color:var(--muted)}
.tbl-name{flex:1;font-family:var(--mono);font-size:.78rem;color:var(--text);font-weight:500;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tbl-rows{font-family:var(--mono);font-size:.58rem;background:var(--surface2);color:var(--text2);padding:2px 7px;border-radius:5px;border:1px solid var(--border);white-space:nowrap}
.tbl-item.active .tbl-rows{background:var(--accent-dim);color:var(--accent);border-color:rgba(229,57,53,.2)}
.no-tables{padding:10px 6px;font-size:.75rem;color:var(--muted);font-family:var(--mono);font-style:italic}
.sidebar-footer{padding:10px;border-top:1px solid var(--border)}
.logout-btn{display:flex;align-items:center;justify-content:center;gap:6px;width:100%;padding:9px;background:none;border:1.5px solid var(--border);border-radius:var(--radius);color:var(--text2);font-family:var(--sans);font-size:.78rem;font-weight:600;cursor:pointer;text-decoration:none;transition:all .15s}
.logout-btn:hover{border-color:var(--accent);color:var(--accent);background:var(--accent-dim)}

.main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}

.topbar{
  padding:12px 16px;border-bottom:1px solid var(--border);
  display:flex;align-items:center;justify-content:space-between;
  background:var(--surface);flex-shrink:0;
}
.topbar-left{display:flex;align-items:center;gap:10px}
.hamburger{display:none;background:none;border:1.5px solid var(--border);border-radius:7px;color:var(--text2);padding:6px 8px;cursor:pointer;font-size:.85rem;line-height:1}
.hamburger:hover{border-color:var(--border2);color:var(--text)}
.topbar-title{font-size:.9rem;font-weight:600;color:var(--text)}
.topbar-title span{color:var(--muted);font-weight:400;margin-left:4px;font-size:.82rem}
.status-pill{display:inline-flex;align-items:center;gap:5px;background:var(--green-dim);color:var(--green);font-family:var(--mono);font-size:.62rem;font-weight:600;padding:3px 10px;border-radius:20px;border:1px solid rgba(34,197,94,.2)}
.status-pill::before{content:'';width:5px;height:5px;background:var(--green);border-radius:50%;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}

.stats-row{display:grid;grid-template-columns:repeat(4,1fr);border-bottom:1px solid var(--border);background:var(--surface);flex-shrink:0}
.stat{padding:11px 16px;border-right:1px solid var(--border)}
.stat:last-child{border-right:none}
.stat-label{font-size:.55rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:var(--mono);font-weight:600}
.stat-val{font-family:var(--mono);font-size:1.2rem;font-weight:700;margin-top:2px;color:var(--text)}
.stat-val.accent{color:var(--accent)}

.content{flex:1;overflow:auto;padding:16px;background:var(--bg)}
.welcome{display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:260px;text-align:center;color:var(--muted);gap:10px}
.welcome-icon{font-size:2.5rem;opacity:.4}
.welcome h2{font-size:.85rem;font-weight:500;color:var(--muted)}

.table-view{display:none}
.tv-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;gap:8px;flex-wrap:wrap}
.tv-title{display:flex;align-items:center;gap:8px;min-width:0}
.tv-title h2{font-size:.95rem;font-weight:700;color:var(--text);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tv-count{font-family:var(--mono);font-size:.6rem;font-weight:600;background:var(--accent-dim);color:var(--accent);padding:2px 8px;border-radius:5px;border:1px solid rgba(229,57,53,.2);white-space:nowrap}
.tv-actions{display:flex;gap:6px;flex-shrink:0}
.btn{display:inline-flex;align-items:center;gap:5px;padding:7px 13px;border-radius:8px;font-family:var(--sans);font-size:.76rem;font-weight:600;cursor:pointer;border:1.5px solid;transition:all .12s;white-space:nowrap}
.btn-primary{background:var(--accent);border-color:var(--accent-b);color:#fff}
.btn-primary:hover{background:var(--accent-b)}
.btn-ghost{background:var(--surface);border-color:var(--border);color:var(--text2)}
.btn-ghost:hover{border-color:var(--border2);color:var(--text);background:var(--surface2)}

.data-table-wrap{border:1.5px solid var(--border);border-radius:var(--radius);overflow:hidden;background:var(--surface);box-shadow:var(--shadow);overflow-x:auto;-webkit-overflow-scrolling:touch}
.data-table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:.74rem}
.data-table th{padding:9px 14px;text-align:left;font-size:.58rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);background:var(--surface2);border-bottom:1.5px solid var(--border);white-space:nowrap;font-weight:600}
.data-table td{padding:9px 14px;border-bottom:1px solid var(--border);color:var(--text2);max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;vertical-align:middle}
.data-table tr:last-child td{border-bottom:none}
.data-table tr:hover td{background:var(--surface2);color:var(--text)}
.td-null{opacity:.3;font-style:italic}
.td-bool-true{color:var(--green);font-weight:600}
.td-bool-false{color:var(--accent);font-weight:600}
.td-num{color:var(--indigo)}
.td-actions{display:flex;gap:4px}
.td-btn{padding:3px 8px;font-family:var(--sans);font-size:.63rem;font-weight:600;border-radius:5px;cursor:pointer;border:1.5px solid;transition:all .12s;background:transparent;white-space:nowrap}
.td-btn-edit{border-color:var(--border);color:var(--text2)}
.td-btn-edit:hover{border-color:var(--border2);color:var(--text);background:var(--surface2)}
.td-btn-del{border-color:rgba(229,57,53,.2);color:var(--accent)}
.td-btn-del:hover{background:var(--accent-dim);border-color:rgba(229,57,53,.4)}
.empty-table{text-align:center;padding:40px;color:var(--muted);font-size:.8rem}

.query-terminal{margin-top:12px;border:1.5px solid var(--border);border-radius:var(--radius);overflow:hidden;background:var(--surface);box-shadow:var(--shadow)}
.qt-header{padding:8px 13px;background:var(--surface2);border-bottom:1px solid var(--border)}
.qt-label{font-family:var(--mono);font-size:.58rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-weight:600}
.qt-body{padding:10px;display:flex;gap:7px}
.qt-input{flex:1;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;padding:9px 12px;font-family:var(--mono);font-size:.78rem;color:var(--text);outline:none;transition:border-color .15s;min-width:0}
.qt-input:focus{border-color:var(--accent)}
.qt-run{padding:9px 14px;background:var(--accent);border:1.5px solid var(--accent-b);border-radius:8px;color:#fff;font-family:var(--sans);font-size:.76rem;font-weight:700;cursor:pointer;transition:background .12s;white-space:nowrap}
.qt-run:hover{background:var(--accent-b)}
.qt-result{margin:0 10px 10px;padding:10px 12px;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;font-family:var(--mono);font-size:.72rem;color:var(--text2);display:none;white-space:pre-wrap;word-break:break-all;max-height:160px;overflow:auto}
.qt-result.ok{color:var(--green);border-color:rgba(34,197,94,.3);background:var(--green-dim)}
.qt-result.err{color:var(--accent);border-color:rgba(229,57,53,.3);background:var(--accent-dim)}
.qt-result.show{display:block}

.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(6px);z-index:100;align-items:flex-end;justify-content:center;padding:0}
.modal-bg.open{display:flex}
.modal{
  background:var(--surface);border:1.5px solid var(--border);
  border-radius:var(--radius-lg) var(--radius-lg) 0 0;
  padding:20px 20px 32px;width:100%;max-width:560px;
  box-shadow:0 -10px 40px rgba(0,0,0,.4);
  animation:slideUp .22s ease;
  max-height:90dvh;display:flex;flex-direction:column;
}
@keyframes slideUp{from{opacity:0;transform:translateY(20px)}to{opacity:1;transform:translateY(0)}}
.modal-handle{width:36px;height:4px;background:var(--border2);border-radius:2px;margin:0 auto 16px;flex-shrink:0}
.modal-title{font-size:.9rem;font-weight:700;color:var(--text);margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border);flex-shrink:0}
.modal-scroll{flex:1;overflow-y:auto;padding-right:2px}
.modal-field{margin-bottom:12px}
.modal-field label{display:flex;align-items:center;gap:6px;font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:var(--mono);font-weight:600;margin-bottom:5px}
.modal-field label .ftype{color:var(--accent);font-size:.58rem;background:var(--accent-dim);padding:1px 5px;border-radius:4px;border:1px solid rgba(229,57,53,.2)}
.modal-field input,.modal-field textarea{
  width:100%;background:var(--bg);border:1.5px solid var(--border);
  border-radius:8px;padding:9px 12px;font-family:var(--mono);font-size:.82rem;
  color:var(--text);outline:none;transition:border-color .15s;
  -webkit-appearance:none;
}
.modal-field textarea{resize:vertical;min-height:80px;line-height:1.5}
.modal-field input:focus,.modal-field textarea:focus{border-color:var(--accent)}
.modal-actions{display:flex;gap:8px;margin-top:16px;padding-top:12px;border-top:1px solid var(--border);flex-shrink:0}
.modal-actions .btn{flex:1;justify-content:center;padding:11px}

.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%) translateY(80px);background:var(--surface2);border:1.5px solid var(--border);border-radius:10px;padding:10px 18px;font-family:var(--sans);font-size:.78rem;font-weight:500;color:var(--text);z-index:200;opacity:0;transition:all .25s;pointer-events:none;box-shadow:var(--shadow);white-space:nowrap}
.toast.show{transform:translateX(-50%) translateY(0);opacity:1}
.toast.ok{border-color:rgba(34,197,94,.4);color:var(--green)}
.toast.err{border-color:rgba(229,57,53,.4);color:var(--accent)}

.overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:50}
.overlay.open{display:block}

@media(max-width:680px){
  .sidebar{
    position:fixed;left:0;top:0;bottom:0;z-index:60;
    transform:translateX(-100%);width:260px;
  }
  .sidebar.open{transform:translateX(0)}
  .close-sidebar{display:block}
  .hamburger{display:flex;align-items:center}
  .stats-row{grid-template-columns:repeat(2,1fr)}
  .stat{padding:9px 12px}
  .stat-val{font-size:1rem}
  .content{padding:12px}
  .tv-header{flex-direction:column;align-items:flex-start}
  .tv-actions{width:100%;justify-content:flex-end}
  .modal-bg{align-items:flex-end}
  .modal{border-radius:var(--radius-lg) var(--radius-lg) 0 0;max-height:92dvh}
  .topbar{padding:10px 12px}
}
</style>
</head>
<body>
<div class="overlay" id="overlay" onclick="closeSidebar()"></div>
<div class="layout">
  <aside class="sidebar" id="sidebar">
    <div class="sidebar-brand">
      <div>
        <div class="brand-name">EVO<span>DB</span></div>
        <div class="brand-db">{{DBPATH}}</div>
      </div>
      <button class="close-sidebar" onclick="closeSidebar()">✕</button>
    </div>
    <div class="sidebar-section">
      <div class="sidebar-label">Tablas</div>
      <div id="tableList">{{TABLELIST}}</div>
    </div>
    <div class="sidebar-footer">
      <a href="/logout" class="logout-btn">✕ Cerrar sesión</a>
    </div>
  </aside>

  <main class="main">
    <div class="topbar">
      <div class="topbar-left">
        <button class="hamburger" onclick="openSidebar()">☰</button>
        <div class="topbar-title">Dashboard <span>/ EvoDB</span></div>
      </div>
      <div class="status-pill">Online</div>
    </div>
    <div class="stats-row">
      <div class="stat"><div class="stat-label">Tablas</div><div class="stat-val accent">{{TABLECOUNT}}</div></div>
      <div class="stat"><div class="stat-label">Registros</div><div class="stat-val">{{ROWCOUNT}}</div></div>
      <div class="stat"><div class="stat-label">Uptime</div><div class="stat-val" style="font-size:.85rem;margin-top:4px">{{UPTIME}}</div></div>
      <div class="stat"><div class="stat-label">Queries</div><div class="stat-val">{{QUERIES}}</div></div>
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
          <div class="qt-header"><span class="qt-label">◈ Terminal</span></div>
          <div class="qt-body">
            <input class="qt-input" id="qtInput" placeholder="PULL tabla LIMIT 10" onkeydown="if(event.key==='Enter')runQuery()">
            <button class="qt-run" onclick="runQuery()">Run</button>
          </div>
          <div class="qt-result" id="qtResult"></div>
        </div>
      </div>
    </div>
  </main>
</div>

<div class="modal-bg" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal">
    <div class="modal-handle"></div>
    <div class="modal-title" id="modalTitle">Insertar fila</div>
    <div class="modal-scroll" id="modalFields"></div>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeModal()">Cancelar</button>
      <button class="btn btn-primary" onclick="submitModal()" id="modalSubmit">Insertar</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
let currentTable = null, currentSchema = [], currentRows = [], editRowIndex = null;

function openSidebar(){
  document.getElementById('sidebar').classList.add('open');
  document.getElementById('overlay').classList.add('open');
}
function closeSidebar(){
  document.getElementById('sidebar').classList.remove('open');
  document.getElementById('overlay').classList.remove('open');
}

function showToast(msg, type='ok') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 2800);
}

function formatCellValue(val, type) {
  if (val === null || val === undefined) return '<span class="td-null">null</span>';
  if (type === 'BOOL') {
    return (val === true || val === 'true')
      ? '<span class="td-bool-true">true</span>'
      : '<span class="td-bool-false">false</span>';
  }
  if (type === 'INT' || type === 'FLOAT') return '<span class="td-num">' + val + '</span>';
  const str = String(val);
  const escaped = str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return '<span title="' + escaped + '">' + (escaped.length > 36 ? escaped.slice(0,36)+'…' : escaped) + '</span>';
}

async function loadTable(name) {
  document.querySelectorAll('.tbl-item').forEach(el => el.classList.remove('active'));
  const item = document.getElementById('titem-' + name);
  if (item) item.classList.add('active');
  currentTable = name;
  closeSidebar();
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
  } catch(e) { showToast('Error al cargar tabla', 'err'); }
}

function renderTable() {
  const head = document.getElementById('tvHead');
  const body = document.getElementById('tvBody');
  document.getElementById('tvCount').textContent = currentRows.length + ' rows';
  if (!currentSchema.length) {
    head.innerHTML = ''; 
    body.innerHTML = '<tr><td colspan="99"><div class="empty-table">Sin esquema</div></td></tr>';
    return;
  }
  head.innerHTML = '<tr>' + currentSchema.map(c =>
    '<th>' + c.name + ' <small style="opacity:.4">' + c.type + (c.indexed?' ⬡':'') + '</small></th>'
  ).join('') + '<th style="width:100px">Acciones</th></tr>';
  if (!currentRows.length) {
    body.innerHTML = '<tr><td colspan="' + (currentSchema.length+1) + '"><div class="empty-table">Sin registros</div></td></tr>';
    return;
  }
  body.innerHTML = currentRows.map((row, i) =>
    '<tr>' + currentSchema.map(c =>
      '<td>' + formatCellValue(row[c.name], c.type) + '</td>'
    ).join('') +
    '<td><div class="td-actions">' +
    '<button class="td-btn td-btn-edit" onclick="openEditModal(' + i + ')">Edit</button>' +
    '<button class="td-btn td-btn-del" onclick="deleteRow(' + i + ')">Del</button>' +
    '</div></td></tr>'
  ).join('');
}

async function refreshTable() {
  if (!currentTable) return;
  try {
    const res = await fetch('/api/table/' + currentTable);
    const data = await res.json();
    currentSchema = data.schema || [];
    currentRows = data.rows || [];
    renderTable();
    showToast('↺ Tabla actualizada');
  } catch(e) { showToast('Error al refrescar', 'err'); }
}

function isMultiline(col) {
  return col.type === 'STRING' || col.type === 'JSON';
}

function buildField(c, val) {
  const label = '<label>' + c.name + ' <span class=\"ftype\">' + c.type + (c.indexed?' ⯁':'') + '</span></label>';
  let display = '';
  if (val !== null && val !== undefined) {
    if (typeof val === 'object') {
      display = JSON.stringify(val, null, 2);
    } else {
      const s = String(val);
      const unescaped = s.replace(/\\"/g, '"');
      if ((unescaped.startsWith('{') || unescaped.startsWith('[')) && (unescaped.endsWith('}') || unescaped.endsWith(']'))) {
        try { display = JSON.stringify(JSON.parse(unescaped), null, 2); } catch(e) { display = unescaped; }
      } else {
        display = unescaped;
      }
    }
  }
  if (isMultiline(c)) {
    const tmp = document.createElement('div');
    tmp.textContent = display;
    const safe = tmp.innerHTML;
    return '<div class=\"modal-field\">' + label +
      '<textarea id=\"mf_' + c.name + '\" placeholder=\"' + c.name + '\" rows=\"6\" style=\"font-size:.72rem;line-height:1.6\">' + safe + '</textarea></div>';
  }
  const escaped = display.replace(/\"/g,'&quot;');
  return '<div class=\"modal-field\">' + label +
    '<input id=\"mf_' + c.name + '\" value=\"' + escaped + '\" placeholder=\"' + c.name + '\"></div>';
}

function getFieldValue(c) {
  const el = document.getElementById('mf_' + c.name);
  return el ? el.value : '';
}

function serializeVal(c, rawVal) {
  const v = rawVal.trim();
  if (v === '' || v.toLowerCase() === 'null') return 'NULL';
  if (c.type === 'INT') return isNaN(Number(v)) ? JSON.stringify(v) : v;
  if (c.type === 'FLOAT') return isNaN(parseFloat(v)) ? JSON.stringify(v) : v;
  if (c.type === 'BOOL') {
    const lower = v.toLowerCase();
    if (lower === 'true' || lower === 'false') return lower;
    return JSON.stringify(v);
  }
  return '"' + v.replace(/\\/g,'\\\\').replace(/"/g,'\\"') + '"';
}

function openInsertModal() {
  editRowIndex = null;
  document.getElementById('modalTitle').textContent = 'Insertar — ' + currentTable;
  document.getElementById('modalSubmit').textContent = 'Insertar';
  document.getElementById('modalFields').innerHTML = currentSchema.map(c => buildField(c, '')).join('');
  document.getElementById('modal').classList.add('open');
  const first = currentSchema[0];
  if (first) { const el = document.getElementById('mf_' + first.name); if(el) el.focus(); }
}

function openEditModal(i) {
  editRowIndex = i;
  const row = currentRows[i];
  document.getElementById('modalTitle').textContent = 'Editar — ' + currentTable;
  document.getElementById('modalSubmit').textContent = 'Guardar';
  document.getElementById('modalFields').innerHTML = currentSchema.map(c => buildField(c, row[c.name])).join('');
  document.getElementById('modal').classList.add('open');
}

function closeModal() { document.getElementById('modal').classList.remove('open'); }

async function submitModal() {
  const vals = currentSchema.map(c => serializeVal(c, getFieldValue(c)));
  let cmd;
  if (editRowIndex !== null) {
    const keyCol = currentSchema.find(c => c.indexed) || currentSchema[0];
    if (!keyCol) { showToast('Sin columna clave', 'err'); return; }
    cmd = 'UPSERT ' + currentTable + ' KEY ' + keyCol.name + ' (' + vals.join(', ') + ')';
  } else {
    cmd = 'PUSH ' + currentTable + ' (' + vals.join(', ') + ')';
  }
  try {
    const res = await fetch('/api/table/' + currentTable, {
      method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({cmd})
    });
    const data = await res.json();
    if (data.result?.startsWith('ERR')) { showToast(data.result, 'err'); }
    else { showToast(editRowIndex !== null ? '✓ Fila actualizada' : '✓ Fila insertada'); closeModal(); await refreshTable(); }
  } catch(e) { showToast('Error de red', 'err'); }
}

async function deleteRow(i) {
  const row = currentRows[i];
  const keyCol = currentSchema.find(c => c.indexed) || currentSchema[0];
  if (!keyCol) { showToast('Sin columna clave', 'err'); return; }
  const keyVal = row[keyCol.name];
  if (!confirm('¿Eliminar fila donde ' + keyCol.name + ' = ' + keyVal + '?')) return;
  const keyValSerialized = serializeVal(keyCol, String(keyVal));
  const cmd = 'BURN ' + currentTable + ' WHERE ' + keyCol.name + ' = ' + keyValSerialized;
  try {
    const res = await fetch('/api/table/' + currentTable, {
      method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({cmd})
    });
    const data = await res.json();
    if (data.result?.startsWith('ERR')) { showToast(data.result, 'err'); }
    else { showToast('✓ Fila eliminada'); await refreshTable(); }
  } catch(e) { showToast('Error de red', 'err'); }
}

async function runQuery() {
  const input = document.getElementById('qtInput');
  const result = document.getElementById('qtResult');
  const cmd = input.value.trim();
  if (!cmd) return;
  result.className = 'qt-result show'; result.textContent = 'Ejecutando...';
  try {
    const res = await fetch('/query', {
      method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({cmd})
    });
    const data = await res.json();
    const r = data.result || '';
    result.textContent = r;
    result.className = 'qt-result show ' + (r.startsWith('ERR') ? 'err' : 'ok');
    if (!r.startsWith('ERR')) await refreshTable();
  } catch(e) { result.textContent = 'Error de red'; result.className = 'qt-result show err'; }
}
</script>
</body>
</html>`
