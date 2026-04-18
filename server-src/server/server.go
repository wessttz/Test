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

// formatRows serializes rows as JSON for the wire protocol.
// All values are serialized as strings for backwards compatibility with clients.
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
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>EvoDB Dashboard</title>
<style>
:root{--bg:#f5f5f7;--surface:#fff;--surface2:#f0f0f3;--border:#e2e2e8;--border2:#d0d0d8;--accent:#d32f2f;--accent-b:#b71c1c;--accent-dim:rgba(211,47,47,.09);--text:#1a1a1e;--text2:#5a5a68;--muted:#9898a6;--green:#16a34a;--green-dim:rgba(22,163,74,.1);--mono:'IBM Plex Mono',monospace;--sans:'DM Sans',sans-serif;--radius:10px;--shadow:0 1px 3px rgba(0,0,0,.07),0 4px 16px rgba(0,0,0,.05)}
*,*::before,*::after{margin:0;padding:0;box-sizing:border-box}
html{height:100%}
body{min-height:100%;background:var(--bg);color:var(--text);font-family:var(--sans);display:flex;flex-direction:column;-webkit-font-smoothing:antialiased}
.layout{display:flex;height:100vh;overflow:hidden}
.sidebar{width:220px;min-width:220px;background:var(--surface);border-right:1px solid var(--border);display:flex;flex-direction:column;overflow:hidden}
.sidebar-brand{padding:18px 14px 14px;border-bottom:1px solid var(--border)}
.brand-name{font-family:var(--mono);font-size:.95rem;font-weight:700;color:var(--text)}
.brand-name span{color:var(--accent)}
.brand-db{margin-top:5px;font-family:var(--mono);font-size:.62rem;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sidebar-section{padding:12px 8px 8px;flex:1;overflow-y:auto}
.sidebar-label{font-size:.58rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:var(--mono);padding:0 8px;margin-bottom:5px;font-weight:600}
.tbl-item{display:flex;align-items:center;gap:7px;padding:7px 9px;border-radius:var(--radius);cursor:pointer;border:1.5px solid transparent;margin-bottom:2px;transition:background .12s}
.tbl-item:hover{background:var(--surface2)}
.tbl-item.active{background:var(--accent-dim);border-color:rgba(211,47,47,.3)}
.tbl-icon{font-size:.6rem;color:var(--muted)}
.tbl-name{flex:1;font-family:var(--mono);font-size:.78rem;color:var(--text);font-weight:500}
.tbl-rows{font-family:var(--mono);font-size:.6rem;background:var(--surface2);color:var(--text2);padding:2px 6px;border-radius:5px;border:1px solid var(--border)}
.tbl-item.active .tbl-rows{background:var(--accent-dim);color:var(--accent)}
.no-tables{padding:10px 9px;font-size:.75rem;color:var(--muted);font-family:var(--mono);font-style:italic}
.sidebar-footer{padding:10px 8px;border-top:1px solid var(--border)}
.logout-btn{display:flex;align-items:center;gap:7px;width:100%;padding:7px 9px;background:none;border:1.5px solid var(--border);border-radius:var(--radius);color:var(--text2);font-family:var(--sans);font-size:.78rem;font-weight:500;cursor:pointer;text-decoration:none;transition:all .15s}
.logout-btn:hover{border-color:var(--accent);color:var(--accent);background:var(--accent-dim)}
.main{flex:1;display:flex;flex-direction:column;overflow:hidden;min-width:0}
.topbar{padding:13px 18px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;background:var(--surface)}
.topbar-title{font-size:.92rem;font-weight:600;color:var(--text)}
.topbar-title span{color:var(--muted);font-weight:400;margin-left:4px}
.status-pill{display:inline-flex;align-items:center;gap:5px;background:var(--green-dim);color:var(--green);font-family:var(--mono);font-size:.65rem;font-weight:600;padding:3px 9px;border-radius:20px;border:1px solid rgba(22,163,74,.2)}
.status-pill::before{content:'';width:5px;height:5px;background:var(--green);border-radius:50%;animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.stats-row{display:grid;grid-template-columns:repeat(4,1fr);border-bottom:1px solid var(--border);background:var(--surface)}
.stat{padding:14px 18px;border-right:1px solid var(--border)}
.stat:last-child{border-right:none}
.stat-label{font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:var(--mono);font-weight:600}
.stat-val{font-family:var(--mono);font-size:1.3rem;font-weight:700;margin-top:2px;color:var(--text)}
.stat-val.accent{color:var(--accent)}
.content{flex:1;overflow:auto;padding:18px;background:var(--bg)}
.welcome{display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:280px;text-align:center;color:var(--muted);gap:10px}
.welcome h2{font-size:.88rem;font-weight:500;color:var(--muted)}
.table-view{display:none}
.tv-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:8px}
.tv-title{display:flex;align-items:center;gap:8px}
.tv-title h2{font-size:.98rem;font-weight:700;color:var(--text)}
.tv-count{font-family:var(--mono);font-size:.62rem;font-weight:600;background:var(--accent-dim);color:var(--accent);padding:2px 7px;border-radius:5px;border:1px solid rgba(211,47,47,.2)}
.tv-actions{display:flex;gap:6px}
.btn{display:inline-flex;align-items:center;gap:5px;padding:6px 12px;border-radius:8px;font-family:var(--sans);font-size:.76rem;font-weight:600;cursor:pointer;border:1.5px solid;transition:all .12s}
.btn-primary{background:var(--accent);border-color:var(--accent-b);color:#fff}
.btn-primary:hover{background:var(--accent-b)}
.btn-ghost{background:var(--surface);border-color:var(--border);color:var(--text2)}
.btn-ghost:hover{border-color:var(--border2);color:var(--text);background:var(--surface2)}
.data-table-wrap{border:1.5px solid var(--border);border-radius:var(--radius);overflow:hidden;background:var(--surface);box-shadow:var(--shadow);overflow-x:auto}
.data-table{width:100%;border-collapse:collapse;font-family:var(--mono);font-size:.76rem}
.data-table th{padding:9px 13px;text-align:left;font-size:.6rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted);background:var(--surface2);border-bottom:1.5px solid var(--border);white-space:nowrap;font-weight:600}
.data-table td{padding:8px 13px;border-bottom:1px solid var(--border);color:var(--text2);max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.data-table tr:last-child td{border-bottom:none}
.data-table tr:hover td{background:var(--surface2);color:var(--text)}
.td-null{opacity:.3;font-style:italic}
.td-bool-true{color:var(--green);font-weight:600}
.td-bool-false{color:var(--accent);font-weight:600}
.td-num{color:#6366f1}
.td-actions{display:flex;gap:4px}
.td-btn{padding:2px 7px;font-family:var(--sans);font-size:.65rem;font-weight:600;border-radius:5px;cursor:pointer;border:1.5px solid;transition:all .12s;background:transparent}
.td-btn-edit{border-color:var(--border);color:var(--text2)}
.td-btn-edit:hover{border-color:var(--border2);color:var(--text);background:var(--surface2)}
.td-btn-del{border-color:rgba(211,47,47,.2);color:var(--accent)}
.td-btn-del:hover{background:var(--accent-dim);border-color:rgba(211,47,47,.4)}
.empty-table{text-align:center;padding:40px;color:var(--muted);font-size:.8rem}
.query-terminal{margin-top:14px;border:1.5px solid var(--border);border-radius:var(--radius);overflow:hidden;background:var(--surface);box-shadow:var(--shadow)}
.qt-header{padding:8px 13px;background:var(--surface2);border-bottom:1px solid var(--border)}
.qt-label{font-family:var(--mono);font-size:.6rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-weight:600}
.qt-body{padding:9px;display:flex;gap:7px}
.qt-input{flex:1;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;padding:8px 11px;font-family:var(--mono);font-size:.78rem;color:var(--text);outline:none;transition:border-color .15s;min-width:0}
.qt-input:focus{border-color:var(--accent)}
.qt-run{padding:8px 13px;background:var(--accent);border:1.5px solid var(--accent-b);border-radius:8px;color:#fff;font-family:var(--sans);font-size:.76rem;font-weight:700;cursor:pointer;transition:background .12s;white-space:nowrap}
.qt-run:hover{background:var(--accent-b)}
.qt-result{margin:0 9px 9px;padding:9px 11px;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;font-family:var(--mono);font-size:.74rem;color:var(--text2);display:none;white-space:pre-wrap;word-break:break-all;max-height:140px;overflow:auto}
.qt-result.ok{color:var(--green);border-color:rgba(22,163,74,.3);background:var(--green-dim)}
.qt-result.err{color:var(--accent);border-color:rgba(211,47,47,.3);background:var(--accent-dim)}
.qt-result.show{display:block}
.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.3);backdrop-filter:blur(4px);z-index:100;align-items:center;justify-content:center;padding:16px}
.modal-bg.open{display:flex}
.modal{background:var(--surface);border:1.5px solid var(--border);border-radius:14px;padding:22px;width:100%;max-width:480px;box-shadow:0 20px 60px rgba(0,0,0,.15);animation:slideUp .2s ease}
@keyframes slideUp{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.modal-title{font-size:.88rem;font-weight:700;color:var(--text);margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid var(--border)}
.modal-field{margin-bottom:11px}
.modal-field label{display:block;font-size:.62rem;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);font-family:var(--mono);font-weight:600;margin-bottom:4px}
.modal-field input{width:100%;background:var(--bg);border:1.5px solid var(--border);border-radius:8px;padding:8px 11px;font-family:var(--mono);font-size:.82rem;color:var(--text);outline:none;transition:border-color .15s}
.modal-field input:focus{border-color:var(--accent)}
.modal-actions{display:flex;gap:7px;justify-content:flex-end;margin-top:16px;padding-top:12px;border-top:1px solid var(--border)}
.toast{position:fixed;bottom:18px;right:18px;background:var(--surface);border:1.5px solid var(--border);border-radius:10px;padding:10px 15px;font-family:var(--sans);font-size:.78rem;font-weight:500;color:var(--text);z-index:200;transform:translateY(60px);opacity:0;transition:all .25s;pointer-events:none;box-shadow:var(--shadow)}
.toast.show{transform:translateY(0);opacity:1}
.toast.ok{border-color:rgba(22,163,74,.4);color:var(--green)}
.toast.err{border-color:rgba(211,47,47,.4);color:var(--accent)}
</style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <div class="sidebar-brand">
      <div class="brand-name">EVO<span>DB</span></div>
      <div class="brand-db">{{DBPATH}}</div>
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
      <div class="topbar-title">Dashboard <span>/ EvoDB</span></div>
      <div class="status-pill">Online</div>
    </div>
    <div class="stats-row">
      <div class="stat"><div class="stat-label">Tablas</div><div class="stat-val accent">{{TABLECOUNT}}</div></div>
      <div class="stat"><div class="stat-label">Registros</div><div class="stat-val">{{ROWCOUNT}}</div></div>
      <div class="stat"><div class="stat-label">Uptime</div><div class="stat-val" style="font-size:1rem">{{UPTIME}}</div></div>
      <div class="stat"><div class="stat-label">Queries</div><div class="stat-val">{{QUERIES}}</div></div>
    </div>
    <div class="content">
      <div class="welcome" id="welcomeScreen">
        <div style="font-size:2rem">⬡</div>
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
            <input class="qt-input" id="qtInput" placeholder="PULL users LIMIT 10" onkeydown="if(event.key==='Enter')runQuery()">
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
    <div class="modal-title" id="modalTitle">Insertar fila</div>
    <div id="modalFields"></div>
    <div class="modal-actions">
      <button class="btn btn-ghost" onclick="closeModal()">Cancelar</button>
      <button class="btn btn-primary" onclick="submitModal()" id="modalSubmit">Insertar</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>

<script>
let currentTable = null, currentSchema = [], currentRows = [], editRowIndex = null;

function showToast(msg, type='ok') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = 'toast show ' + type;
  setTimeout(() => t.className = 'toast', 2500);
}

function formatCellValue(val, type) {
  if (val === null || val === undefined) return '<span class="td-null">null</span>';
  if (type === 'BOOL') {
    return val === true || val === 'true'
      ? '<span class="td-bool-true">true</span>'
      : '<span class="td-bool-false">false</span>';
  }
  if (type === 'INT' || type === 'FLOAT') {
    return '<span class="td-num">' + val + '</span>';
  }
  const str = String(val);
  const escaped = str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  return '<span title="' + escaped + '">' + (escaped.length > 40 ? escaped.slice(0,40)+'…' : escaped) + '</span>';
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
  } catch(e) { showToast('Error al cargar tabla', 'err'); }
}

function renderTable() {
  const head = document.getElementById('tvHead');
  const body = document.getElementById('tvBody');
  document.getElementById('tvCount').textContent = currentRows.length + ' rows';
  if (!currentSchema.length) { head.innerHTML = ''; body.innerHTML = '<tr><td colspan="99"><div class="empty-table">Sin esquema</div></td></tr>'; return; }
  head.innerHTML = '<tr>' + currentSchema.map(c =>
    '<th>' + c.name + ' <small style="opacity:.45">' + c.type + (c.indexed?' ⬡':'') + '</small></th>'
  ).join('') + '<th style="width:90px">Acciones</th></tr>';
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

async function refreshTable() { if (currentTable) await loadTable(currentTable); }

function openInsertModal() {
  editRowIndex = null;
  document.getElementById('modalTitle').textContent = 'Insertar — ' + currentTable;
  document.getElementById('modalSubmit').textContent = 'Insertar';
  document.getElementById('modalFields').innerHTML = currentSchema.map(c =>
    '<div class="modal-field"><label>' + c.name + ' <span style="color:var(--accent);margin-left:4px">' + c.type + '</span></label>' +
    '<input id="mf_' + c.name + '" placeholder="' + c.name + '"></div>'
  ).join('');
  document.getElementById('modal').classList.add('open');
  if (currentSchema[0]) document.getElementById('mf_' + currentSchema[0].name)?.focus();
}

function openEditModal(i) {
  editRowIndex = i;
  const row = currentRows[i];
  document.getElementById('modalTitle').textContent = 'Editar — ' + currentTable;
  document.getElementById('modalSubmit').textContent = 'Guardar';
  document.getElementById('modalFields').innerHTML = currentSchema.map(c =>
    '<div class="modal-field"><label>' + c.name + ' <span style="color:var(--accent);margin-left:4px">' + c.type + '</span></label>' +
    '<input id="mf_' + c.name + '" value="' + (row[c.name] !== null && row[c.name] !== undefined ? String(row[c.name]).replace(/"/g,'&quot;') : '') + '"></div>'
  ).join('');
  document.getElementById('modal').classList.add('open');
}

function closeModal() { document.getElementById('modal').classList.remove('open'); }

async function submitModal() {
  const vals = currentSchema.map(c => JSON.stringify(document.getElementById('mf_' + c.name)?.value || ''));
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
    else { showToast(editRowIndex !== null ? 'Fila actualizada' : 'Fila insertada'); closeModal(); await refreshTable(); }
  } catch(e) { showToast('Error de red', 'err'); }
}

async function deleteRow(i) {
  const row = currentRows[i];
  const keyCol = currentSchema.find(c => c.indexed) || currentSchema[0];
  if (!keyCol) { showToast('Sin columna clave', 'err'); return; }
  const keyVal = row[keyCol.name];
  if (!confirm('¿Eliminar fila donde ' + keyCol.name + ' = ' + keyVal + '?')) return;
  const cmd = 'BURN ' + currentTable + ' WHERE ' + keyCol.name + ' = ' + JSON.stringify(String(keyVal));
  try {
    const res = await fetch('/api/table/' + currentTable, {
      method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({cmd})
    });
    const data = await res.json();
    if (data.result?.startsWith('ERR')) { showToast(data.result, 'err'); }
    else { showToast('Fila eliminada'); await refreshTable(); }
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
