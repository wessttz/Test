package server

import (
	"bufio"
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
	purple = "\033[35m"
	cyan   = "\033[36m"
	green  = "\033[32m"
	yellow = "\033[33m"
	red    = "\033[31m"
	dim    = "\033[2m"
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

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for range ticker.C {
			if err := s.db.Checkpoint(); err != nil {
				fmt.Printf(yellow+"  [!] Checkpoint error: %v\n"+reset, err)
			}
		}
	}()

	go func() {
		ln, err := net.Listen("tcp", s.addr)
		if err != nil {
			fmt.Printf(red+bold+"  ✗ TCP error: %v\n"+reset, err)
			return
		}
		defer ln.Close()
		fmt.Printf(green+"  ✦ TCP     → "+reset+cyan+"%s"+reset+"\n", s.addr)
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
	fmt.Printf(green+"  ✦ HTTP    → "+reset+cyan+"%s"+reset+"\n", httpAddr)
	fmt.Printf(dim+"  ─────────────────────────────────────\n"+reset)
	fmt.Printf(purple+bold+"  ◈ EvoDB está listo para recibir consultas\n"+reset)
	fmt.Printf(dim+"  ─────────────────────────────────────\n"+reset)

	http.HandleFunc("/query", s.httpQuery)
	http.HandleFunc("/ping", s.statusPage)
	http.HandleFunc("/api/status", s.apiStatus)

	return http.ListenAndServe(httpAddr, nil)
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
			"name":    t,
			"rows":    count,
			"columns": colNames,
		})
	}
	uptime := time.Since(s.startTime)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "online",
		"uptime":  uptime.String(),
		"queries": s.queries.Load(),
		"tables":  tableStats,
		"db":      s.dbPath,
	})
}

func (s *Server) statusPage(w http.ResponseWriter, r *http.Request) {
	tables := s.db.ListTables()
	uptime := time.Since(s.startTime)
	hours := int(uptime.Hours())
	minutes := int(uptime.Minutes()) % 60

	tableCards := ""
	totalRows := 0
	for _, t := range tables {
		count, _ := s.db.CountRows(t, "", nil)
		cols, _ := s.db.GetSchema(t)
		totalRows += count
		colTags := ""
		for _, c := range cols {
			colTags += fmt.Sprintf("<span class=\"col-tag\"><i class=\"dot\"></i>%s <small>%s</small></span>", c.Name, c.Type)
		}
		tableCards += fmt.Sprintf(
			"<div class=\"table-card\">"+
				"<div class=\"table-header\">"+
				"<span class=\"table-icon\">&#128194;</span>"+
				"<span class=\"table-title\">%s</span>"+
				"<span class=\"row-count\">%d rows</span>"+
				"</div>"+
				"<div class=\"table-body\">%s</div>"+
				"</div>",
			t, count, colTags)
	}
	if tableCards == "" {
		tableCards = "<div class=\"empty-state\">No active tables found in core.</div>"
	}

	css := "<style>" +
		":root{--bg:#0d0d0f;--card-bg:#16161a;--accent:#ff4d4d;--accent-dim:rgba(255,77,77,0.15);--text-main:#e1e1e6;--text-dim:#88888c;--border:#2a2a30}" +
		"*{margin:0;padding:0;box-sizing:border-box}" +
		"body{background-color:var(--bg);color:var(--text-main);font-family:Inter,sans-serif;line-height:1.6;padding:40px 20px}" +
		".container{max-width:900px;margin:0 auto}" +
		".header{display:flex;align-items:center;justify-content:space-between;margin-bottom:40px;border-bottom:1px solid var(--border);padding-bottom:20px}" +
		".brand{display:flex;align-items:center;gap:15px}" +
		".logo-svg{width:45px;height:45px;fill:var(--accent);filter:drop-shadow(0 0 8px var(--accent-dim))}" +
		".brand-text{font-family:monospace;font-size:1.5rem;font-weight:700;letter-spacing:-1px}" +
		".brand-text span{color:var(--accent)}" +
		".stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin-bottom:40px}" +
		".stat-card{background:var(--card-bg);border:1px solid var(--border);padding:20px;border-radius:12px;position:relative;overflow:hidden}" +
		".stat-card::after{content:\"\";position:absolute;top:0;left:0;width:4px;height:100%;background:var(--accent)}" +
		".stat-label{font-size:0.7rem;text-transform:uppercase;color:var(--text-dim);letter-spacing:1px;font-weight:700}" +
		".stat-value{font-family:monospace;font-size:1.8rem;color:#fff;margin-top:5px}" +
		".section-title{font-size:0.9rem;text-transform:uppercase;color:var(--text-dim);margin-bottom:20px;display:flex;align-items:center;gap:10px}" +
		".section-title::after{content:\"\";flex:1;height:1px;background:var(--border)}" +
		".table-card{background:var(--card-bg);border:1px solid var(--border);border-radius:12px;margin-bottom:16px}" +
		".table-card:hover{border-color:var(--accent)}" +
		".table-header{padding:15px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:12px}" +
		".table-title{font-family:monospace;font-weight:700;flex:1;color:var(--accent)}" +
		".row-count{font-size:0.75rem;background:var(--accent-dim);color:var(--accent);padding:2px 8px;border-radius:4px;font-weight:700}" +
		".table-body{padding:15px 20px;display:flex;flex-wrap:wrap;gap:8px}" +
		".col-tag{background:#1c1c22;border:1px solid var(--border);padding:4px 10px;border-radius:6px;font-size:0.75rem;font-family:monospace;display:flex;align-items:center;gap:6px}" +
		".col-tag small{color:var(--text-dim)}" +
		".dot{width:6px;height:6px;background:var(--accent);border-radius:50%;display:inline-block}" +
		".footer{margin-top:60px;text-align:center;font-size:0.8rem;color:var(--text-dim)}" +
		".footer a{color:var(--accent);text-decoration:none;font-weight:600}" +
		".db-path{font-family:monospace;background:#000;padding:15px;border-radius:8px;border:1px dashed var(--border);font-size:0.85rem;color:var(--text-dim)}" +
		".db-path b{color:var(--text-main)}" +
		".empty-state{padding:30px;text-align:center;color:var(--text-dim);font-style:italic}" +
		"</style>"

	logoSVG := "<svg class=\"logo-svg\" viewBox=\"0 0 24 24\"><path d=\"M12,2C6.48,2,2,6.48,2,12s4.48,10,10,10,10-4.48,10-10S17.52,2,12,2ZM7,9.5c0-.83,.67-1.5,1.5-1.5s1.5,.67,1.5,1.5-.67,1.5-1.5,1.5-1.5-.67-1.5-1.5ZM12,18c-2.33,0-4.31-1.46-5.11-3.5h10.22c-.8,2.04-2.78,3.5-5.11,3.5ZM15.5,11c-.83,0-1.5-.67-1.5-1.5s.67-1.5,1.5-1.5,1.5,.67,1.5,1.5-.67,1.5-1.5,1.5Z\"/></svg>"

	html := "<!DOCTYPE html><html lang=\"es\"><head><meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1.0\"><title>EvoDB</title>" +
		"<link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap\" rel=\"stylesheet\">" +
		css + "</head><body><div class=\"container\">" +
		"<header class=\"header\"><div class=\"brand\">" + logoSVG +
		"<div class=\"brand-text\">LURUS<span>.DB</span></div></div>" +
		"<div class=\"row-count\" style=\"font-size:0.9rem;padding:5px 15px\">System Active</div></header>" +
		fmt.Sprintf(
			"<div class=\"stats-grid\">"+
				"<div class=\"stat-card\"><div class=\"stat-label\">Uptime</div><div class=\"stat-value\">%dh %dm</div></div>"+
				"<div class=\"stat-card\"><div class=\"stat-label\">Total Tables</div><div class=\"stat-value\">%d</div></div>"+
				"<div class=\"stat-card\"><div class=\"stat-label\">Total Records</div><div class=\"stat-value\">%d</div></div>"+
				"</div>",
			hours, minutes, len(tables), totalRows) +
		"<div class=\"section-title\">Schema Explorer</div>" +
		"<div class=\"tables-container\">" + tableCards + "</div>" +
		"<div class=\"section-title\" style=\"margin-top:40px\">Database Source</div>" +
		fmt.Sprintf("<div class=\"db-path\">root@lurus:~$ evodb --path <b>%s</b></div>", s.dbPath) +
		"<footer class=\"footer\">Lurus&bull; <a href=\"/api/status\">REST API</a></footer>" +
		"</div></body></html>"

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(html))
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
	w.Write(buildResponse(response))
}

func buildResponse(result string) []byte {
	b, _ := json.Marshal(result)
	return append(append([]byte(`{"result":`), b...), '}')
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	addr := conn.RemoteAddr().String()
	fmt.Printf(green+bold+"  [+] "+reset+cyan+"%s"+reset+" conectado\n", addr)
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
	fmt.Printf(yellow+"  [-] "+reset+dim+"%s"+reset+" desconectado\n", addr)
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