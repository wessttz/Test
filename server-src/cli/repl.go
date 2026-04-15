package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"evodb/client"
)

var commands = []string{
	"FORGE", "PUSH", "UPSERT", "PULL", "COUNT", "BURN",
	"REFORGE", "DROP", "TABLES", "SCHEMA", "INDEX",
	"WHERE", "SET", "KEY", "LIMIT", "ON", "AND", "ORDER", "BY", "ASC", "DESC",
}

var keywords = []string{
	"INT", "FLOAT", "STRING", "BOOL", "JSON", "NULL", "INDEX",
	"true", "false",
}

func complete(line string) []string {
	upper := strings.ToUpper(strings.TrimSpace(line))
	var matches []string
	all := append(commands, keywords...)
	for _, c := range all {
		if strings.HasPrefix(c, upper) {
			matches = append(matches, c)
		}
	}
	return matches
}

const banner = `
  ██╗     ██╗   ██╗██████╗ ██╗   ██╗███████╗
  ██║     ██║   ██║██╔══██╗██║   ██║██╔════╝
  ██║     ██║   ██║██████╔╝██║   ██║███████╗
  ██║     ██║   ██║██╔══██╗██║   ██║╚════██║
  ███████╗╚██████╔╝██║  ██║╚██████╔╝███████║
  ╚══════╝ ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚══════╝  v3.0

  Type a command or 'help' for reference. Ctrl+C to exit.
`

const helpText = `
Commands:
  FORGE <table> (col TYPE [INDEX], ...)            Crear tabla
  PUSH <table> (val, ...)                          Insertar fila
  UPSERT <table> KEY <col> (val, ...)              Insertar o reemplazar
  PULL <table> [WHERE conditions] [ORDER BY col [ASC|DESC]] [LIMIT n]
  COUNT <table> [WHERE col = val]                  Contar filas
  BURN <table> WHERE col = val                     Eliminar filas
  REFORGE <table> SET col=val WHERE col=val        Actualizar filas
  DROP <table>                                     Eliminar tabla
  TABLES                                           Listar tablas
  SCHEMA <table>                                   Ver esquema
  INDEX <table> ON <col>                           Agregar índice

Types: INT  FLOAT  STRING  BOOL  JSON  NULL
Operators: =  !=  <  <=  >  >=
Multiple conditions: AND

Examples:
  FORGE users (id STRING INDEX, coins INT INDEX, level INT, name STRING)
  UPSERT users KEY id ("u001", 100, 3, "Ana")
  PULL users WHERE coins > 500 ORDER BY coins DESC LIMIT 10
  PULL users WHERE level >= 5 AND coins > 1000
  COUNT users WHERE coins = 0
  REFORGE users SET coins = 200 WHERE id = "u001"
  BURN users WHERE id = "u001"
`

func Run(addr string) {
	fmt.Print(banner)
	fmt.Printf("  Connecting to %s...\n\n", addr)

	c := client.Connect(addr)
	defer c.Close()

	if _, err := c.Tables(); err != nil {
		fmt.Printf("  ERROR: Cannot connect to Lurus server at %s\n", addr)
		fmt.Printf("  Start it with: lurus serve <file.lurus>\n\n")
		os.Exit(1)
	}
	fmt.Printf("  Connected.\n\n")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("lurus> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch line {
		case "help", `\h`:
			fmt.Print(helpText)
			continue
		case "exit", "quit", `\q`:
			fmt.Println("Bye.")
			return
		}

		if strings.HasSuffix(line, "?") {
			word := strings.TrimSuffix(line, "?")
			matches := complete(word)
			if len(matches) > 0 {
				fmt.Printf("  Suggestions: %s\n", strings.Join(matches, "  "))
			} else {
				fmt.Println("  No suggestions.")
			}
			continue
		}

		resp, err := c.Send(line)
		if err != nil {
			fmt.Printf("  Connection error: %v\n", err)
			c = client.Connect(addr)
			continue
		}

		if strings.HasPrefix(resp, "ERR") {
			fmt.Printf("  \033[31m%s\033[0m\n\n", resp)
		} else {
			payload := strings.TrimPrefix(resp, "OK ")
			if strings.HasPrefix(payload, "[") || strings.HasPrefix(payload, "{") {
				fmt.Printf("  \033[1mOK\033[0m\n")
				prettyPrint(payload)
			} else {
				fmt.Printf("  \033[1m%s\033[0m\n\n", resp)
			}
		}
	}
}

func prettyPrint(s string) {
	var out strings.Builder
	indent := 0
	inStr := false
	for i, ch := range s {
		switch {
		case ch == '"' && (i == 0 || s[i-1] != '\\'):
			inStr = !inStr
			out.WriteRune(ch)
		case inStr:
			out.WriteRune(ch)
		case ch == '{' || ch == '[':
			out.WriteRune(ch)
			indent++
			out.WriteString("\n" + strings.Repeat("  ", indent))
		case ch == '}' || ch == ']':
			indent--
			out.WriteString("\n" + strings.Repeat("  ", indent))
			out.WriteRune(ch)
		case ch == ',':
			out.WriteRune(ch)
			out.WriteString("\n" + strings.Repeat("  ", indent))
		case ch == ':':
			out.WriteString(": ")
		default:
			out.WriteRune(ch)
		}
	}
	fmt.Printf("  %s\n\n", strings.ReplaceAll(out.String(), "\n", "\n  "))
}
