package main

import (
	"fmt"
	"os"

	"evodb/cli"
	"evodb/server"
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

const usage = bold + purple + `
  ███████╗██╗   ██╗ ██████╗ ██████╗ ██████╗ 
  ██╔════╝██║   ██║██╔═══██╗██╔══██╗██╔══██╗
  █████╗  ██║   ██║██║   ██║██║  ██║██████╔╝
  ██╔══╝  ╚██╗ ██╔╝██║   ██║██║  ██║██╔══██╗
  ███████╗ ╚████╔╝ ╚██████╔╝██████╔╝██████╔╝
  ╚══════╝  ╚═══╝   ╚═════╝ ╚═════╝ ╚═════╝ ` + reset + `

` + cyan + bold + `Uso:` + reset + `
  ` + yellow + `evodb serve` + reset + ` <database.evodb> [addr]   Inicia el servidor  ` + dim + `(default: :7777)` + reset + `
  ` + yellow + `evodb repl` + reset + `  [addr]                     Abre la CLI interactiva ` + dim + `(default: localhost:7777)` + reset + `

` + cyan + bold + `Ejemplos:` + reset + `
  ` + dim + `evodb serve mybot.evodb` + reset + `
  ` + dim + `evodb serve mybot.evodb :9000` + reset + `
  ` + dim + `evodb repl` + reset + `
  ` + dim + `evodb repl localhost:9000` + reset + `
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		if len(os.Args) < 3 {
			fmt.Print(usage)
			os.Exit(1)
		}
		dbFile := os.Args[2]
		addr := ":7777"
		if len(os.Args) >= 4 {
			addr = os.Args[3]
		}

		fmt.Printf(purple+bold+"  ╭───────────────────────────────────╮\n"+reset)
		fmt.Printf(purple+bold+"  │"+reset+cyan+bold+"   EVODB  —  Database Server    "+reset+purple+bold+"│\n"+reset)
		fmt.Printf(purple+bold+"  ╰───────────────────────────────────╯\n"+reset)
		fmt.Printf(green+"  ✦ DB     → "+reset+"%s\n", dbFile)
		fmt.Printf(green+"  ✦ Addr   → "+reset+"%s\n", addr)
		fmt.Printf(dim+"  ─────────────────────────────────────\n"+reset)

		srv := server.New(dbFile, addr)
		if err := srv.Start(); err != nil {
			fmt.Fprintf(os.Stderr, red+bold+"  ✗ Error: %v\n"+reset, err)
			os.Exit(1)
		}

	case "repl":
		addr := "localhost:7777"
		if len(os.Args) >= 3 {
			addr = os.Args[2]
		}
		fmt.Printf(purple+bold+"  ╭───────────────────────────────────╮\n"+reset)
		fmt.Printf(purple+bold+"  │"+reset+cyan+bold+"   EVODB  —  Interactive REPL    "+reset+purple+bold+"│\n"+reset)
		fmt.Printf(purple+bold+"  ╰───────────────────────────────────╯\n"+reset)
		fmt.Printf(green+"  ✦ Conectando → "+reset+"%s\n\n", addr)
		cli.Run(addr)

	default:
		fmt.Printf(red+bold+"  ✗ Comando desconocido: %s\n"+reset, os.Args[1])
		fmt.Print(usage)
		os.Exit(1)
	}
}
