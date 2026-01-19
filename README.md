# Supago

<div align="center">
    <img src="https://media.tenor.com/Uxz5-w-2uaIAAAAi/hmm-penguin.gif"/>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white" />
  <img src="https://img.shields.io/badge/Status-Under%20Construction-yellow" />
  <!-- <img src="https://img.shields.io/github/license/ngobam/supago" /> -->
  <!-- <img src="https://img.shields.io/github/actions/workflow/status/ngobam/supago/ci.yml?label=CI" /> -->
  <!-- <img src="https://img.shields.io/github/issues/ngobam/supago" /> -->
  <!-- <img src="https://img.shields.io/github/issues-pr/ngobam/supago" /> -->
  <img src="https://img.shields.io/badge/Changelog-Available-blue" />
  <img src="https://img.shields.io/github/last-commit/ngobam/supago" />
  <!-- <img src="https://img.shields.io/badge/Supabase-Ready-3ECF8E?logo=supabase&logoColor=white" /> -->
</p>



## Introduction
Supago is a CLI tool that provides a proxy layer between your applications and Supabase.

## Features
- Execute database queries using a **Database as Code** workflow
- Pull existing table schemas from Supabase for version control
- Push schema changes to Supabase programmatically
- REST API for automation and integration

## Project Structure
```md
supago/
│── api/
│   └── http/
│       ├── routes/
│       ├── handler/
│       └── presenter/
│
├── cmd/
│   └── main.go
│
├── internal/
│   ├── config/
│   ├── domain/
│   ├── usecase/
│   └── repository/
│
├── pkg/ 
│   ├──cli/
│   ├──logger/
│   ├──supabase/
│
├── .env
├── go.mod
└── README.md
```
| Directory  | Layer | Description |
|-----------|-------|-------------|
| `api/` | API / Transport Layer | Contains the transport layer of the application (REST and GraphQL). This layer is responsible only for handling requests and responses. |
| `cmd/` | Application Entry Point | Contains the application entry point (`main.go`). Responsible for bootstrapping the application. |
| `internal/` | Core / Business Layer | Core business logic of the application (domain, usecase, repository, configuration). Packages inside `internal` **cannot be imported by external projects**. |
| `pkg/` | Shared / Reusable Libraries | Reusable packages that can be imported by other projects or tools. |


## Run
### Available Command
```bash
Supago CLI

Usage:
  supago [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  pull        Pull table schema from supabase
  push        Push table schema to supabase
  server      Start Supago server

Flags:
  -h, --help   help for supago

Use "supago [command] --help" for more information about a command.
```

### Run Server
```bash
go run cmd/main.go server
```

### Pull Table
```bash
go run cmd/main.go pull -h                                                                          

Pull table schema from Supabase and display column information

Usage:
  supago pull <table_name> [flags]
  supago pull [command]

Examples:
supago pull blogs

Available Commands:
  check       Check database setup
  setup       Setup database functions

Flags:
  -h, --help   help for pull

```

```bash
go run cmd/main.go pull profiles                                                                    

Table: Blogs
Columns:
  • id                   bigint          NOT NULL   default: -
  • title                character varying NULL       default: -
  • description          text            NULL       default: -
  • content              text            NULL       default: -
  • tags                 character varying NULL       default: -
  • status               character varying NULL       default: -
  • category             character varying NULL       default: -
  • created_at           timestamp with time zone NOT NULL   default: now()
  • author_id            uuid            NULL       default: -
  • date                 date            NULL       default: -
  • type                 character varying NULL       default: -
  • thumbnail            character varying NULL       default: -

Generated model: internal/domain/blogs.go
```

### Push Table

```bash
go run cmd/main.go push -h

Push table schema to supabase

Usage:
  supago push <table_name> [flags]

Flags:
  -h, --help          help for push
      --path string   Directory for table schema (default "internal/domain")
```

```bash
go run cmd/main.go push examples 

Executing Query...
CREATE TABLE Examples (
  id BIGINT NOT NULL,
  name TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);
table 'examples' pushed successfully
```

