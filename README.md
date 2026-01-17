# Supago

<div align="center">
    <img src="https://media.tenor.com/Uxz5-w-2uaIAAAAi/hmm-penguin.gif"/>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white" />
  <img src="https://img.shields.io/badge/Supabase-Ready-3ECF8E?logo=supabase&logoColor=white" />
  <br/>
  <i>Under Construction</i>
</p>


## Introduction
Supago is a CLI tool that provides a proxy layer between your applications and Supabase.

## Features
- Execute Query (Database as Code)
- Pull Supabase Table Schema

## Project Structure
```md
supago/
│
├── api/
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
│   │   └── config.go
│   │
│   ├── domain/
│   │   └── user.go
│   │
│   ├── usecase/
│   │   └── user_usecase.go
│   │
│   ├── repository/
│   │   ├── user_repository.go
│   │   └── supabase_user_repo.go
│   │
│   └── infra/
│       └── supabase/
│           └── client.go
│
├── .env
├── go.mod
└── README.md

```

## Run
### Available Command
```bash
go run cmd/main.go help
```
```bash
Supago CLI

Usage:
  supago [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  help        Help about any command
  pull        Pull Supabase Model
  server      Start Supago server

Flags:
  -h, --help   help for supago

Use "supago [command] --help" for more information about a command.
```

### Run Server
```bash
go run cmd/main.go server
```

### Pull Model
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
