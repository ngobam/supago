# Supago

<div align="center">
    <img src="https://media.tenor.com/Uxz5-w-2uaIAAAAi/hmm-penguin.gif"/>
</div>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white" />
  <img src="https://img.shields.io/badge/Supabase-Ready-3ECF8E?logo=supabase&logoColor=white" />
</p>


## Introduction
Supago is a CLI tool that provides a proxy layer between your applications and Supabase.

## Features
*_under construction_*

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
```bash
go run cmd/main.go server
```