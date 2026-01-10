# Supago

![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go&logoColor=white)
![Supabase](https://img.shields.io/badge/Supabase-Ready-3ECF8E?logo=supabase&logoColor=white)
![Gin](https://img.shields.io/badge/Gin-Framework-00ADD8?logo=gin&logoColor=white)


## Introduction
Supago provides proxy layer between your applications and Supabase.

## Features
- 🔒 **Secure API Proxy** - Hide Supabase credentials and control access
- 🏗️ **Clean Architecture** - Separation of concerns with clear boundaries
- ⚡ **High Performance** - Built on Gin framework for fast HTTP routing
- 🔧 **Easy Configuration** - Environment-based configuration management
- 📦 **Modular Design** - Easily extendable and maintainable codebase
- 🧪 **Testable** - Architecture designed for comprehensive unit testing

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
go run cmd/main.go
```