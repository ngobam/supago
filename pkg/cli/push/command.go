package push

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"

	"github.com/rosfandy/supago/internal/config"
	"github.com/rosfandy/supago/pkg/supabase/drivers"
	"github.com/rosfandy/supago/pkg/supabase/query"
)

func Run(tableName string, path string) error {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		return fmt.Errorf("load config failed: %w", err)
	}

	if path == "" {
		path = "internal/domain"
	}

	file := filepath.Join(path, tableName+".go")

	structName := toPascalCase(tableName)

	columns, err := parseStructFile(file, structName)
	if err != nil {
		return err
	}

	driver := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(driver)

	if err := q.InsertTableSchema(&tableName, columns); err != nil {
		return fmt.Errorf("%w", err)
	}

	fmt.Printf("table '%s' pushed successfully\n", tableName)
	return nil
}

func parseStructFile(filePath, structName string) ([]query.ColumnSchema, error) {
	fset := token.NewFileSet()

	node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", filePath, err)
	}

	for _, decl := range node.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}

		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != structName {
				continue
			}

			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}

			return buildColumnsFromStruct(st), nil
		}
	}

	return nil, fmt.Errorf("struct %s not found in %s", structName, filePath)
}

func buildColumnsFromStruct(st *ast.StructType) []query.ColumnSchema {
	var cols []query.ColumnSchema

	for _, field := range st.Fields.List {
		if field.Tag == nil {
			continue
		}

		tag := strings.Trim(field.Tag.Value, "`")
		dbTag := parseDBTag(tag)
		if dbTag == "" {
			continue
		}

		cols = append(cols, query.ColumnSchema{
			ColumnName: dbTag,
			DataType:   pgTypeFromExpr(field.Type),
			IsNullable: false,
		})
	}

	return cols
}

func parseDBTag(tag string) string {
	parts := strings.Split(tag, " ")
	for _, p := range parts {
		if strings.HasPrefix(p, `db:"`) {
			return strings.Trim(p[4:], `"`)
		}
	}
	return ""
}

func pgTypeFromExpr(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		switch t.Name {
		case "int", "int64":
			return "BIGINT"
		case "string":
			return "TEXT"
		case "bool":
			return "BOOLEAN"
		}
	case *ast.SelectorExpr:
		if t.Sel.Name == "Time" {
			return "TIMESTAMP"
		}
	}
	return "TEXT"
}

func toPascalCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
