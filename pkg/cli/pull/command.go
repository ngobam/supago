package pull

import (
	"fmt"

	"github.com/rosfandy/supago/internal/config"
	"github.com/rosfandy/supago/pkg/logger"
	"github.com/rosfandy/supago/pkg/supabase/drivers"
	"github.com/rosfandy/supago/pkg/supabase/query"
)

var PullLogger = logger.HcLog().Named("supago.pull")

func Run(name *string) (*query.TableSchemaResult, error) {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		PullLogger.Error("Failed to load config", "error", err)
		return nil, err
	}

	d := drivers.NewSupabase(cfg)
	q := query.NewTableSchemaQuery(d)

	result, err := q.GetTableSchema(name)
	if err != nil {
		PullLogger.Error("", "", err)
		return nil, err
	}

	if result == nil {
		PullLogger.Error("Result is nil")
		return nil, fmt.Errorf("result is nil")
	}

	PullLogger.Info("Table schema", "table", result.TableName, "columns", result.Columns)
	return result, nil
}
