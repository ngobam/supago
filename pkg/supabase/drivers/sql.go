package drivers

import (
	"fmt"
)

func (s *Supabase) From(tableName string) *Supabase {
	// Reset URL to REST API base
	baseUrl := s.Config.SupabaseUrl()
	s.Url = fmt.Sprintf("%s/rest/v1/%s", baseUrl, tableName)
	return s
}

// Select adds select parameter to the URL
func (s *Supabase) Select(columns string) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%sselect=%s", s.Url, separator, columns)
	return s
}

// Eq adds equality filter
func (s *Supabase) Eq(column, value string) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%s%s=eq.%s", s.Url, separator, column, value)
	return s
}

// Neq adds not equal filter
func (s *Supabase) Neq(column, value string) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%s%s=neq.%s", s.Url, separator, column, value)
	return s
}

// Gt adds greater than filter
func (s *Supabase) Gt(column, value string) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%s%s=gt.%s", s.Url, separator, column, value)
	return s
}

// Lt adds less than filter
func (s *Supabase) Lt(column, value string) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%s%s=lt.%s", s.Url, separator, column, value)
	return s
}

// Order adds ordering
func (s *Supabase) Order(column string, ascending bool) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	direction := "desc"
	if ascending {
		direction = "asc"
	}

	s.Url = fmt.Sprintf("%s%sorder=%s.%s", s.Url, separator, column, direction)
	return s
}

// Limit adds limit
func (s *Supabase) Limit(limit int) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%slimit=%d", s.Url, separator, limit)
	return s
}

// Offset adds offset
func (s *Supabase) Offset(offset int) *Supabase {
	if s.Url == "" {
		return s
	}

	separator := "?"
	if contains(s.Url, "?") {
		separator = "&"
	}

	s.Url = fmt.Sprintf("%s%soffset=%d", s.Url, separator, offset)
	return s
}

// Insert inserts data and returns the query for execution
func (s *Supabase) Insert(data interface{}) *Supabase {
	s.Payload = data
	return s
}

// Upsert inserts or updates data
func (s *Supabase) Upsert(data interface{}) *Supabase {
	s.Payload = data
	s.AddHeader("Prefer", "resolution=merge-duplicates")
	return s
}

// Single ensures only one row is returned
func (s *Supabase) Single() *Supabase {
	s.AddHeader("Accept", "application/vnd.pgrst.object+json")
	return s
}

// RPC calls a Supabase RPC function via REST API
func (s *Supabase) RPC(functionName string, params interface{}) *Supabase {
	baseUrl := s.Config.SupabaseUrl()
	s.Url = fmt.Sprintf("%s/rest/v1/rpc/%s", baseUrl, functionName)
	s.Payload = params
	s.Headers["Authorization"] = fmt.Sprintf("Bearer %s", s.Config.SupabaseApiKey)
	return s
}

// Helper function to check if string contains substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(func() bool {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
			return false
		})())
}
