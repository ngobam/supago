package drivers

import (
	"fmt"
)

func (s *Supabase) From(tableName string) *Supabase {
	baseUrl := s.Config.SupabaseUrl()
	s.Url = fmt.Sprintf("%s/rest/v1/%s", baseUrl, tableName)
	return s
}

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

func (s *Supabase) Insert(data interface{}) *Supabase {
	s.Payload = data
	return s
}

func (s *Supabase) Upsert(data interface{}) *Supabase {
	s.Payload = data
	s.AddHeader("Prefer", "resolution=merge-duplicates")
	return s
}

func (s *Supabase) Single() *Supabase {
	s.AddHeader("Accept", "application/vnd.pgrst.object+json")
	return s
}

func (s *Supabase) RPC(functionName string, params interface{}) *Supabase {
	baseUrl := s.Config.SupabaseUrl()
	s.Url = fmt.Sprintf("%s/rest/v1/rpc/%s", baseUrl, functionName)
	s.Payload = params
	s.Headers["Authorization"] = fmt.Sprintf("Bearer %s", s.Config.SupabaseApiKey)
	return s
}

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
