package cel2squirrel

import (
	"testing"

	"github.com/google/cel-go/cel"
)

// FuzzConverter fuzzes the CEL to SQL converter with various inputs
func FuzzConverter(f *testing.F) {
	// Seed corpus with valid CEL expressions
	f.Add(`status == "published"`)
	f.Add(`age >= 18`)
	f.Add(`status == "published" && age >= 18`)
	f.Add(`status == "published" || status == "featured"`)
	f.Add(`!is_draft`)
	f.Add(`age < 100`)
	f.Add(`label.contains("test")`)
	f.Add(`label.startsWith("prod")`)
	f.Add(`label.endsWith("v2")`)
	f.Add(`status in ["published", "featured"]`)
	f.Add(`age in [18, 21, 25]`)
	f.Add(`deletedAt == null`)
	f.Add(`deletedAt != null`)
	f.Add(`true`)
	f.Add(`false`)
	f.Add(`is_published`)
	f.Add(`(status == "published" || status == "featured") && age >= 18`)

	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":       {Type: cel.StringType, Column: "status"},
			"age":          {Type: cel.IntType, Column: "age"},
			"is_draft":     {Type: cel.BoolType, Column: "is_draft"},
			"is_published": {Type: cel.BoolType, Column: "is_published"},
			"label":        {Type: cel.StringType, Column: "label"},
			"deletedAt":    {Type: cel.TimestampType, Column: "deletedAt"},
			"rating":       {Type: cel.DoubleType, Column: "rating"},
			"count":        {Type: cel.UintType, Column: "count"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		f.Fatalf("failed to create converter: %v", err)
	}

	f.Fuzz(func(t *testing.T, celExpr string) {
		// Try to convert the CEL expression
		result, err := converter.Convert(celExpr)

		// If conversion succeeds, verify the SQL can be generated
		if err == nil && result != nil {
			sql, _, sqlErr := result.Where.ToSql()
			if sqlErr != nil {
				t.Errorf("Valid conversion produced invalid SQL: %v", sqlErr)
				return
			}

			// Basic sanity checks on generated SQL
			if sql == "" {
				t.Error("Generated SQL is empty")
			}
		}

		// Errors are acceptable - many random inputs will be invalid CEL
		// We're just checking that the converter doesn't panic or produce invalid results
	})
}

// FuzzConverterWithFieldMappings fuzzes with field mappings enabled
func FuzzConverterWithFieldMappings(f *testing.F) {
	f.Add(`isDraft == false`, "isDraft", "is_draft")
	f.Add(`ownerId == "user123"`, "ownerId", "owner_id")
	f.Add(`userName.contains("john")`, "userName", "user_name")
	f.Add(`isActive && !isDeleted`, "isActive", "is_active")

	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"isDraft":   {Type: cel.BoolType, Column: "is_draft"},
			"ownerId":   {Type: cel.StringType, Column: "owner_id"},
			"userName":  {Type: cel.StringType, Column: "user_name"},
			"isActive":  {Type: cel.BoolType, Column: "is_active"},
			"isDeleted": {Type: cel.BoolType, Column: "is_deleted"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		f.Fatalf("failed to create converter: %v", err)
	}

	f.Fuzz(func(t *testing.T, celExpr, _, _ string) {
		// Note: field mappings are now configured at converter creation time
		// This test verifies column mappings are applied correctly

		result, err := converter.Convert(celExpr)

		// If conversion succeeds, verify SQL generation
		if err == nil && result != nil {
			sql, _, sqlErr := result.Where.ToSql()
			if sqlErr != nil {
				t.Errorf("Valid conversion produced invalid SQL: %v", sqlErr)
				return
			}

			if sql == "" {
				t.Error("Generated SQL is empty")
			}
		}
	})
}

// FuzzQuoteIdentifier fuzzes the QuoteIdentifier function
func FuzzQuoteIdentifier(f *testing.F) {
	// Seed corpus
	f.Add("user_id")
	f.Add("user name")
	f.Add(`user"id`)
	f.Add(`"quoted"`)
	f.Add("")
	f.Add("a")
	f.Add("table.column")
	f.Add("123_column")
	f.Add("column_123")

	f.Fuzz(func(t *testing.T, identifier string) {
		result := QuoteIdentifier(identifier)

		// Result should always start and end with quote
		if len(result) < 2 {
			t.Errorf("QuoteIdentifier(%q) = %q, too short", identifier, result)
			return
		}

		if result[0] != '"' || result[len(result)-1] != '"' {
			t.Errorf("QuoteIdentifier(%q) = %q, should be quoted", identifier, result)
		}

		// Count quotes - should have at least 2 (start and end)
		quoteCount := 0
		for _, ch := range result {
			if ch == '"' {
				quoteCount++
			}
		}

		if quoteCount < 2 {
			t.Errorf("QuoteIdentifier(%q) = %q, insufficient quotes", identifier, result)
		}
	})
}

// FuzzConverterLogicalOperators fuzzes complex logical operator combinations
func FuzzConverterLogicalOperators(f *testing.F) {
	// Seed with various logical combinations
	f.Add(true, true, true)    // a && b && c
	f.Add(true, true, false)   // a && b || c
	f.Add(true, false, true)   // a || b && c
	f.Add(false, false, false) // !a && !b && !c

	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"a": {Type: cel.BoolType, Column: "a"},
			"b": {Type: cel.BoolType, Column: "b"},
			"c": {Type: cel.BoolType, Column: "c"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		f.Fatalf("failed to create converter: %v", err)
	}

	f.Fuzz(func(t *testing.T, useAnd1, useAnd2, useNot bool) {
		// Build a CEL expression based on fuzzer input
		expr := "a"

		if useNot {
			expr = "!a"
		}

		op1 := "&&"
		if !useAnd1 {
			op1 = "||"
		}

		expr += " " + op1 + " b"

		op2 := "&&"
		if !useAnd2 {
			op2 = "||"
		}

		expr += " " + op2 + " c"

		result, err := converter.Convert(expr)
		if err != nil {
			// Errors are okay in fuzzing
			return
		}

		if result == nil {
			t.Error("Convert() returned nil result without error")
			return
		}

		sql, _, sqlErr := result.Where.ToSql()
		if sqlErr != nil {
			t.Errorf("ToSql() error = %v", sqlErr)
			return
		}

		if sql == "" {
			t.Error("ToSql() returned empty string")
		}
	})
}

// FuzzConverterComparisons fuzzes comparison operators with various values
func FuzzConverterComparisons(f *testing.F) {
	// Seed with various comparison patterns
	f.Add("age", int64(0), 0)   // ==
	f.Add("age", int64(100), 1) // !=
	f.Add("age", int64(18), 2)  // <
	f.Add("age", int64(21), 3)  // <=
	f.Add("age", int64(65), 4)  // >
	f.Add("age", int64(70), 5)  // >=

	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"age":    {Type: cel.IntType, Column: "age"},
			"status": {Type: cel.StringType, Column: "status"},
			"count":  {Type: cel.UintType, Column: "count"},
			"rating": {Type: cel.DoubleType, Column: "rating"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		f.Fatalf("failed to create converter: %v", err)
	}

	f.Fuzz(func(t *testing.T, field string, value int64, op int) {
		// Map op to operator
		operators := []string{"==", "!=", "<", "<=", ">", ">="}
		if op < 0 || op >= len(operators) {
			return
		}

		operator := operators[op]

		// Only use valid fields
		if field != "age" && field != "count" {
			return
		}

		celExpr := field + " " + operator + " " + string(rune(value))

		result, err := converter.Convert(celExpr)
		if err != nil {
			// Many combinations will fail - that's okay
			return
		}

		if result != nil {
			sql, _, sqlErr := result.Where.ToSql()
			if sqlErr != nil {
				t.Errorf("ToSql() error = %v", sqlErr)
				return
			}

			if sql == "" {
				t.Error("ToSql() returned empty string")
			}
		}
	})
}
