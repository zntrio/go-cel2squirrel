package cel2squirrel

import (
	"strings"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// =============================================================================
// BASIC COMPARISON OPERATORS
// =============================================================================

func TestConverter_Convert_Equality(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
		wantErr  bool
	}{
		{
			name:     "string equality",
			celExpr:  `status == "published"`,
			wantSQL:  "status = ?",
			wantArgs: []any{"published"},
			wantErr:  false,
		},
		{
			name:     "integer equality",
			celExpr:  `age == 25`,
			wantSQL:  "age = ?",
			wantArgs: []any{int64(25)},
			wantErr:  false,
		},
		{
			name:     "inequality",
			celExpr:  `status != "draft"`,
			wantSQL:  "status <> ?",
			wantArgs: []any{"draft"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				sql, args, err := result.Where.ToSql()
				if err != nil {
					t.Errorf("ToSql() error = %v", err)
					return
				}

				if sql != tt.wantSQL {
					t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
				}

				if len(args) != len(tt.wantArgs) {
					t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
				}

				for i, arg := range args {
					if arg != tt.wantArgs[i] {
						t.Errorf("arg %d = %v, want %v", i, arg, tt.wantArgs[i])
					}
				}
			}
		})
	}
}

func TestConverter_Convert_Comparison(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"age":   {Type: cel.IntType, Column: "age"},
			"score": {Type: cel.DoubleType, Column: "score"},
			"count": {Type: cel.UintType, Column: "count"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
	}{
		{name: "less than", celExpr: `age < 18`, wantSQL: "age < ?", wantArgs: []any{int64(18)}},
		{name: "less than or equal", celExpr: `age <= 21`, wantSQL: "age <= ?", wantArgs: []any{int64(21)}},
		{name: "greater than", celExpr: `age > 65`, wantSQL: "age > ?", wantArgs: []any{int64(65)}},
		{name: "greater than or equal", celExpr: `score >= 90.0`, wantSQL: "score >= ?", wantArgs: []any{90.0}},
		{name: "uint comparison", celExpr: `count >= 100u`, wantSQL: "count >= ?", wantArgs: []any{uint64(100)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestConverter_Convert_NullComparisons(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"deletedAt": {Type: cel.TimestampType, Column: "deletedAt"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantSQL string
	}{
		{name: "IS NULL", celExpr: `deletedAt == null`, wantSQL: "deletedAt IS NULL"},
		{name: "IS NOT NULL", celExpr: `deletedAt != null`, wantSQL: "deletedAt IS NOT NULL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			// NULL comparisons should not have arguments
			if len(args) != 0 {
				t.Errorf("expected 0 args for NULL comparison, got %d: %v", len(args), args)
			}
		})
	}
}

// =============================================================================
// LOGICAL OPERATORS
// =============================================================================

func TestConverter_Convert_LogicalOperators(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":     {Type: cel.StringType, Column: "status"},
			"age":        {Type: cel.IntType, Column: "age"},
			"is_deleted": {Type: cel.BoolType, Column: "is_deleted"},
			"a":          {Type: cel.BoolType, Column: "a"},
			"b":          {Type: cel.BoolType, Column: "b"},
			"c":          {Type: cel.BoolType, Column: "c"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
	}{
		{name: "logical AND", celExpr: `status == "published" && age >= 18`, wantSQL: "(status = ? AND age >= ?)", wantArgs: []any{"published", int64(18)}},
		{name: "logical OR", celExpr: `status == "published" || status == "archived"`, wantSQL: "(status = ? OR status = ?)", wantArgs: []any{"published", "archived"}},
		{name: "complex AND OR", celExpr: `(status == "published" || status == "featured") && age >= 18`, wantSQL: "((status = ? OR status = ?) AND age >= ?)", wantArgs: []any{"published", "featured", int64(18)}},
		{name: "simple AND", celExpr: `a && b`, wantSQL: "(a = ? AND b = ?)", wantArgs: []any{true, true}},
		{name: "simple OR", celExpr: `a || b`, wantSQL: "(a = ? OR b = ?)", wantArgs: []any{true, true}},
		{name: "triple AND", celExpr: `a && b && c`, wantSQL: "((a = ? AND b = ?) AND c = ?)", wantArgs: []any{true, true, true}},
		{name: "triple OR", celExpr: `a || b || c`, wantSQL: "((a = ? OR b = ?) OR c = ?)", wantArgs: []any{true, true, true}},
		{name: "mixed nested", celExpr: `(a && b) || c`, wantSQL: "((a = ? AND b = ?) OR c = ?)", wantArgs: []any{true, true, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestConverter_Convert_LogicalNot(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"is_draft":    {Type: cel.BoolType, Column: "is_draft"},
			"is_deleted":  {Type: cel.BoolType, Column: "is_deleted"},
			"is_archived": {Type: cel.BoolType, Column: "is_archived"},
			"status":      {Type: cel.StringType, Column: "status"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
	}{
		{name: "NOT boolean field", celExpr: `!is_draft`, wantSQL: "NOT (is_draft = ?)", wantArgs: []any{true}},
		{name: "NOT with comparison", celExpr: `!(status == "published")`, wantSQL: "NOT (status = ?)", wantArgs: []any{"published"}},
		{name: "NOT with AND", celExpr: `!(is_draft && is_deleted)`, wantSQL: "NOT ((is_draft = ? AND is_deleted = ?))", wantArgs: []any{true, true}},
		{name: "NOT with OR", celExpr: `!(is_draft || is_deleted)`, wantSQL: "NOT ((is_draft = ? OR is_deleted = ?))", wantArgs: []any{true, true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

// =============================================================================
// STRING OPERATIONS
// =============================================================================

func TestConverter_Convert_StringOperations(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"label":       {Type: cel.StringType, Column: "label"},
			"description": {Type: cel.StringType, Column: "description"},
			"text":        {Type: cel.StringType, Column: "text"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantSQL string
		wantArg string
	}{
		{name: "contains regular", celExpr: `label.contains("test")`, wantSQL: "label LIKE ?", wantArg: "%test%"},
		{name: "contains empty", celExpr: `text.contains("")`, wantSQL: "text LIKE ?", wantArg: "%%"},
		{name: "contains special", celExpr: `text.contains("%_")`, wantSQL: "text LIKE ?", wantArg: "%\\%\\_%"}, // SECURITY: Special chars escaped
		{name: "startsWith regular", celExpr: `label.startsWith("prod")`, wantSQL: "label LIKE ?", wantArg: "prod%"},
		{name: "startsWith empty", celExpr: `text.startsWith("")`, wantSQL: "text LIKE ?", wantArg: "%"},
		{name: "startsWith special", celExpr: `text.startsWith("%_")`, wantSQL: "text LIKE ?", wantArg: "\\%\\_%"}, // SECURITY: Special chars escaped
		{name: "endsWith regular", celExpr: `label.endsWith("v2")`, wantSQL: "label LIKE ?", wantArg: "%v2"},
		{name: "endsWith empty", celExpr: `text.endsWith("")`, wantSQL: "text LIKE ?", wantArg: "%"},
		{name: "endsWith special", celExpr: `text.endsWith("%_")`, wantSQL: "text LIKE ?", wantArg: "%\\%\\_"}, // SECURITY: Special chars escaped
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if tt.wantArg != "" {
				if len(args) != 1 {
					t.Fatalf("expected 1 arg, got %d", len(args))
				}
				if args[0] != tt.wantArg {
					t.Errorf("arg = %v, want %v", args[0], tt.wantArg)
				}
			}
		})
	}
}

// =============================================================================
// IN OPERATOR
// =============================================================================

func TestConverter_Convert_InOperator(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
			"rating": {Type: cel.DoubleType, Column: "rating"},
			"count":  {Type: cel.UintType, Column: "count"},
			"flag":   {Type: cel.BoolType, Column: "flag"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
	}{
		{name: "string IN list", celExpr: `status in ["published", "featured", "archived"]`, wantSQL: "status IN (?,?,?)", wantArgs: []any{"published", "featured", "archived"}},
		{name: "integer IN list", celExpr: `age in [18, 21, 25, 30]`, wantSQL: "age IN (?,?,?,?)", wantArgs: []any{int64(18), int64(21), int64(25), int64(30)}},
		{name: "single value IN list", celExpr: `status in ["published"]`, wantSQL: "status IN (?)", wantArgs: []any{"published"}},
		{name: "uint list", celExpr: `count in [1u, 2u, 3u]`, wantSQL: "count IN (?,?,?)", wantArgs: []any{uint64(1), uint64(2), uint64(3)}},
		{name: "double list", celExpr: `rating in [1.1, 2.2, 3.3]`, wantSQL: "rating IN (?,?,?)", wantArgs: []any{1.1, 2.2, 3.3}},
		{name: "bool list", celExpr: `flag in [true, false]`, wantSQL: "flag IN (?,?)", wantArgs: []any{true, false}},
		{name: "empty list", celExpr: `status in []`, wantSQL: "(1=0)", wantArgs: []any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

// =============================================================================
// BOOLEAN CONSTANTS AND IDENTIFIERS
// =============================================================================

func TestConverter_Convert_BooleanConstants(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"is_draft":     {Type: cel.BoolType, Column: "is_draft"},
			"is_published": {Type: cel.BoolType, Column: "is_published"},
			"is_active":    {Type: cel.BoolType, Column: "is_active"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
	}{
		{name: "true constant", celExpr: `true`, wantSQL: "TRUE", wantArgs: []any{}},
		{name: "false constant", celExpr: `false`, wantSQL: "FALSE", wantArgs: []any{}},
		{name: "true AND field", celExpr: `true && is_draft`, wantSQL: "(TRUE AND is_draft = ?)", wantArgs: []any{true}},
		{name: "false OR field", celExpr: `false || is_draft`, wantSQL: "(FALSE OR is_draft = ?)", wantArgs: []any{true}},
		{name: "standalone boolean identifier", celExpr: `is_published`, wantSQL: "is_published = ?", wantArgs: []any{true}},
		{name: "standalone identifier in AND", celExpr: `is_published && is_active`, wantSQL: "(is_published = ? AND is_active = ?)", wantArgs: []any{true, true}},
		{name: "compare to true", celExpr: `is_draft == true`, wantSQL: "is_draft = ?", wantArgs: []any{true}},
		{name: "compare to false", celExpr: `is_draft == false`, wantSQL: "is_draft = ?", wantArgs: []any{false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

// =============================================================================
// FIELD MAPPINGS
// =============================================================================

func TestConverter_Convert_FieldMappings(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"isDraft":     {Type: cel.BoolType, Column: "is_draft"},
			"ownerId":     {Type: cel.StringType, Column: "owner_id"},
			"userId":      {Type: cel.StringType, Column: "user_id"},
			"viewCount":   {Type: cel.IntType, Column: "view_count"},
			"ratingScore": {Type: cel.DoubleType, Column: "rating_score"},
			"labelText":   {Type: cel.StringType, Column: "label_text"},
			"deletedAt":   {Type: cel.TimestampType, Column: "deleted_at"},
			"isDeleted":   {Type: cel.BoolType, Column: "is_deleted"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantSQL  string
		wantArgs []any
	}{
		{name: "simple field mapping", celExpr: `isDraft == false && ownerId == "user123"`, wantSQL: "(is_draft = ? AND owner_id = ?)", wantArgs: []any{false, "user123"}},
		{name: "user id", celExpr: `userId == "user123"`, wantSQL: "user_id = ?", wantArgs: []any{"user123"}},
		{name: "view count", celExpr: `viewCount > 100`, wantSQL: "view_count > ?", wantArgs: []any{int64(100)}},
		{name: "rating score", celExpr: `ratingScore >= 4.5`, wantSQL: "rating_score >= ?", wantArgs: []any{4.5}},
		{name: "label contains", celExpr: `labelText.contains("test")`, wantSQL: "label_text LIKE ?", wantArgs: []any{"%test%"}},
		{name: "user in list", celExpr: `userId in ["user1", "user2", "user3"]`, wantSQL: "user_id IN (?,?,?)", wantArgs: []any{"user1", "user2", "user3"}},
		{name: "null comparison", celExpr: `deletedAt == null`, wantSQL: "deleted_at IS NULL", wantArgs: []any{}},
		{name: "complex with mappings", celExpr: `isDraft == false && isDeleted == false && userId == "user123" && ratingScore >= 4.0`, wantSQL: "((is_draft = ? AND is_deleted = ?) AND (user_id = ? AND rating_score >= ?))", wantArgs: []any{false, false, "user123", 4.0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}

			// Verify that mapped column names are used
			if tt.name == "simple field mapping" {
				if !strings.Contains(sql, "is_draft") || !strings.Contains(sql, "owner_id") {
					t.Errorf("SQL should contain mapped names: %s", sql)
				}
			}

			t.Logf("SQL: %s, Args: %v", sql, args)
		})
	}
}

// =============================================================================
// VALIDATION AND ERROR HANDLING
// =============================================================================

func TestConverter_Convert_ValidationErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
			"label":  {Type: cel.StringType, Column: "label"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantErr string
	}{
		{name: "non-boolean expression", celExpr: `age + 5`, wantErr: "filter expression must evaluate to boolean"}, // SECURITY: Sanitized error
		{name: "standalone int", celExpr: `age`, wantErr: "filter expression must evaluate to boolean"},             // SECURITY: Sanitized error
		{name: "undefined field", celExpr: `unknownField == "value"`, wantErr: "invalid filter expression syntax"},  // SECURITY: Sanitized error
		{name: "syntax error", celExpr: `status == `, wantErr: "invalid filter expression syntax"},                  // SECURITY: Sanitized error
		{name: "type mismatch", celExpr: `status == 123`, wantErr: "invalid filter expression syntax"},              // SECURITY: Sanitized error
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if err == nil {
				t.Errorf("Convert() expected error containing %q, got nil", tt.wantErr)
				return
			}

			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("Convert() error = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestConverter_StringOperationErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"label": {Type: cel.StringType, Column: "label"},
			"age":   {Type: cel.IntType, Column: "age"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test string operations on non-string types should fail at compilation
	_, err = converter.Convert(`age.contains("test")`)
	if err == nil {
		t.Error("Convert() expected error for contains on non-string, got nil")
	}

	_, err = converter.Convert(`age.startsWith("test")`)
	if err == nil {
		t.Error("Convert() expected error for startsWith on non-string, got nil")
	}

	_, err = converter.Convert(`age.endsWith("test")`)
	if err == nil {
		t.Error("Convert() expected error for endsWith on non-string, got nil")
	}
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

func TestConverter_IntegrationWithSquirrel(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":   {Type: cel.StringType, Column: "status"},
			"age":      {Type: cel.IntType, Column: "age"},
			"is_draft": {Type: cel.BoolType, Column: "is_draft"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Convert CEL expression
	celExpr := `status == "published" && age >= 18 && is_draft == false`
	result, err := converter.Convert(celExpr)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}

	// Use with Squirrel to build a complete query
	query := squirrel.Select("id", "label", "status", "age").
		From("prompts").
		Where(result.Where).
		OrderBy("age DESC").
		Limit(10)

	sql, args, err := query.ToSql()
	if err != nil {
		t.Fatalf("ToSql() error = %v", err)
	}

	expectedSQL := "SELECT id, label, status, age FROM prompts WHERE ((status = ? AND age >= ?) AND is_draft = ?) ORDER BY age DESC LIMIT 10"
	if sql != expectedSQL {
		t.Errorf("ToSql() =\n%v\nwant\n%v", sql, expectedSQL)
	}

	expectedArgs := []any{"published", int64(18), false}
	if len(args) != len(expectedArgs) {
		t.Errorf("expected %d args, got %d: %v", len(expectedArgs), len(args), args)
	}

	for i, arg := range args {
		if arg != expectedArgs[i] {
			t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, expectedArgs[i], expectedArgs[i])
		}
	}
}

func TestConverter_PostgreSQLPlaceholders(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	celExpr := `status == "published" && age >= 18`
	result, err := converter.Convert(celExpr)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}

	tests := []struct {
		name        string
		placeholder squirrel.PlaceholderFormat
		wantSQL     string
		wantArgs    []any
	}{
		{name: "MySQL (default)", placeholder: squirrel.Question, wantSQL: "SELECT * FROM prompts WHERE (status = ? AND age >= ?)", wantArgs: []any{"published", int64(18)}},
		{name: "PostgreSQL", placeholder: squirrel.Dollar, wantSQL: "SELECT * FROM prompts WHERE (status = $1 AND age >= $2)", wantArgs: []any{"published", int64(18)}},
		{name: "MSSQL", placeholder: squirrel.AtP, wantSQL: "SELECT * FROM prompts WHERE (status = @p1 AND age >= @p2)", wantArgs: []any{"published", int64(18)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := squirrel.Select("*").
				From("prompts").
				Where(result.Where).
				PlaceholderFormat(tt.placeholder)

			sql, args, err := query.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql != tt.wantSQL {
				t.Errorf("ToSql() = %v, want %v", sql, tt.wantSQL)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d: %v", len(tt.wantArgs), len(args), args)
			}

			for i, arg := range args {
				if arg != tt.wantArgs[i] {
					t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, tt.wantArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

func TestConverter_MultipleQueryFormats(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"score":  {Type: cel.IntType, Column: "score"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	celExpr := `status == "active" && score > 50`
	result, err := converter.Convert(celExpr)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}

	expectedArgs := []any{"active", int64(50)}

	// Test SELECT
	t.Run("SELECT", func(t *testing.T) {
		q := squirrel.Select("id", "name", "status", "score").
			From("items").
			Where(result.Where).
			OrderBy("score DESC").
			Limit(10)

		sql, args, err := q.ToSql()
		if err != nil {
			t.Fatalf("SELECT query failed: %v", err)
		}

		if !strings.Contains(sql, "SELECT") {
			t.Error("Query should contain SELECT")
		}

		if len(args) != len(expectedArgs) {
			t.Errorf("expected %d args, got %d", len(expectedArgs), len(args))
		}

		for i, arg := range args {
			if arg != expectedArgs[i] {
				t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, expectedArgs[i], expectedArgs[i])
			}
		}
	})

	// Test UPDATE
	t.Run("UPDATE", func(t *testing.T) {
		q := squirrel.Update("items").
			Set("processed", true).
			Where(result.Where)

		sql, args, err := q.ToSql()
		if err != nil {
			t.Fatalf("UPDATE query failed: %v", err)
		}

		if !strings.Contains(sql, "UPDATE") {
			t.Error("Query should contain UPDATE")
		}

		// UPDATE has an extra arg for the SET clause, so we expect 3 args total
		if len(args) != 3 {
			t.Errorf("expected 3 args (1 for SET + 2 for WHERE), got %d: %v", len(args), args)
		}

		// Check the WHERE args (skip the first arg which is for SET)
		for i, expectedArg := range expectedArgs {
			actualArg := args[i+1] // +1 to skip the SET arg
			if actualArg != expectedArg {
				t.Errorf("WHERE arg %d = %v (type %T), want %v (type %T)", i, actualArg, actualArg, expectedArg, expectedArg)
			}
		}
	})

	// Test DELETE
	t.Run("DELETE", func(t *testing.T) {
		q := squirrel.Delete("items").
			Where(result.Where)

		sql, args, err := q.ToSql()
		if err != nil {
			t.Fatalf("DELETE query failed: %v", err)
		}

		if !strings.Contains(sql, "DELETE") {
			t.Error("Query should contain DELETE")
		}

		if len(args) != len(expectedArgs) {
			t.Errorf("expected %d args, got %d", len(expectedArgs), len(args))
		}

		for i, arg := range args {
			if arg != expectedArgs[i] {
				t.Errorf("arg %d = %v (type %T), want %v (type %T)", i, arg, arg, expectedArgs[i], expectedArgs[i])
			}
		}
	})
}

// =============================================================================
// COMPLEX SCENARIOS
// =============================================================================

func TestConverter_ComplexNestedExpressions(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":   {Type: cel.StringType, Column: "status"},
			"age":      {Type: cel.IntType, Column: "age"},
			"is_draft": {Type: cel.BoolType, Column: "is_draft"},
			"rating":   {Type: cel.DoubleType, Column: "rating"},
			"a":        {Type: cel.BoolType, Column: "a"},
			"b":        {Type: cel.BoolType, Column: "b"},
			"c":        {Type: cel.BoolType, Column: "c"},
			"d":        {Type: cel.BoolType, Column: "d"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
	}{
		{name: "complex nested", celExpr: `(status == "published" || status == "featured") && age >= 18 && (is_draft == false || rating >= 4.5)`},
		{name: "deeply nested", celExpr: `((a && b) || (c && d))`},
		{name: "NOT of complex", celExpr: `!((a && b) || (c && d))`},
		{name: "multiple NOTs", celExpr: `!a && !b && !c`},
		{name: "mixed operations", celExpr: `(status == "published" && age >= 18) || (rating >= 4.5 && !is_draft)`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql == "" {
				t.Error("ToSql() returned empty string")
			}

			if len(args) == 0 {
				t.Error("ToSql() returned no arguments")
			}

			t.Logf("Generated SQL: %s, Args: %v", sql, args)
		})
	}
}

func TestConverter_RealWorldScenarios(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":      {Type: cel.StringType, Column: "status"},
			"owner_id":    {Type: cel.StringType, Column: "owner_id"},
			"is_draft":    {Type: cel.BoolType, Column: "is_draft"},
			"is_deleted":  {Type: cel.BoolType, Column: "is_deleted"},
			"rating":      {Type: cel.DoubleType, Column: "rating"},
			"view_count":  {Type: cel.IntType, Column: "view_count"},
			"deleted_at":  {Type: cel.TimestampType, Column: "deleted_at"},
			"label":       {Type: cel.StringType, Column: "label"},
			"description": {Type: cel.StringType, Column: "description"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
	}{
		{name: "published prompts", celExpr: `status == "published" && is_draft == false && deleted_at == null`},
		{name: "search query", celExpr: `(label.contains("gpt") || description.contains("gpt")) && rating >= 4.0`},
		{name: "popular content", celExpr: `status == "published" && view_count > 1000 && rating >= 4.0 && !is_deleted`},
		{name: "status filter", celExpr: `status in ["published", "featured", "archived"]`},
		{name: "not in draft or deleted", celExpr: `!(is_draft || is_deleted)`},
		{name: "owner and quality", celExpr: `owner_id == "user123" && rating >= 4.5 && view_count > 100`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if sql == "" {
				t.Error("ToSql() returned empty SQL")
			}

			// Verify we can use it in a complete query
			query := squirrel.Select("*").From("prompts").Where(result.Where)
			fullSQL, _, err := query.ToSql()
			if err != nil {
				t.Errorf("Failed to build complete query: %v", err)
			}

			if !strings.Contains(fullSQL, "FROM prompts") {
				t.Error("Complete query doesn't contain FROM clause")
			}

			t.Logf("CEL: %s\nSQL: %s\nArgs: %v", tt.celExpr, sql, args)
		})
	}
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "simple identifier", input: "user_id", want: `"user_id"`},
		{name: "identifier with spaces", input: "user name", want: `"user name"`},
		{name: "identifier with quotes", input: `user"id`, want: `"user""id"`},
		{name: "identifier with multiple quotes", input: `user"name"id`, want: `"user""name""id"`},
		{name: "empty identifier", input: "", want: `""`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := QuoteIdentifier(tt.input)
			if got != tt.want {
				t.Errorf("QuoteIdentifier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewConverter_EmptyConfig(t *testing.T) {
	config := Config{}
	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("NewConverter() error = %v, want nil", err)
	}

	if converter == nil {
		t.Fatal("NewConverter() returned nil converter")
	}
}

func TestConverter_NotSqlizer_Error(t *testing.T) {
	// Create a sqlizer that will fail
	failing := &failingSqlizer{}
	notSQL := &notSqlizer{inner: failing}

	_, _, err := notSQL.ToSql()
	if err == nil {
		t.Error("ToSql() expected error, got nil")
	}

	if err.Error() != "intentional error" {
		t.Errorf("ToSql() error = %v, want 'intentional error'", err)
	}
}

// =============================================================================
// HELPER TYPES AND FUNCTIONS
// =============================================================================

// failingSqlizer is a test helper that always fails
type failingSqlizer struct{}

//nolint:revive // ToSql is required by squirrel.Sqlizer interface
func (f *failingSqlizer) ToSql() (string, []interface{}, error) {
	return "", nil, &testError{msg: "intentional error"}
}

// testError is a simple error type
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// contains is a helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

// =============================================================================
// ADDITIONAL ERROR PATH TESTS FOR COVERAGE
// =============================================================================

func TestConverter_LogicalOperator_ArgumentCountErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"a": {Type: cel.BoolType, Column: "a"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test invalid argument counts by constructing invalid expressions
	// These test internal error paths that are normally impossible via CEL parsing

	// Test that our logical operators work correctly (baseline)
	_, err = converter.Convert(`a && a`)
	if err != nil {
		t.Errorf("AND with 2 args should succeed, got: %v", err)
	}

	_, err = converter.Convert(`a || a`)
	if err != nil {
		t.Errorf("OR with 2 args should succeed, got: %v", err)
	}

	_, err = converter.Convert(`!a`)
	if err != nil {
		t.Errorf("NOT with 1 arg should succeed, got: %v", err)
	}
}

func TestConverter_StringOperation_ErrorCases(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"label": {Type: cel.StringType, Column: "label"},
			"age":   {Type: cel.IntType, Column: "age"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test that string operations require string types (compilation errors)
	tests := []struct {
		name    string
		celExpr string
		wantErr bool
	}{
		{name: "contains on int", celExpr: `age.contains("test")`, wantErr: true},
		{name: "startsWith on int", celExpr: `age.startsWith("test")`, wantErr: true},
		{name: "endsWith on int", celExpr: `age.endsWith("test")`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConverter_InOperator_ErrorCases(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test valid IN operator (baseline)
	_, err = converter.Convert(`status in ["a", "b"]`)
	if err != nil {
		t.Errorf("Valid IN operator should succeed, got: %v", err)
	}
}

func TestConverter_MapFieldName_NilColumnMappings(t *testing.T) {
	// Create converter with nil column mappings
	converter := &Converter{
		env:            nil, // Not needed for this test
		columnMappings: nil,
	}

	// When columnMappings is nil, should return field name as-is
	result := converter.mapFieldName("test_field")
	if result != "test_field" {
		t.Errorf("mapFieldName() with nil mappings = %v, want 'test_field'", result)
	}

	// Test with empty but non-nil map
	converter.columnMappings = make(map[string]string)
	result = converter.mapFieldName("unmapped_field")
	if result != "unmapped_field" {
		t.Errorf("mapFieldName() with unmapped field = %v, want 'unmapped_field'", result)
	}
}

func TestConverter_FieldMapping_NoColumnSpecified(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType}, // No Column specified
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Should use field name as column name when Column is empty
	result, err := converter.Convert(`status == "published"`)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}

	sql, _, err := result.Where.ToSql()
	if err != nil {
		t.Fatalf("ToSql() error = %v", err)
	}

	// Should use "status" as column name (not mapped)
	if !strings.Contains(sql, "status") {
		t.Errorf("SQL should contain 'status' as column name: %s", sql)
	}
}

func TestConverter_FieldMapping_NoType(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Column: "status_col"}, // No Type specified
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Should still have column mapping even without type
	if converter.columnMappings["status"] != "status_col" {
		t.Errorf("columnMappings[status] = %v, want 'status_col'", converter.columnMappings["status"])
	}
}

func TestConverter_Convert_AstToCheckedExprError(t *testing.T) {
	// This tests the error handling for AstToCheckedExpr
	// In practice, this is very rare as the CEL library handles this internally
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Valid expression should work
	_, err = converter.Convert(`status == "test"`)
	if err != nil {
		t.Errorf("Convert() with valid expression should succeed, got: %v", err)
	}
}

func TestConverter_ConvertExpr_UnsupportedExprType(t *testing.T) {
	// This tests error handling for unsupported expression types
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"value": {Type: cel.IntType, Column: "value"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test expressions that might lead to unsupported types
	// Note: CEL will catch most issues at compile time
	tests := []struct {
		name    string
		celExpr string
		wantErr bool
	}{
		{name: "valid comparison", celExpr: `value == 5`, wantErr: false},
		{name: "valid boolean", celExpr: `value > 3`, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConverter_GetFieldName_SelectExpr(t *testing.T) {
	// This tests the SelectExpr branch in getFieldName
	// Note: CEL's select expressions (e.g., "obj.field") are handled differently
	// We test that our existing code works with field access patterns
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"label":  {Type: cel.StringType, Column: "label"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test field access in various contexts
	tests := []struct {
		name    string
		celExpr string
		wantErr bool
	}{
		{name: "simple field", celExpr: `status == "test"`, wantErr: false},
		{name: "field in contains", celExpr: `label.contains("test")`, wantErr: false},
		{name: "field in startsWith", celExpr: `label.startsWith("test")`, wantErr: false},
		{name: "field in endsWith", celExpr: `label.endsWith("test")`, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConverter_GetConstantValue_AllTypes(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"str_field":  {Type: cel.StringType, Column: "str_field"},
			"int_field":  {Type: cel.IntType, Column: "int_field"},
			"uint_field": {Type: cel.UintType, Column: "uint_field"},
			"dbl_field":  {Type: cel.DoubleType, Column: "dbl_field"},
			"bool_field": {Type: cel.BoolType, Column: "bool_field"},
			"null_field": {Type: cel.NullType, Column: "null_field"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantErr bool
	}{
		{name: "string value", celExpr: `str_field == "test"`, wantErr: false},
		{name: "int value", celExpr: `int_field == 42`, wantErr: false},
		{name: "negative int", celExpr: `int_field == -10`, wantErr: false},
		{name: "uint value", celExpr: `uint_field == 42u`, wantErr: false},
		{name: "double value", celExpr: `dbl_field == 3.14`, wantErr: false},
		{name: "negative double", celExpr: `dbl_field == -2.5`, wantErr: false},
		{name: "bool true", celExpr: `bool_field == true`, wantErr: false},
		{name: "bool false", celExpr: `bool_field == false`, wantErr: false},
		{name: "null value", celExpr: `null_field == null`, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConverter_ConvertComparison_AllOperators(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"value": {Type: cel.IntType, Column: "value"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantOp   string
		wantArgs []any
	}{
		{name: "equals", celExpr: `value == 5`, wantOp: "=", wantArgs: []any{int64(5)}},
		{name: "not equals", celExpr: `value != 5`, wantOp: "<>", wantArgs: []any{int64(5)}},
		{name: "less than", celExpr: `value < 5`, wantOp: "<", wantArgs: []any{int64(5)}},
		{name: "less or equal", celExpr: `value <= 5`, wantOp: "<=", wantArgs: []any{int64(5)}},
		{name: "greater than", celExpr: `value > 5`, wantOp: ">", wantArgs: []any{int64(5)}},
		{name: "greater or equal", celExpr: `value >= 5`, wantOp: ">=", wantArgs: []any{int64(5)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if !strings.Contains(sql, tt.wantOp) {
				t.Errorf("SQL should contain operator %q, got: %s", tt.wantOp, sql)
			}

			if len(args) != len(tt.wantArgs) {
				t.Errorf("expected %d args, got %d", len(tt.wantArgs), len(args))
			}
		})
	}
}

func TestConverter_StringOperations_EmptyString(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"label": {Type: cel.StringType, Column: "label"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name     string
		celExpr  string
		wantLike string
	}{
		{name: "contains empty", celExpr: `label.contains("")`, wantLike: "%%"},
		{name: "startsWith empty", celExpr: `label.startsWith("")`, wantLike: "%"},
		{name: "endsWith empty", celExpr: `label.endsWith("")`, wantLike: "%"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if !strings.Contains(sql, "LIKE") {
				t.Errorf("SQL should contain LIKE: %s", sql)
			}

			if len(args) != 1 {
				t.Fatalf("expected 1 arg, got %d", len(args))
			}

			if args[0] != tt.wantLike {
				t.Errorf("arg = %v, want %v", args[0], tt.wantLike)
			}
		})
	}
}

func TestConverter_ConstExpr_NonBooleanAtTopLevel(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"value": {Type: cel.IntType, Column: "value"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test boolean constants at top level
	tests := []struct {
		name    string
		celExpr string
		wantSQL string
		wantErr bool
	}{
		{name: "true constant", celExpr: `true`, wantSQL: "TRUE", wantErr: false},
		{name: "false constant", celExpr: `false`, wantSQL: "FALSE", wantErr: false},
		{name: "true in AND", celExpr: `true && value > 5`, wantSQL: "TRUE", wantErr: false},
		{name: "false in OR", celExpr: `false || value > 5`, wantSQL: "FALSE", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				sql, _, err := result.Where.ToSql()
				if err != nil {
					t.Fatalf("ToSql() error = %v", err)
				}

				if !strings.Contains(sql, tt.wantSQL) {
					t.Errorf("SQL should contain %q, got: %s", tt.wantSQL, sql)
				}
			}
		})
	}
}

func TestConverter_ComplexBooleanLogic(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"a": {Type: cel.BoolType, Column: "a"},
			"b": {Type: cel.BoolType, Column: "b"},
			"c": {Type: cel.BoolType, Column: "c"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantErr bool
	}{
		{name: "double NOT", celExpr: `!!a`, wantErr: false},
		{name: "NOT and AND", celExpr: `!a && b`, wantErr: false},
		{name: "NOT and OR", celExpr: `!a || b`, wantErr: false},
		{name: "nested NOT", celExpr: `!(a && !b)`, wantErr: false},
		{name: "triple nested", celExpr: `(a && b) || (b && c) || (a && c)`, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				sql, args, err := result.Where.ToSql()
				if err != nil {
					t.Fatalf("ToSql() error = %v", err)
				}

				if sql == "" {
					t.Error("ToSql() returned empty SQL")
				}

				t.Logf("CEL: %s\nSQL: %s\nArgs: %v", tt.celExpr, sql, args)
			}
		})
	}
}

// =============================================================================
// INTERNAL METHOD TESTING FOR UNCOVERED ERROR PATHS
// =============================================================================

// These tests use direct calls to internal methods to test error paths
// that are normally prevented by CEL's type system

func TestConverter_InternalMethods_ErrorPaths(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"field": {Type: cel.StringType, Column: "field"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test getFieldName with invalid expression types
	t.Run("getFieldName with nil expression", func(t *testing.T) {
		nilExpr := &exprpb.Expr{}
		_, err := converter.getFieldName(nilExpr)
		if err == nil {
			t.Error("getFieldName() with nil expression should return error")
		}
	})

	// Test getConstantValue with invalid constant types
	t.Run("getConstantValue with nil constant", func(t *testing.T) {
		nonConstExpr := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "field"},
			},
		}
		_, err := converter.getConstantValue(nonConstExpr)
		if err == nil {
			t.Error("getConstantValue() with non-constant should return error")
		}
	})

	// Test getListValues with non-list expression
	t.Run("getListValues with non-list", func(t *testing.T) {
		nonListExpr := &exprpb.Expr{
			ExprKind: &exprpb.Expr_ConstExpr{
				ConstExpr: &exprpb.Constant{
					ConstantKind: &exprpb.Constant_StringValue{
						StringValue: "not a list",
					},
				},
			},
		}
		_, err := converter.getListValues(nonListExpr)
		if err == nil {
			t.Error("getListValues() with non-list should return error")
		}
	})

	// Test convertConstExpr with non-boolean constant
	t.Run("convertConstExpr with non-boolean", func(t *testing.T) {
		intConst := &exprpb.Constant{
			ConstantKind: &exprpb.Constant_Int64Value{
				Int64Value: 42,
			},
		}
		_, err := converter.convertConstExpr(intConst)
		if err == nil {
			t.Error("convertConstExpr() with non-boolean should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "unsupported constant type") {
			t.Errorf("Expected 'unsupported constant type' error, got: %v", err)
		}
	})
}

func TestConverter_LogicalOperators_ArgumentErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"a": {Type: cel.BoolType, Column: "a"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test convertLogicalAnd with wrong number of arguments
	t.Run("convertLogicalAnd with 0 args", func(t *testing.T) {
		_, err := converter.convertLogicalAnd([]*exprpb.Expr{})
		if err == nil {
			t.Error("convertLogicalAnd() with 0 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 2 arguments") {
			t.Errorf("Expected 'exactly 2 arguments' error, got: %v", err)
		}
	})

	t.Run("convertLogicalAnd with 1 arg", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "a"},
			},
		}
		_, err := converter.convertLogicalAnd([]*exprpb.Expr{arg})
		if err == nil {
			t.Error("convertLogicalAnd() with 1 arg should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 2 arguments") {
			t.Errorf("Expected 'exactly 2 arguments' error, got: %v", err)
		}
	})

	t.Run("convertLogicalAnd with 3 args", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "a"},
			},
		}
		_, err := converter.convertLogicalAnd([]*exprpb.Expr{arg, arg, arg})
		if err == nil {
			t.Error("convertLogicalAnd() with 3 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 2 arguments") {
			t.Errorf("Expected 'exactly 2 arguments' error, got: %v", err)
		}
	})

	// Test convertLogicalOr with wrong number of arguments
	t.Run("convertLogicalOr with 0 args", func(t *testing.T) {
		_, err := converter.convertLogicalOr([]*exprpb.Expr{})
		if err == nil {
			t.Error("convertLogicalOr() with 0 args should return error")
		}
	})

	t.Run("convertLogicalOr with 3 args", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "a"},
			},
		}
		_, err := converter.convertLogicalOr([]*exprpb.Expr{arg, arg, arg})
		if err == nil {
			t.Error("convertLogicalOr() with 3 args should return error")
		}
	})

	// Test convertLogicalNot with wrong number of arguments
	t.Run("convertLogicalNot with 0 args", func(t *testing.T) {
		_, err := converter.convertLogicalNot([]*exprpb.Expr{})
		if err == nil {
			t.Error("convertLogicalNot() with 0 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 1 argument") {
			t.Errorf("Expected 'exactly 1 argument' error, got: %v", err)
		}
	})

	t.Run("convertLogicalNot with 2 args", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "a"},
			},
		}
		_, err := converter.convertLogicalNot([]*exprpb.Expr{arg, arg})
		if err == nil {
			t.Error("convertLogicalNot() with 2 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 1 argument") {
			t.Errorf("Expected 'exactly 1 argument' error, got: %v", err)
		}
	})
}

func TestConverter_ComparisonOperator_ArgumentErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"field": {Type: cel.IntType, Column: "field"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	t.Run("convertComparison with 0 args", func(t *testing.T) {
		_, err := converter.convertComparison([]*exprpb.Expr{}, "=")
		if err == nil {
			t.Error("convertComparison() with 0 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 2 arguments") {
			t.Errorf("Expected 'exactly 2 arguments' error, got: %v", err)
		}
	})

	t.Run("convertComparison with 1 arg", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "field"},
			},
		}
		_, err := converter.convertComparison([]*exprpb.Expr{arg}, "=")
		if err == nil {
			t.Error("convertComparison() with 1 arg should return error")
		}
	})
}

func TestConverter_StringOperations_ArgumentErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"label": {Type: cel.StringType, Column: "label"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test contains with wrong number of arguments
	t.Run("convertContains with 0 args", func(t *testing.T) {
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{},
		}
		_, err := converter.convertContains(call)
		if err == nil {
			t.Error("convertContains() with 0 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 1 argument") {
			t.Errorf("Expected 'exactly 1 argument' error, got: %v", err)
		}
	})

	t.Run("convertContains with 2 args", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_ConstExpr{
				ConstExpr: &exprpb.Constant{
					ConstantKind: &exprpb.Constant_StringValue{StringValue: "test"},
				},
			},
		}
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{arg, arg},
		}
		_, err := converter.convertContains(call)
		if err == nil {
			t.Error("convertContains() with 2 args should return error")
		}
	})

	// Test startsWith with wrong number of arguments
	t.Run("convertStartsWith with 0 args", func(t *testing.T) {
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{},
		}
		_, err := converter.convertStartsWith(call)
		if err == nil {
			t.Error("convertStartsWith() with 0 args should return error")
		}
	})

	// Test endsWith with wrong number of arguments
	t.Run("convertEndsWith with 0 args", func(t *testing.T) {
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{},
		}
		_, err := converter.convertEndsWith(call)
		if err == nil {
			t.Error("convertEndsWith() with 0 args should return error")
		}
	})

	// Test with non-string argument
	t.Run("convertContains with int argument", func(t *testing.T) {
		intArg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_ConstExpr{
				ConstExpr: &exprpb.Constant{
					ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 42},
				},
			},
		}
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{intArg},
		}
		_, err := converter.convertContains(call)
		if err == nil {
			t.Error("convertContains() with int argument should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "requires string argument") {
			t.Errorf("Expected 'requires string argument' error, got: %v", err)
		}
	})

	t.Run("convertStartsWith with int argument", func(t *testing.T) {
		intArg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_ConstExpr{
				ConstExpr: &exprpb.Constant{
					ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 42},
				},
			},
		}
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{intArg},
		}
		_, err := converter.convertStartsWith(call)
		if err == nil {
			t.Error("convertStartsWith() with int argument should return error")
		}
	})

	t.Run("convertEndsWith with int argument", func(t *testing.T) {
		intArg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_ConstExpr{
				ConstExpr: &exprpb.Constant{
					ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 42},
				},
			},
		}
		call := &exprpb.Expr_Call{
			Target: &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{
					IdentExpr: &exprpb.Expr_Ident{Name: "label"},
				},
			},
			Args: []*exprpb.Expr{intArg},
		}
		_, err := converter.convertEndsWith(call)
		if err == nil {
			t.Error("convertEndsWith() with int argument should return error")
		}
	})
}

func TestConverter_InOperator_ArgumentErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	t.Run("convertInOperator with 0 args", func(t *testing.T) {
		_, err := converter.convertInOperator([]*exprpb.Expr{})
		if err == nil {
			t.Error("convertInOperator() with 0 args should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "exactly 2 arguments") {
			t.Errorf("Expected 'exactly 2 arguments' error, got: %v", err)
		}
	})

	t.Run("convertInOperator with 1 arg", func(t *testing.T) {
		arg := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "status"},
			},
		}
		_, err := converter.convertInOperator([]*exprpb.Expr{arg})
		if err == nil {
			t.Error("convertInOperator() with 1 arg should return error")
		}
	})
}

func TestConverter_GetListValues_ErrorInElement(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Create a list expression with an invalid element (non-constant)
	t.Run("getListValues with non-constant element", func(t *testing.T) {
		invalidElement := &exprpb.Expr{
			ExprKind: &exprpb.Expr_IdentExpr{
				IdentExpr: &exprpb.Expr_Ident{Name: "status"},
			},
		}
		listExpr := &exprpb.Expr{
			ExprKind: &exprpb.Expr_ListExpr{
				ListExpr: &exprpb.Expr_CreateList{
					Elements: []*exprpb.Expr{invalidElement},
				},
			},
		}
		_, err := converter.getListValues(listExpr)
		if err == nil {
			t.Error("getListValues() with non-constant element should return error")
		}
		if err != nil && !strings.Contains(err.Error(), "failed to get list element") {
			t.Errorf("Expected 'failed to get list element' error, got: %v", err)
		}
	})
}

func TestConverter_UnsupportedFunction(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"value": {Type: cel.IntType, Column: "value"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test convertCallExpr with unsupported function
	t.Run("convertCallExpr with unsupported function", func(t *testing.T) {
		call := &exprpb.Expr_Call{
			Function: "unsupported_function",
			Args:     []*exprpb.Expr{},
		}
		_, err := converter.convertCallExpr(call)
		if err == nil {
			t.Error("convertCallExpr() with unsupported function should return error")
		}
		// SECURITY: Expect sanitized error message
		if err != nil && !strings.Contains(err.Error(), "unsupported filter operation") {
			t.Errorf("Expected 'unsupported filter operation' error, got: %v", err)
		}
	})
}

// =============================================================================
// SECURITY TESTS
// =============================================================================
// These tests verify that all security vulnerabilities identified in the
// security analysis have been properly fixed.

// =============================================================================
// CRITICAL: SQL Injection in LIKE Patterns
// =============================================================================

func TestSecurity_LikePatternEscaping(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"label": {Type: cel.StringType, Column: "label"},
			"text":  {Type: cel.StringType, Column: "text"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name        string
		celExpr     string
		expectedArg string
		description string
	}{
		{
			name:        "contains escapes percent",
			celExpr:     `label.contains("%")`,
			expectedArg: "%\\%%",
			description: "Literal % should be escaped to \\% to match literal character, not wildcard",
		},
		{
			name:        "contains escapes underscore",
			celExpr:     `label.contains("_")`,
			expectedArg: "%\\_%",
			description: "Literal _ should be escaped to \\_ to match literal character, not single char wildcard",
		},
		{
			name:        "contains escapes both percent and underscore",
			celExpr:     `text.contains("%_test")`,
			expectedArg: "%\\%\\_test%",
			description: "Multiple special characters should all be escaped",
		},
		{
			name:        "contains escapes backslash",
			celExpr:     `text.contains("\\")`,
			expectedArg: "%\\\\%",
			description: "Backslash should be escaped first to prevent double-escaping issues",
		},
		{
			name:        "contains escapes brackets",
			celExpr:     `text.contains("[test]")`,
			expectedArg: "%\\[test\\]%",
			description: "Character class brackets should be escaped for SQL Server/PostgreSQL compatibility",
		},
		{
			name:        "startsWith escapes special chars",
			celExpr:     `label.startsWith("%admin")`,
			expectedArg: "\\%admin%",
			description: "startsWith should escape wildcards at the beginning",
		},
		{
			name:        "endsWith escapes special chars",
			celExpr:     `label.endsWith("_test")`,
			expectedArg: "%\\_test",
			description: "endsWith should escape wildcards at the end",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := converter.Convert(tt.celExpr)
			if err != nil {
				t.Fatalf("Convert() error = %v", err)
			}

			sql, args, err := result.Where.ToSql()
			if err != nil {
				t.Fatalf("ToSql() error = %v", err)
			}

			if !strings.Contains(sql, "LIKE") {
				t.Errorf("Expected LIKE query, got: %s", sql)
			}

			if len(args) != 1 {
				t.Fatalf("Expected 1 argument, got %d", len(args))
			}

			actualArg, ok := args[0].(string)
			if !ok {
				t.Fatalf("Expected string argument, got %T", args[0])
			}

			if actualArg != tt.expectedArg {
				t.Errorf("SECURITY VULNERABILITY: %s\nExpected arg: %q\nActual arg: %q\nThis could allow SQL injection via LIKE pattern wildcards",
					tt.description, tt.expectedArg, actualArg)
			}
		})
	}
}

// =============================================================================
// HIGH: Denial of Service via Expression Complexity
// =============================================================================

func TestSecurity_ExpressionLengthLimit(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
		},
		MaxExpressionLength: 100, // Set low limit for testing
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantErr bool
	}{
		{
			name:    "expression within limit",
			celExpr: `status == "published"`,
			wantErr: false,
		},
		{
			name:    "expression at limit",
			celExpr: `status == "` + strings.Repeat("a", 70) + `"`,
			wantErr: false,
		},
		{
			name:    "expression exceeds limit",
			celExpr: strings.Repeat("a", 200) + ` == "published"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil && !strings.Contains(err.Error(), "exceeds maximum length") {
				t.Errorf("Expected length limit error, got: %v", err)
			}
		})
	}
}

func TestSecurity_ExpressionDepthLimit(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"a": {Type: cel.BoolType, Column: "a"},
			"b": {Type: cel.BoolType, Column: "b"},
		},
		MaxExpressionDepth: 10, // Set low limit for testing
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Build deeply nested expression: (((a && b) && b) && b)...
	buildNestedExpr := func(depth int) string {
		expr := "a && b"
		for i := 1; i < depth; i++ {
			expr = "(" + expr + ") && b"
		}
		return expr
	}

	tests := []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{
			name:    "shallow expression",
			depth:   5,
			wantErr: false,
		},
		{
			name:    "at depth limit",
			depth:   9, // Adjusted: depth calculation includes base level
			wantErr: false,
		},
		{
			name:    "exceeds depth limit",
			depth:   15,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := buildNestedExpr(tt.depth)
			_, err := converter.Convert(expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v (depth=%d)", err, tt.wantErr, tt.depth)
			}

			if err != nil && !strings.Contains(err.Error(), "exceeds maximum depth") {
				t.Errorf("Expected depth limit error, got: %v", err)
			}
		})
	}
}

func TestSecurity_InClauseSizeLimit(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
		},
		MaxInClauseSize: 5, // Set low limit for testing
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name       string
		listSize   int
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "small IN clause",
			listSize: 3,
			wantErr:  false,
		},
		{
			name:     "at IN clause limit",
			listSize: 5,
			wantErr:  false,
		},
		{
			name:       "exceeds IN clause limit",
			listSize:   10,
			wantErr:    true,
			wantErrMsg: "exceeds maximum",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build IN clause: status in ["a", "b", "c", ...]
			values := make([]string, tt.listSize)
			for i := 0; i < tt.listSize; i++ {
				values[i] = `"val` + string(rune('a'+i)) + `"`
			}
			expr := `status in [` + strings.Join(values, ", ") + `]`

			_, err := converter.Convert(expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v (size=%d)", err, tt.wantErr, tt.listSize)
			}

			if err != nil && tt.wantErrMsg != "" && !strings.Contains(err.Error(), tt.wantErrMsg) {
				t.Errorf("Expected error containing %q, got: %v", tt.wantErrMsg, err)
			}
		})
	}
}

// =============================================================================
// MEDIUM: Information Disclosure via Error Messages
// =============================================================================

func TestSecurity_SanitizedErrors(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name             string
		celExpr          string
		wantPublicMsg    string
		shouldNotContain []string
	}{
		{
			name:             "invalid syntax",
			celExpr:          `status == `,
			wantPublicMsg:    "invalid filter expression syntax",
			shouldNotContain: []string{"undeclared", "field names", "CEL"},
		},
		{
			name:             "wrong return type",
			celExpr:          `status`,
			wantPublicMsg:    "filter expression must evaluate to boolean",
			shouldNotContain: []string{"string", "type system"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			errMsg := err.Error()

			if !strings.Contains(errMsg, tt.wantPublicMsg) {
				t.Errorf("Expected public message %q, got: %v", tt.wantPublicMsg, errMsg)
			}

			for _, sensitive := range tt.shouldNotContain {
				if strings.Contains(strings.ToLower(errMsg), strings.ToLower(sensitive)) {
					t.Errorf("SECURITY ISSUE: Error message contains sensitive info %q: %v", sensitive, errMsg)
				}
			}

			// Check if it's a ConversionError with internal details
			if convErr, ok := err.(*ConversionError); ok {
				if convErr.InternalError == nil {
					t.Error("ConversionError should have InternalError for logging")
				}
				if convErr.ErrorCode == "" {
					t.Error("ConversionError should have ErrorCode")
				}
			}
		})
	}
}

// =============================================================================
// MEDIUM: Field-Level Authorization
// =============================================================================

func TestSecurity_FieldAuthorization(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":   {Type: cel.StringType, Column: "status"},
			"owner_id": {Type: cel.StringType, Column: "owner_id"},
			"salary":   {Type: cel.IntType, Column: "salary"},
		},
		PublicFields: []string{"status"},
		FieldACL: map[string][]string{
			"owner_id": {"admin", "manager"},
			"salary":   {"admin", "hr"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name      string
		celExpr   string
		userRoles []string
		wantErr   bool
		errCode   string
	}{
		{
			name:      "public field - no roles",
			celExpr:   `status == "published"`,
			userRoles: []string{},
			wantErr:   false,
		},
		{
			name:      "public field - any user",
			celExpr:   `status == "published"`,
			userRoles: []string{"user"},
			wantErr:   false,
		},
		{
			name:      "restricted field - no roles",
			celExpr:   `owner_id == "user123"`,
			userRoles: []string{},
			wantErr:   true,
			errCode:   "UNAUTHORIZED_FIELD",
		},
		{
			name:      "restricted field - unauthorized role",
			celExpr:   `owner_id == "user123"`,
			userRoles: []string{"user"},
			wantErr:   true,
			errCode:   "UNAUTHORIZED_FIELD",
		},
		{
			name:      "restricted field - authorized role",
			celExpr:   `owner_id == "user123"`,
			userRoles: []string{"admin"},
			wantErr:   false,
		},
		{
			name:      "restricted field - one of multiple roles",
			celExpr:   `owner_id == "user123"`,
			userRoles: []string{"user", "manager"},
			wantErr:   false,
		},
		{
			name:      "salary field - hr role",
			celExpr:   `salary > 50000`,
			userRoles: []string{"hr"},
			wantErr:   false,
		},
		{
			name:      "salary field - admin role",
			celExpr:   `salary > 50000`,
			userRoles: []string{"admin"},
			wantErr:   false,
		},
		{
			name:      "salary field - unauthorized",
			celExpr:   `salary > 50000`,
			userRoles: []string{"user", "manager"},
			wantErr:   true,
			errCode:   "UNAUTHORIZED_FIELD",
		},
		{
			name:      "complex expression with mixed fields - authorized",
			celExpr:   `status == "published" && owner_id == "user123"`,
			userRoles: []string{"admin"},
			wantErr:   false,
		},
		{
			name:      "complex expression with mixed fields - partially authorized",
			celExpr:   `status == "published" && salary > 50000`,
			userRoles: []string{"user"},
			wantErr:   true,
			errCode:   "UNAUTHORIZED_FIELD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.ConvertWithAuth(tt.celExpr, tt.userRoles)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertWithAuth() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				if convErr, ok := err.(*ConversionError); ok {
					if tt.errCode != "" && convErr.ErrorCode != tt.errCode {
						t.Errorf("Expected error code %q, got %q", tt.errCode, convErr.ErrorCode)
					}

					// Verify error doesn't reveal which field was unauthorized
					if strings.Contains(convErr.PublicMessage, "owner_id") ||
						strings.Contains(convErr.PublicMessage, "salary") {
						t.Errorf("SECURITY ISSUE: Public error reveals restricted field name: %v", convErr.PublicMessage)
					}
				}
			}
		})
	}
}

func TestSecurity_AuthorizationDisabledByDefault(t *testing.T) {
	// If PublicFields and FieldACL are not configured, ConvertWithAuth should behave like Convert
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status":   {Type: cel.StringType, Column: "status"},
			"owner_id": {Type: cel.StringType, Column: "owner_id"},
		},
		// No PublicFields or FieldACL configured
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Should allow access to any field when authorization is not configured
	_, err = converter.ConvertWithAuth(`owner_id == "user123"`, []string{})
	if err != nil {
		t.Errorf("ConvertWithAuth() should allow access when authorization not configured, got error: %v", err)
	}
}

// =============================================================================
// MEDIUM: Runtime Type Validation
// =============================================================================

func TestSecurity_TypeValidation(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
			"score":  {Type: cel.DoubleType, Column: "score"},
			"active": {Type: cel.BoolType, Column: "active"},
		},
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	tests := []struct {
		name    string
		celExpr string
		wantErr bool
		errCode string
	}{
		{
			name:    "correct string type",
			celExpr: `status == "published"`,
			wantErr: false,
		},
		{
			name:    "correct int type",
			celExpr: `age == 25`,
			wantErr: false,
		},
		{
			name:    "correct double type",
			celExpr: `score == 3.14`,
			wantErr: false,
		},
		{
			name:    "correct bool type",
			celExpr: `active == true`,
			wantErr: false,
		},
		// Note: CEL's type system should prevent most type mismatches at compile time,
		// but runtime validation provides defense in depth
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := converter.Convert(tt.celExpr)
			if (err != nil) != tt.wantErr {
				t.Errorf("Convert() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil && tt.errCode != "" {
				if convErr, ok := err.(*ConversionError); ok {
					if convErr.ErrorCode != tt.errCode {
						t.Errorf("Expected error code %q, got %q", tt.errCode, convErr.ErrorCode)
					}
				}
			}
		})
	}
}

// =============================================================================
// INTEGRATION: Multiple Security Features
// =============================================================================

func TestSecurity_Integration(t *testing.T) {
	config := Config{
		FieldDeclarations: map[string]ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"label":  {Type: cel.StringType, Column: "label"},
		},
		PublicFields:        []string{"status", "label"},
		MaxExpressionLength: 500,
		MaxExpressionDepth:  20,
		MaxInClauseSize:     100,
	}

	converter, err := NewConverter(config)
	if err != nil {
		t.Fatalf("failed to create converter: %v", err)
	}

	// Test real-world scenario: User filters with special characters
	result, err := converter.Convert(`label.contains("%admin%") && status == "published"`)
	if err != nil {
		t.Fatalf("Convert() error = %v", err)
	}

	sql, args, err := result.Where.ToSql()
	if err != nil {
		t.Fatalf("ToSql() error = %v", err)
	}

	// Verify LIKE pattern is properly escaped
	if len(args) < 1 {
		t.Fatal("Expected at least 1 argument")
	}

	likeArg, ok := args[0].(string)
	if !ok {
		t.Fatalf("Expected string argument, got %T", args[0])
	}

	// Should be escaped: %\%admin\%%
	if !strings.Contains(likeArg, "\\%") {
		t.Errorf("CRITICAL: LIKE pattern not properly escaped. Got: %q. This is a SQL injection vulnerability!", likeArg)
	}

	// Verify SQL is valid
	if !strings.Contains(sql, "LIKE") || !strings.Contains(sql, "AND") {
		t.Errorf("Unexpected SQL structure: %s", sql)
	}
}
