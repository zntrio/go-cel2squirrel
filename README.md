# cel2squirrel

[![Go Reference](https://pkg.go.dev/badge/zntr.io/cel2squirrel.svg)](https://pkg.go.dev/zntr.io/cel2squirrel)
[![Go Report Card](https://goreportcard.com/badge/zntr.io/cel2squirrel)](https://goreportcard.com/report/zntr.io/cel2squirrel)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Go package that converts Common Expression Language (CEL) expressions to SQL WHERE clauses using the Squirrel SQL builder.

## Overview

`cel2squirrel` enables filtering using CEL expressions (as specified in [AIP-160](https://google.aip.dev/160)) and converts them to SQL queries that can be executed against a database. It validates that expressions are boolean and provides type-safe conversion to Squirrel SQL builder objects.

## Features

- ✅ **CEL Expression Parsing**: Parse and validate CEL expressions
- ✅ **Boolean Validation**: Ensure expressions return boolean values
- ✅ **Type-Safe Conversion**: Convert CEL AST to Squirrel SQL builder
- ✅ **Field Mapping**: Map CEL field names to SQL column names
- ✅ **Operator Support**:
  - Comparison: `==`, `!=`, `<`, `<=`, `>`, `>=`
  - Logical: `&&` (AND), `||` (OR), `!` (NOT)
  - String: `contains()`, `startsWith()`, `endsWith()`
  - Membership: `in` operator
  - Null: `== null`, `!= null`
- ✅ **PostgreSQL Compatible**: Works with PostgreSQL placeholder format (`$1`, `$2`, etc.)
- 🔒 **Security Hardened**: SQL injection protection, DoS prevention, field-level authorization

## Installation

```bash
go get zntr.io/cel2squirrel
```

## Usage

### Basic Example

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/Masterminds/squirrel"
    "github.com/google/cel-go/cel"
    "zntr.io/cel2squirrel"
)

func main() {
    // Define field declarations (CEL variables and their types)
    config := cel2squirrel.Config{
        FieldDeclarations: map[string]*cel.Type{
            "status": cel.StringType,
            "age":    cel.IntType,
            "rating": cel.DoubleType,
        },
    }
    
    // Create converter
    converter, err := cel2squirrel.NewConverter(config)
    if err != nil {
        log.Fatal(err)
    }
    
    // Convert CEL expression to SQL
    celExpr := `status == "published" && age >= 18`
    result, err := converter.Convert(celExpr, nil)
    if err != nil {
        log.Fatal(err)
    }
    
    // Build complete query with Squirrel
    query := squirrel.Select("*").
        From("users").
        Where(result.Where)
    
    sql, args, _ := query.ToSql()
    fmt.Println(sql)
    // Output: SELECT * FROM users WHERE (status = ? AND age >= ?)
    fmt.Println(args)
    // Output: [published 18]
}
```

### With Field Mappings

Map CEL field names to different SQL column names:

```go
config := cel2squirrel.Config{
    FieldDeclarations: map[string]*cel.Type{
        "isDraft": cel.BoolType,
        "ownerId": cel.StringType,
    },
}

converter, _ := cel2squirrel.NewConverter(config)

// Map camelCase CEL fields to snake_case SQL columns
fieldMappings := map[string]string{
    "isDraft": "is_draft",
    "ownerId": "owner_id",
}

celExpr := `isDraft == false && ownerId == "user123"`
result, _ := converter.Convert(celExpr, fieldMappings)

sql, args, _ := result.Where.ToSql()
// SQL: (is_draft = ? AND owner_id = ?)
// Args: [false user123]
```

### PostgreSQL Placeholders

Use PostgreSQL-style numbered placeholders:

```go
query := squirrel.Select("*").
    From("users").
    Where(result.Where).
    PlaceholderFormat(squirrel.Dollar)

sql, args, _ := query.ToSql()
// SQL: SELECT * FROM users WHERE (status = $1 AND age >= $2)
```

### Complex Expressions

Handle complex nested boolean expressions:

```go
celExpr := `(status == "published" || status == "featured") && age >= 18 && rating > 4.0`
result, _ := converter.Convert(celExpr, nil)

query := squirrel.Select("id", "name", "rating").
    From("users").
    Where(result.Where).
    OrderBy("rating DESC").
    Limit(10)

sql, args, _ := query.ToSql()
// SQL: SELECT id, name, rating FROM users 
//      WHERE ((status = ? OR status = ?) AND age >= ? AND rating > ?) 
//      ORDER BY rating DESC LIMIT 10
// Args: [published featured 18 4.0]
```

### String Operations

Use CEL string methods:

```go
// Contains
celExpr := `name.contains("john")`
// SQL: name LIKE ?
// Args: [%john%]

// Starts with
celExpr := `name.startsWith("Dr. ")`
// SQL: name LIKE ?
// Args: [Dr. %]

// Ends with
celExpr := `email.endsWith("@example.com")`
// SQL: email LIKE ?
// Args: [%@example.com]
```

### Null Comparisons

Handle NULL values:

```go
// IS NULL
celExpr := `deletedAt == null`
// SQL: deletedAt IS NULL

// IS NOT NULL
celExpr := `deletedAt != null`
// SQL: deletedAt IS NOT NULL
```

### IN Operator

Filter with multiple values:

```go
celExpr := `status in ["published", "featured", "archived"]`
result, _ := converter.Convert(celExpr, nil)

sql, args, _ := result.Where.ToSql()
// SQL: status IN (?,?,?)
// Args: [published featured archived]
```

## Real-World Example

Example implementation of a database repository with CEL filtering (AIP-160 compliant):

```go
package repository

import (
    "context"
    "fmt"
    
    "github.com/Masterminds/squirrel"
    "github.com/google/cel-go/cel"
    "zntr.io/cel2squirrel"
)

type UserRepository struct {
    db        *sql.DB
    converter *cel2squirrel.Converter
}

func NewUserRepository(db *sql.DB) (*UserRepository, error) {
    // Setup CEL converter
    config := cel2squirrel.Config{
        FieldDeclarations: map[string]*cel.Type{
            "status":     cel.StringType,
            "isActive":   cel.BoolType,
            "rating":     cel.DoubleType,
            "createTime": cel.TimestampType,
        },
    }
    
    converter, err := cel2squirrel.NewConverter(config)
    if err != nil {
        return nil, fmt.Errorf("failed to create CEL converter: %w", err)
    }
    
    return &UserRepository{
        db:        db,
        converter: converter,
    }, nil
}

func (r *UserRepository) Search(ctx context.Context, filter string, pageSize int) ([]User, error) {
    // Map CEL fields to SQL columns
    fieldMappings := map[string]string{
        "isActive":   "is_active",
        "createTime": "create_time",
    }
    
    // Convert CEL filter to SQL
    result, err := r.converter.Convert(filter, fieldMappings)
    if err != nil {
        return nil, fmt.Errorf("invalid filter expression: %w", err)
    }
    
    // Build query with Squirrel
    query := squirrel.Select("*").
        From("users").
        Where(result.Where).
        PlaceholderFormat(squirrel.Dollar).
        Limit(uint64(pageSize))
    
    sql, args, err := query.ToSql()
    if err != nil {
        return nil, err
    }
    
    // Execute query
    rows, err := r.db.QueryContext(ctx, sql, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    // Scan results...
    var users []User
    for rows.Next() {
        var u User
        // Scan into user struct...
        users = append(users, u)
    }
    
    return users, rows.Err()
}
```

### API Usage Example

```http
GET /v1/users?filter=status == "active" && rating > 4.0&page_size=20
```

## Supported CEL Operations

### Comparison Operators

| CEL Operator | SQL Equivalent | Example |
|--------------|----------------|---------|
| `==` | `=` | `status == "published"` |
| `!=` | `<>` or `IS NOT` | `status != "draft"` |
| `<` | `<` | `age < 18` |
| `<=` | `<=` | `age <= 21` |
| `>` | `>` | `age > 65` |
| `>=` | `>=` | `rating >= 4.5` |

### Logical Operators

| CEL Operator | SQL Equivalent | Example |
|--------------|----------------|---------|
| `&&` | `AND` | `status == "published" && age >= 18` |
| `\|\|` | `OR` | `status == "draft" \|\| status == "published"` |
| `!` | `NOT` | `!(isDraft)` |

### String Operations

| CEL Function | SQL Equivalent | Example |
|--------------|----------------|---------|
| `contains(x)` | `LIKE '%x%'` | `label.contains("test")` |
| `startsWith(x)` | `LIKE 'x%'` | `label.startsWith("prod")` |
| `endsWith(x)` | `LIKE '%x'` | `label.endsWith("v2")` |

### Membership Operators

| CEL Operator | SQL Equivalent | Example |
|--------------|----------------|---------|
| `in` | `IN (...)` | `status in ["published", "featured"]` |

### Null Comparisons

| CEL Expression | SQL Equivalent | Example |
|----------------|----------------|---------|
| `field == null` | `IS NULL` | `deletedAt == null` |
| `field != null` | `IS NOT NULL` | `deletedAt != null` |

## Error Handling

The converter validates expressions and returns descriptive errors:

```go
// Non-boolean expression
celExpr := `age + 5`  // Returns: "CEL expression must return a boolean, got int"

// Undefined field
celExpr := `unknownField == "value"`  // Returns: "failed to compile CEL expression: undeclared reference..."

// Syntax error
celExpr := `status == `  // Returns: "failed to compile CEL expression: Syntax error..."
```

## Type Declarations

Use CEL types directly to define field types:

```go
import "github.com/google/cel-go/cel"

FieldDeclarations: map[string]*cel.Type{
    "stringField":    cel.StringType,
    "intField":       cel.IntType,
    "doubleField":    cel.DoubleType,
    "boolField":      cel.BoolType,
    "uintField":      cel.UintType,
    "timestampField": cel.TimestampType,
    "durationField":  cel.DurationType,
    "bytesField":     cel.BytesType,
    
    // Complex types
    "stringList":     cel.ListType(cel.StringType),
    "stringMap":      cel.MapType(cel.StringType, cel.StringType),
}
```

## Limitations

- **No Function Calls**: Custom CEL functions are not supported (only built-in string methods)
- **Simple Expressions**: Complex nested member access is limited
- **List Literals Only**: The `in` operator requires constant list literals
- **No Arithmetic in Filters**: Expressions like `age + 5 > 30` are not supported

## Performance Considerations

- **Prepared Statements**: The generated SQL is parameterized and suitable for prepared statements
- **Validation**: CEL expressions are validated before conversion, preventing SQL injection
- **Caching**: Consider caching converter instances for frequently used field declarations

## Security

### Overview

The cel2squirrel package implements multiple layers of security to protect against common vulnerabilities when converting user-supplied filter expressions to SQL queries. All security features are enabled by default with secure defaults.

### SQL Injection Protection

**LIKE Pattern Escaping**: The package automatically escapes SQL special characters (`%`, `_`, `\`, `[`, `]`) in string operations to prevent LIKE pattern injection:

```go
// User input: name.contains("%")
// Unsafe: LIKE '%%%'  (matches all records!)
// Safe:   LIKE '%\%%' (matches literal % character)

celExpr := `name.contains("%admin")`
result, _ := converter.Convert(celExpr)
// Generates: name LIKE ?
// Args: [%\%admin%]  // Special chars properly escaped
```

**Parameterized Queries**: All values are passed as query parameters, never concatenated into SQL strings:

```go
celExpr := `status == "published" && age > 18`
// Generates parameterized SQL: status = ? AND age > ?
// Args: ["published", 18]
```

### Denial of Service Prevention

Configure expression complexity limits to prevent resource exhaustion:

```go
config := cel2squirrel.Config{
    FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
        "status": {Type: cel.StringType, Column: "status"},
    },

    // Security limits (default values shown)
    MaxExpressionLength: 10000,  // Max 10KB expression
    MaxExpressionDepth:  50,     // Max 50 levels of nesting
    MaxInClauseSize:     1000,   // Max 1000 values in IN clause
}

converter, _ := cel2squirrel.NewConverter(config)

// These will be rejected:
_, err := converter.Convert(strings.Repeat("a", 20000))  // Too long
_, err := converter.Convert(deeplyNestedExpression)       // Too deep
_, err := converter.Convert(`status in [...]`)            // Too many values
```

### Field-Level Authorization

Restrict which fields users can filter by based on their roles:

```go
config := cel2squirrel.Config{
    FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
        "status":   {Type: cel.StringType, Column: "status"},
        "owner_id": {Type: cel.StringType, Column: "owner_id"},
        "salary":   {Type: cel.IntType, Column: "salary"},
    },

    // Public fields accessible to all users
    PublicFields: []string{"status"},

    // Role-based access control
    FieldACL: map[string][]string{
        "owner_id": {"admin", "manager"},
        "salary":   {"admin", "hr"},
    },
}

converter, _ := cel2squirrel.NewConverter(config)

// Check authorization before conversion
userRoles := []string{"user"}
_, err := converter.ConvertWithAuth(`owner_id == "user123"`, userRoles)
// Returns: "access denied: insufficient permissions"

adminRoles := []string{"admin"}
result, _ := converter.ConvertWithAuth(`owner_id == "user123"`, adminRoles)
// Success: admin can filter by owner_id
```

### Error Message Sanitization

The package sanitizes error messages to prevent information disclosure:

```go
// Internal error with details (logged server-side)
convErr := err.(*cel2squirrel.ConversionError)
log.Printf("Conversion failed: %v", convErr.InternalError)
// Logs: "undeclared reference to 'secretField'"

// Public error message (returned to user)
fmt.Println(convErr.Error())
// Returns: "invalid filter expression syntax"
// Does NOT reveal field names or internal structure
```

### Runtime Type Validation

Defense-in-depth type checking validates that values match declared field types:

```go
config := cel2squirrel.Config{
    FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
        "age": {Type: cel.IntType, Column: "age"},
    },
}

converter, _ := cel2squirrel.NewConverter(config)

// CEL's type system catches this at compile time
_, err := converter.Convert(`age == "not a number"`)
// Returns type mismatch error

// Runtime validation provides additional protection
```

### Secure Configuration Example

A production-ready secure configuration:

```go
func NewSecureConverter() (*cel2squirrel.Converter, error) {
    config := cel2squirrel.DefaultConfig() // Start with secure defaults

    config.FieldDeclarations = map[string]cel2squirrel.ColumnMapping{
        "status":     {Type: cel.StringType, Column: "status"},
        "created_at": {Type: cel.TimestampType, Column: "created_at"},
        "owner_id":   {Type: cel.StringType, Column: "owner_id"},
        "is_private": {Type: cel.BoolType, Column: "is_private"},
    }

    // Define public fields
    config.PublicFields = []string{"status", "created_at"}

    // Restrict sensitive fields
    config.FieldACL = map[string][]string{
        "owner_id":   {"admin", "manager"},
        "is_private": {"admin"},
    }

    // Adjust limits for your use case
    config.MaxExpressionLength = 1000  // Shorter limit for API
    config.MaxExpressionDepth = 10     // Prevent deeply nested attacks
    config.MaxInClauseSize = 100       // Reasonable batch size

    return cel2squirrel.NewConverter(config)
}

// Usage in HTTP handler
func (h *Handler) SearchRecords(w http.ResponseWriter, r *http.Request) {
    filter := r.URL.Query().Get("filter")

    // Get user roles from authentication context
    userRoles := h.getUserRoles(r.Context())

    // Convert with authorization check
    result, err := h.converter.ConvertWithAuth(filter, userRoles)
    if err != nil {
        if convErr, ok := err.(*cel2squirrel.ConversionError); ok {
            // Return sanitized error to user
            http.Error(w, convErr.PublicMessage, http.StatusBadRequest)

            // Log detailed error server-side
            log.Printf("Filter conversion failed: %v (code=%s)",
                convErr.InternalError, convErr.ErrorCode)
            return
        }
        http.Error(w, "Invalid filter", http.StatusBadRequest)
        return
    }

    // Build and execute query...
}
```

### Security Best Practices

1. **Always use field-level authorization** for multi-tenant or multi-user systems
2. **Log security events** to detect attack patterns
3. **Set conservative limits** for expression complexity based on your use case
4. **Never expose internal errors** to end users
5. **Use ConvertWithAuth()** instead of Convert() when authorization is configured
6. **Validate user roles** from trusted authentication context, never from user input
7. **Monitor for unusual patterns** like deeply nested expressions or large IN clauses
8. **Test security controls** regularly with security-focused test cases
9. **Keep dependencies updated** (CEL, Squirrel) for security patches

## Testing

Run tests:

```bash
go test -v ./...
```

Run with coverage:

```bash
go test -cover ./...
```

Run fuzz tests:

```bash
go test -fuzz=Fuzz -fuzztime=30s
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

### Development Setup

1. Clone the repository
2. Install dependencies: `go mod download`
3. Run tests: `go test ./...`
4. Run linter: `go vet ./...`

### Guidelines

- Write tests for new features
- Follow Go best practices and idioms
- Update documentation for API changes
- Ensure all tests pass before submitting PR

## Use Cases

This library is ideal for:

- **REST APIs**: Implement AIP-160 compliant filtering in your API endpoints
- **Multi-tenant Applications**: Safe, field-level authorization for database queries
- **Admin Dashboards**: Flexible search and filtering capabilities
- **Data Export Tools**: User-defined filtering without SQL injection risks
- **GraphQL Backends**: Convert filter arguments to SQL queries

## References

- [Google AIP-160: Filtering](https://google.aip.dev/160)
- [CEL Specification](https://github.com/google/cel-spec)
- [CEL Go Library](https://github.com/google/cel-go)
- [Squirrel SQL Builder](https://github.com/Masterminds/squirrel)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

- Report issues on [GitHub Issues](https://github.com/zntr/go-cel2squirrel/issues)
- For questions and discussions, use [GitHub Discussions](https://github.com/zntr/go-cel2squirrel/discussions)

## Acknowledgments

Built with:
- [google/cel-go](https://github.com/google/cel-go) - Common Expression Language implementation
- [Masterminds/squirrel](https://github.com/Masterminds/squirrel) - SQL query builder

