// Package cel2squirrel provides a converter for transforming Common Expression Language (CEL)
// expressions into SQL WHERE clauses using the Squirrel SQL builder.
package cel2squirrel

import (
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

// SecurityLogger is an interface for logging security-relevant events.
// Implementations should log these events to a security audit log for monitoring.
type SecurityLogger interface {
	// LogConversionAttempt logs an attempt to convert a CEL expression to SQL.
	LogConversionAttempt(expr string, success bool, err error, duration time.Duration)

	// LogComplexExpression logs when an expression is unusually complex.
	LogComplexExpression(expr string, depth int, length int)

	// LogUnauthorizedField logs when a user attempts to access a restricted field.
	LogUnauthorizedField(expr string, field string, userRoles []string)

	// LogUnsupportedOperation logs when an unsupported CEL function is used.
	LogUnsupportedOperation(expr string, operation string)
}

// Converter converts CEL expressions to Squirrel SQL builder objects.
type Converter struct {
	env                 *cel.Env
	columnMappings      map[string]string
	fieldDeclarations   map[string]ColumnMapping
	maxExpressionLength int
	maxExpressionDepth  int
	maxInClauseSize     int
	publicFields        map[string]bool
	fieldACL            map[string][]string
	securityLogger      SecurityLogger
}

// Config contains configuration for the CEL to SQL converter.
type Config struct {
	// FieldDeclarations maps CEL variable names to their types and SQL columns.
	// Example: map[string]ColumnMapping{
	//   "age": {Type: cel.IntType, Column: "user_age"},
	//   "name": {Type: cel.StringType, Column: "user_name"},
	//   "tags": {Type: cel.ListType(cel.StringType), Column: "user_tags"},
	// }
	FieldDeclarations map[string]ColumnMapping

	// Security limits to prevent DoS attacks
	// MaxExpressionLength is the maximum allowed length of a CEL expression in characters.
	// Default: 10000. Set to 0 to apply default.
	MaxExpressionLength int

	// MaxExpressionDepth is the maximum nesting depth of boolean operators.
	// Default: 50. Set to 0 to apply default.
	MaxExpressionDepth int

	// MaxInClauseSize is the maximum number of values allowed in an IN clause.
	// Default: 1000. Set to 0 to apply default.
	MaxInClauseSize int

	// Authorization settings for field-level access control
	// PublicFields is a list of field names that any user can filter by.
	// If empty, authorization checks are disabled.
	PublicFields []string

	// FieldACL maps field names to lists of roles that can access them.
	// Only checked if PublicFields is not empty.
	FieldACL map[string][]string
}

// ColumnMapping is a mapping of a CEL field name to a SQL column name.
type ColumnMapping struct {
	// Type is the type of the CEL field.
	Type *cel.Type
	// Column is the name of the SQL column.
	Column string
}

// DefaultConfig returns a Config with secure default values.
func DefaultConfig() Config {
	return Config{
		FieldDeclarations:   make(map[string]ColumnMapping),
		MaxExpressionLength: 10000, // 10KB max expression
		MaxExpressionDepth:  50,    // Max 50 levels of nesting
		MaxInClauseSize:     1000,  // Max 1000 values in IN clause
	}
}

// NewConverter creates a new CEL to SQL converter with the given configuration.
func NewConverter(config Config) (*Converter, error) {
	// Apply secure defaults for zero values
	if config.MaxExpressionLength == 0 {
		config.MaxExpressionLength = 10000
	}
	if config.MaxExpressionDepth == 0 {
		config.MaxExpressionDepth = 50
	}
	if config.MaxInClauseSize == 0 {
		config.MaxInClauseSize = 1000
	}

	// Build CEL environment with field declarations
	var opts []cel.EnvOption
	columnMappings := make(map[string]string)

	// Add field declarations
	if config.FieldDeclarations != nil {
		for name, mapping := range config.FieldDeclarations {
			if mapping.Type != nil {
				opts = append(opts, cel.Variable(name, mapping.Type))
			}
			// Store column mapping (use column name if specified, otherwise use field name)
			if mapping.Column != "" {
				columnMappings[name] = mapping.Column
			} else {
				columnMappings[name] = name
			}
		}
	}

	env, err := cel.NewEnv(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}

	// Build public fields map for O(1) lookup
	publicFields := make(map[string]bool)
	for _, field := range config.PublicFields {
		publicFields[field] = true
	}

	return &Converter{
		env:                 env,
		columnMappings:      columnMappings,
		fieldDeclarations:   config.FieldDeclarations,
		maxExpressionLength: config.MaxExpressionLength,
		maxExpressionDepth:  config.MaxExpressionDepth,
		maxInClauseSize:     config.MaxInClauseSize,
		publicFields:        publicFields,
		fieldACL:            config.FieldACL,
	}, nil
}

// ConvertResult contains the result of converting a CEL expression to SQL.
type ConvertResult struct {
	// Where is the Squirrel Sqlizer that can be used in WHERE clauses
	Where squirrel.Sqlizer

	// Args contains any arguments that need to be bound to the query
	Args []interface{}
}

// ConversionError represents an error that occurred during CEL to SQL conversion.
// It provides both a user-safe public message and detailed internal error for logging.
type ConversionError struct {
	// PublicMessage is a sanitized error message safe to return to end users.
	PublicMessage string
	// InternalError contains the detailed error for internal logging.
	InternalError error
	// ErrorCode is a machine-readable error code.
	ErrorCode string
}

// Error implements the error interface, returning the public message.
func (e *ConversionError) Error() string {
	return e.PublicMessage
}

// Unwrap returns the internal error for error chain unwrapping.
func (e *ConversionError) Unwrap() error {
	return e.InternalError
}

// newConversionError creates a ConversionError with a sanitized public message.
func newConversionError(publicMsg, errorCode string, internalErr error) error {
	return &ConversionError{
		PublicMessage: publicMsg,
		ErrorCode:     errorCode,
		InternalError: internalErr,
	}
}

// Convert parses a CEL expression and converts it to a Squirrel SQL builder object.
// It validates that the expression is boolean and returns a Sqlizer that can be used
// in WHERE clauses. Column mappings are automatically applied based on the converter's
// configuration.
func (c *Converter) Convert(celExpr string) (*ConvertResult, error) {
	var convErr error

	// SECURITY: Validate expression length immediately
	if len(celExpr) > c.maxExpressionLength {
		convErr = fmt.Errorf("expression exceeds maximum length of %d characters (got %d)",
			c.maxExpressionLength, len(celExpr))
		return nil, convErr
	}

	// Parse the CEL expression
	compiled, issues := c.env.Compile(celExpr)
	if issues != nil && issues.Err() != nil {
		// SECURITY: Sanitize error - don't expose field names or internal details
		convErr = newConversionError(
			"invalid filter expression syntax",
			"INVALID_SYNTAX",
			fmt.Errorf("CEL compilation failed: %w", issues.Err()),
		)
		return nil, convErr
	}

	// Validate that the expression returns a boolean
	if compiled.OutputType() != cel.BoolType {
		// SECURITY: Sanitize error - don't expose type system details
		convErr = newConversionError(
			"filter expression must evaluate to boolean",
			"INVALID_TYPE",
			fmt.Errorf("expected boolean, got %v", compiled.OutputType()),
		)
		return nil, convErr
	}

	// Convert AST to checked expression to get the protobuf representation
	// Note: We use protobuf types internally for navigation, but they're not exposed in the public API
	checkedExpr, err := cel.AstToCheckedExpr(compiled)
	if err != nil {
		convErr = fmt.Errorf("failed to convert AST to checked expression: %w", err)
		return nil, convErr
	}

	// SECURITY: Validate expression complexity (depth)
	depth := c.calculateExpressionDepth(checkedExpr.GetExpr())
	if depth > c.maxExpressionDepth {
		convErr = fmt.Errorf("expression exceeds maximum depth of %d (got %d)",
			c.maxExpressionDepth, depth)
		return nil, convErr
	}

	// SECURITY: Log if expression is unusually complex
	if c.securityLogger != nil && (depth > c.maxExpressionDepth/2 || len(celExpr) > c.maxExpressionLength/2) {
		c.securityLogger.LogComplexExpression(
			celExpr,
			depth,
			len(celExpr),
		)
	}

	sqlizer, err := c.convertExpr(checkedExpr.GetExpr())
	if err != nil {
		convErr = fmt.Errorf("failed to convert CEL to SQL: %w", err)
		return nil, convErr
	}

	return &ConvertResult{
		Where: sqlizer,
		Args:  []interface{}{},
	}, nil
}

// ConvertWithAuth converts a CEL expression to SQL with field-level authorization.
// It checks that the user (identified by their roles) is authorized to filter by
// all fields referenced in the expression. If authorization is not configured
// (PublicFields is empty), this behaves the same as Convert().
func (c *Converter) ConvertWithAuth(celExpr string, userRoles []string) (*ConvertResult, error) {
	// If authorization is not configured, use standard Convert
	if len(c.publicFields) == 0 && len(c.fieldACL) == 0 {
		return c.Convert(celExpr)
	}

	// First validate expression length
	if len(celExpr) > c.maxExpressionLength {
		return nil, fmt.Errorf("expression exceeds maximum length of %d characters (got %d)",
			c.maxExpressionLength, len(celExpr))
	}

	// Parse the CEL expression
	compiled, issues := c.env.Compile(celExpr)
	if issues != nil && issues.Err() != nil {
		return nil, newConversionError(
			"invalid filter expression syntax",
			"INVALID_SYNTAX",
			fmt.Errorf("CEL compilation failed: %w", issues.Err()),
		)
	}

	// Validate that the expression returns a boolean
	if compiled.OutputType() != cel.BoolType {
		return nil, newConversionError(
			"filter expression must evaluate to boolean",
			"INVALID_TYPE",
			fmt.Errorf("expected boolean, got %v", compiled.OutputType()),
		)
	}

	// Convert AST to checked expression
	checkedExpr, err := cel.AstToCheckedExpr(compiled)
	if err != nil {
		return nil, fmt.Errorf("failed to convert AST to checked expression: %w", err)
	}

	// SECURITY: Extract referenced fields and check authorization
	referencedFields := c.extractReferencedFields(checkedExpr.GetExpr())
	for _, field := range referencedFields {
		if !c.isFieldAuthorized(field, userRoles) {
			// SECURITY: Log unauthorized access attempt
			if c.securityLogger != nil {
				c.securityLogger.LogUnauthorizedField(
					celExpr,
					field,
					userRoles,
				)
			}

			// SECURITY: Don't reveal which field was unauthorized
			return nil, newConversionError(
				"access denied: insufficient permissions for requested filter",
				"UNAUTHORIZED_FIELD",
				fmt.Errorf("user with roles %v attempted to filter by restricted field: %s",
					userRoles, field),
			)
		}
	}

	// Validate expression complexity (depth)
	depth := c.calculateExpressionDepth(checkedExpr.GetExpr())
	if depth > c.maxExpressionDepth {
		return nil, fmt.Errorf("expression exceeds maximum depth of %d (got %d)",
			c.maxExpressionDepth, depth)
	}

	// Convert to SQL
	sqlizer, err := c.convertExpr(checkedExpr.GetExpr())
	if err != nil {
		return nil, fmt.Errorf("failed to convert CEL to SQL: %w", err)
	}

	return &ConvertResult{
		Where: sqlizer,
		Args:  []interface{}{},
	}, nil
}

// extractReferencedFields recursively extracts all field names referenced in an expression.
func (c *Converter) extractReferencedFields(expr *exprpb.Expr) []string {
	fields := make(map[string]bool)
	c.walkExpr(expr, func(e *exprpb.Expr) {
		if ident := e.GetIdentExpr(); ident != nil {
			fields[ident.Name] = true
		}
		if sel := e.GetSelectExpr(); sel != nil {
			fields[sel.Field] = true
		}
	})

	result := make([]string, 0, len(fields))
	for field := range fields {
		result = append(result, field)
	}
	return result
}

// walkExpr recursively visits all expressions in the tree.
func (c *Converter) walkExpr(expr *exprpb.Expr, fn func(*exprpb.Expr)) {
	if expr == nil {
		return
	}

	fn(expr)

	switch e := expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		if e.CallExpr.Target != nil {
			c.walkExpr(e.CallExpr.Target, fn)
		}
		for _, arg := range e.CallExpr.Args {
			c.walkExpr(arg, fn)
		}
	case *exprpb.Expr_SelectExpr:
		c.walkExpr(e.SelectExpr.Operand, fn)
	case *exprpb.Expr_ListExpr:
		for _, elem := range e.ListExpr.Elements {
			c.walkExpr(elem, fn)
		}
	case *exprpb.Expr_StructExpr:
		for _, entry := range e.StructExpr.Entries {
			c.walkExpr(entry.GetMapKey(), fn)
			c.walkExpr(entry.Value, fn)
		}
	}
}

// isFieldAuthorized checks if a field can be accessed by the given user roles.
func (c *Converter) isFieldAuthorized(field string, userRoles []string) bool {
	// Check if field is public (no authorization required)
	if c.publicFields[field] {
		return true
	}

	// Check role-based ACL
	if allowedRoles, exists := c.fieldACL[field]; exists {
		for _, userRole := range userRoles {
			for _, allowedRole := range allowedRoles {
				if userRole == allowedRole {
					return true
				}
			}
		}
	}

	// Field is restricted and user doesn't have required role
	return false
}

// calculateExpressionDepth recursively calculates the maximum nesting depth of an expression.
func (c *Converter) calculateExpressionDepth(expr *exprpb.Expr) int {
	if expr == nil {
		return 0
	}

	switch e := expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		maxArgDepth := 0
		// Check target (for method calls)
		if e.CallExpr.Target != nil {
			targetDepth := c.calculateExpressionDepth(e.CallExpr.Target)
			if targetDepth > maxArgDepth {
				maxArgDepth = targetDepth
			}
		}
		// Check all arguments
		for _, arg := range e.CallExpr.Args {
			argDepth := c.calculateExpressionDepth(arg)
			if argDepth > maxArgDepth {
				maxArgDepth = argDepth
			}
		}
		return maxArgDepth + 1

	case *exprpb.Expr_SelectExpr:
		return c.calculateExpressionDepth(e.SelectExpr.Operand) + 1

	case *exprpb.Expr_ListExpr:
		maxElemDepth := 0
		for _, elem := range e.ListExpr.Elements {
			elemDepth := c.calculateExpressionDepth(elem)
			if elemDepth > maxElemDepth {
				maxElemDepth = elemDepth
			}
		}
		return maxElemDepth + 1

	case *exprpb.Expr_StructExpr:
		maxEntryDepth := 0
		for _, entry := range e.StructExpr.Entries {
			keyDepth := c.calculateExpressionDepth(entry.GetMapKey())
			valueDepth := c.calculateExpressionDepth(entry.Value)
			if keyDepth > maxEntryDepth {
				maxEntryDepth = keyDepth
			}
			if valueDepth > maxEntryDepth {
				maxEntryDepth = valueDepth
			}
		}
		return maxEntryDepth + 1

	default:
		// Leaf nodes (constants, identifiers)
		return 1
	}
}

// convertExpr converts a CEL expression to a Squirrel Sqlizer.
func (c *Converter) convertExpr(expr *exprpb.Expr) (squirrel.Sqlizer, error) {
	if expr == nil {
		return nil, fmt.Errorf("nil expression")
	}

	switch expr.ExprKind.(type) {
	case *exprpb.Expr_CallExpr:
		callExpr := expr.GetCallExpr()
		if callExpr == nil {
			return nil, fmt.Errorf("nil call expression")
		}
		return c.convertCallExpr(callExpr)
	case *exprpb.Expr_IdentExpr:
		// Standalone identifier (e.g., "is_published")
		ident := expr.GetIdentExpr()
		if ident == nil {
			return nil, fmt.Errorf("nil identifier expression")
		}
		column := c.mapFieldName(ident.Name)
		return squirrel.Eq{column: true}, nil
	case *exprpb.Expr_ConstExpr:
		// Constant value
		constExpr := expr.GetConstExpr()
		if constExpr == nil {
			return nil, fmt.Errorf("nil constant expression")
		}
		return c.convertConstExpr(constExpr)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr.ExprKind)
	}
}

// convertCallExpr converts a CEL call expression to a Squirrel Sqlizer.
func (c *Converter) convertCallExpr(call *exprpb.Expr_Call) (squirrel.Sqlizer, error) {
	if call == nil {
		return nil, fmt.Errorf("nil call expression")
	}

	function := call.Function

	switch function {
	case "_&&_": // Logical AND
		return c.convertLogicalAnd(call.Args)
	case "_||_": // Logical OR
		return c.convertLogicalOr(call.Args)
	case "!_": // Logical NOT
		return c.convertLogicalNot(call.Args)
	case "_==_": // Equality
		return c.convertComparison(call.Args, "=")
	case "_!=_": // Inequality
		return c.convertComparison(call.Args, "!=")
	case "_<_": // Less than
		return c.convertComparison(call.Args, "<")
	case "_<=_": // Less than or equal
		return c.convertComparison(call.Args, "<=")
	case "_>_": // Greater than
		return c.convertComparison(call.Args, ">")
	case "_>=_": // Greater than or equal
		return c.convertComparison(call.Args, ">=")
	case "@in": // IN operator
		return c.convertInOperator(call.Args)
	case "contains": // String contains
		return c.convertContains(call)
	case "startsWith": // String starts with
		return c.convertStartsWith(call)
	case "endsWith": // String ends with
		return c.convertEndsWith(call)
	default:
		// SECURITY: Log unsupported operation attempt
		if c.securityLogger != nil {
			c.securityLogger.LogUnsupportedOperation(
				call.String(),
				function,
			)
		}

		// SECURITY: Sanitize error - don't expose supported function list
		return nil, newConversionError(
			"unsupported filter operation",
			"UNSUPPORTED_OPERATION",
			fmt.Errorf("unsupported CEL function: %s", function),
		)
	}
}

// convertLogicalAnd converts CEL AND operator to Squirrel And.
func (c *Converter) convertLogicalAnd(args []*exprpb.Expr) (squirrel.Sqlizer, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("AND operator requires exactly 2 arguments, got %d", len(args))
	}

	left, err := c.convertExpr(args[0])
	if err != nil {
		return nil, err
	}

	right, err := c.convertExpr(args[1])
	if err != nil {
		return nil, err
	}

	return squirrel.And{left, right}, nil
}

// convertLogicalOr converts CEL OR operator to Squirrel Or.
func (c *Converter) convertLogicalOr(args []*exprpb.Expr) (squirrel.Sqlizer, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("OR operator requires exactly 2 arguments, got %d", len(args))
	}

	left, err := c.convertExpr(args[0])
	if err != nil {
		return nil, err
	}

	right, err := c.convertExpr(args[1])
	if err != nil {
		return nil, err
	}

	return squirrel.Or{left, right}, nil
}

// convertLogicalNot converts CEL NOT operator to SQL NOT.
func (c *Converter) convertLogicalNot(args []*exprpb.Expr) (squirrel.Sqlizer, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("NOT operator requires exactly 1 argument, got %d", len(args))
	}

	inner, err := c.convertExpr(args[0])
	if err != nil {
		return nil, err
	}

	// Squirrel doesn't have a direct NOT, so we use NotEq for simple cases
	// For complex expressions, we wrap in a custom Sqlizer
	return &notSqlizer{inner: inner}, nil
}

// convertComparison converts CEL comparison operators to Squirrel comparison.
func (c *Converter) convertComparison(args []*exprpb.Expr, op string) (squirrel.Sqlizer, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("comparison operator requires exactly 2 arguments, got %d", len(args))
	}

	// Get the field name (left side)
	field, err := c.getFieldName(args[0])
	if err != nil {
		return nil, err
	}
	column := c.mapFieldName(field)

	// Get the value (right side)
	value, err := c.getConstantValue(args[1])
	if err != nil {
		return nil, err
	}

	// SECURITY: Validate type compatibility at runtime
	if value != nil {
		if err := c.validateTypeCompatibility(field, value); err != nil {
			return nil, newConversionError(
				"invalid comparison type",
				"TYPE_MISMATCH",
				fmt.Errorf("type mismatch for field %s: %w", field, err),
			)
		}
	}

	// Handle NULL comparisons
	if value == nil {
		switch op {
		case "=", "==":
			return squirrel.Eq{column: nil}, nil
		case "!=":
			return squirrel.NotEq{column: nil}, nil
		}
	}

	// Convert to appropriate Squirrel type
	switch op {
	case "=", "==":
		return squirrel.Eq{column: value}, nil
	case "!=":
		return squirrel.NotEq{column: value}, nil
	case "<":
		return squirrel.Lt{column: value}, nil
	case "<=":
		return squirrel.LtOrEq{column: value}, nil
	case ">":
		return squirrel.Gt{column: value}, nil
	case ">=":
		return squirrel.GtOrEq{column: value}, nil
	default:
		return nil, fmt.Errorf("unsupported comparison operator: %s", op)
	}
}

// validateTypeCompatibility checks if a value is compatible with a field's declared type.
func (c *Converter) validateTypeCompatibility(fieldName string, value interface{}) error {
	// Get the declared type for this field
	mapping, exists := c.fieldDeclarations[fieldName]
	if !exists || mapping.Type == nil {
		// No type declaration found, skip validation
		return nil
	}

	fieldType := mapping.Type.String()

	// Validate based on declared type
	switch fieldType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "int":
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("expected int, got %T", value)
		}
	case "double":
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("expected double, got %T", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case "uint":
		if _, ok := value.(uint64); !ok {
			return fmt.Errorf("expected uint, got %T", value)
		}
	// Add more type checks as needed
	default:
		// For complex types (lists, maps, etc.), rely on CEL's type checking
		return nil
	}

	return nil
}

// convertInOperator converts CEL IN operator to Squirrel Eq with array.
func (c *Converter) convertInOperator(args []*exprpb.Expr) (squirrel.Sqlizer, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("IN operator requires exactly 2 arguments, got %d", len(args))
	}

	// Get the field name (left side)
	field, err := c.getFieldName(args[0])
	if err != nil {
		return nil, err
	}
	column := c.mapFieldName(field)

	// Get the list (right side)
	list, err := c.getListValues(args[1])
	if err != nil {
		return nil, err
	}

	return squirrel.Eq{column: list}, nil
}

// escapeLikePattern escapes SQL LIKE special characters to prevent injection.
// Escapes: % (any chars), _ (single char), \ (escape char), [ and ] (character class)
func escapeLikePattern(s string) string {
	// Escape backslash first to avoid double-escaping
	s = strings.ReplaceAll(s, "\\", "\\\\")
	// Escape LIKE wildcards
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	// Escape character class brackets (SQL Server, PostgreSQL with certain collations)
	s = strings.ReplaceAll(s, "[", "\\[")
	s = strings.ReplaceAll(s, "]", "\\]")
	return s
}

// convertContains converts CEL contains() to SQL LIKE.
func (c *Converter) convertContains(call *exprpb.Expr_Call) (squirrel.Sqlizer, error) {
	if call == nil {
		return nil, fmt.Errorf("nil call expression")
	}

	if len(call.Args) != 1 {
		return nil, fmt.Errorf("contains() requires exactly 1 argument, got %d", len(call.Args))
	}

	// Get the field name (receiver/target)
	field, err := c.getFieldName(call.Target)
	if err != nil {
		return nil, err
	}
	column := c.mapFieldName(field)

	// Get the search string (argument)
	value, err := c.getConstantValue(call.Args[0])
	if err != nil {
		return nil, err
	}

	strValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("contains() requires string argument, got %T", value)
	}

	// SECURITY FIX: Escape LIKE special characters to prevent SQL injection
	escapedValue := escapeLikePattern(strValue)
	return squirrel.Like{column: fmt.Sprintf("%%%s%%", escapedValue)}, nil
}

// convertStartsWith converts CEL startsWith() to SQL LIKE.
func (c *Converter) convertStartsWith(call *exprpb.Expr_Call) (squirrel.Sqlizer, error) {
	if call == nil {
		return nil, fmt.Errorf("nil call expression")
	}

	if len(call.Args) != 1 {
		return nil, fmt.Errorf("startsWith() requires exactly 1 argument, got %d", len(call.Args))
	}

	// Get the field name (receiver/target)
	field, err := c.getFieldName(call.Target)
	if err != nil {
		return nil, err
	}
	column := c.mapFieldName(field)

	// Get the prefix string (argument)
	value, err := c.getConstantValue(call.Args[0])
	if err != nil {
		return nil, err
	}

	strValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("startsWith() requires string argument, got %T", value)
	}

	// SECURITY FIX: Escape LIKE special characters to prevent SQL injection
	escapedValue := escapeLikePattern(strValue)
	return squirrel.Like{column: fmt.Sprintf("%s%%", escapedValue)}, nil
}

// convertEndsWith converts CEL endsWith() to SQL LIKE.
func (c *Converter) convertEndsWith(call *exprpb.Expr_Call) (squirrel.Sqlizer, error) {
	if call == nil {
		return nil, fmt.Errorf("nil call expression")
	}

	if len(call.Args) != 1 {
		return nil, fmt.Errorf("endsWith() requires exactly 1 argument, got %d", len(call.Args))
	}

	// Get the field name (receiver/target)
	field, err := c.getFieldName(call.Target)
	if err != nil {
		return nil, err
	}
	column := c.mapFieldName(field)

	// Get the suffix string (argument)
	value, err := c.getConstantValue(call.Args[0])
	if err != nil {
		return nil, err
	}

	strValue, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("endsWith() requires string argument, got %T", value)
	}

	// SECURITY FIX: Escape LIKE special characters to prevent SQL injection
	escapedValue := escapeLikePattern(strValue)
	return squirrel.Like{column: fmt.Sprintf("%%%s", escapedValue)}, nil
}

// getFieldName extracts a field name from an expression.
func (c *Converter) getFieldName(expr *exprpb.Expr) (string, error) {
	if ident := expr.GetIdentExpr(); ident != nil {
		return ident.Name, nil
	}

	if sel := expr.GetSelectExpr(); sel != nil {
		return sel.Field, nil
	}

	return "", fmt.Errorf("expression is not a field identifier: %T", expr.ExprKind)
}

// getConstantValue extracts a constant value from an expression.
func (c *Converter) getConstantValue(expr *exprpb.Expr) (interface{}, error) {
	constExpr := expr.GetConstExpr()
	if constExpr == nil {
		return nil, fmt.Errorf("expression is not a constant: %T", expr.ExprKind)
	}

	switch constExpr.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		return constExpr.GetBoolValue(), nil
	case *exprpb.Constant_Int64Value:
		return constExpr.GetInt64Value(), nil
	case *exprpb.Constant_Uint64Value:
		return constExpr.GetUint64Value(), nil
	case *exprpb.Constant_DoubleValue:
		return constExpr.GetDoubleValue(), nil
	case *exprpb.Constant_StringValue:
		return constExpr.GetStringValue(), nil
	case *exprpb.Constant_NullValue:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported constant type: %T", constExpr.ConstantKind)
	}
}

// getListValues extracts list values from an expression.
func (c *Converter) getListValues(expr *exprpb.Expr) ([]interface{}, error) {
	list := expr.GetListExpr()
	if list == nil {
		return nil, fmt.Errorf("expression is not a list: %T", expr.ExprKind)
	}

	// SECURITY: Limit IN clause size to prevent DoS
	if len(list.Elements) > c.maxInClauseSize {
		return nil, fmt.Errorf("IN clause size %d exceeds maximum of %d",
			len(list.Elements), c.maxInClauseSize)
	}

	values := make([]interface{}, len(list.Elements))
	for i, elem := range list.Elements {
		val, err := c.getConstantValue(elem)
		if err != nil {
			return nil, fmt.Errorf("failed to get list element %d: %w", i, err)
		}
		values[i] = val
	}

	return values, nil
}

// convertConstExpr converts a constant expression (shouldn't typically appear at top level).
func (c *Converter) convertConstExpr(constExpr *exprpb.Constant) (squirrel.Sqlizer, error) {
	if constExpr == nil {
		return nil, fmt.Errorf("nil constant expression")
	}

	switch constExpr.ConstantKind.(type) {
	case *exprpb.Constant_BoolValue:
		if constExpr.GetBoolValue() {
			return squirrel.Expr("TRUE"), nil
		}
		return squirrel.Expr("FALSE"), nil
	default:
		return nil, fmt.Errorf("unsupported constant type at top level: %T", constExpr.ConstantKind)
	}
}

// mapFieldName maps a CEL field name to a SQL column name using the converter's column mappings.
func (c *Converter) mapFieldName(field string) string {
	if c.columnMappings != nil {
		if mapped, ok := c.columnMappings[field]; ok {
			return mapped
		}
	}
	return field
}

// notSqlizer wraps a Sqlizer to add NOT prefix.
type notSqlizer struct {
	inner squirrel.Sqlizer
}

//nolint:revive // ToSql is required by squirrel.Sqlizer interface
func (n *notSqlizer) ToSql() (string, []interface{}, error) {
	sql, args, err := n.inner.ToSql()
	if err != nil {
		return "", nil, err
	}

	// Wrap in NOT
	return fmt.Sprintf("NOT (%s)", sql), args, nil
}

// QuoteIdentifier quotes a SQL identifier to prevent SQL injection.
func QuoteIdentifier(name string) string {
	// Replace any double quotes with escaped double quotes
	escaped := strings.ReplaceAll(name, `"`, `""`)
	return fmt.Sprintf(`"%s"`, escaped)
}
