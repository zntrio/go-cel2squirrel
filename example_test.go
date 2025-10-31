package cel2squirrel_test

import (
	"fmt"
	"log"

	"github.com/Masterminds/squirrel"
	"github.com/google/cel-go/cel"
	"zntr.io/cel2squirrel"
)

// Example demonstrates basic usage of the CEL to SQL converter
func Example() {
	// Define field declarations (CEL variables and their types)
	config := cel2squirrel.Config{
		FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
		},
	}

	// Create converter
	converter, err := cel2squirrel.NewConverter(config)
	if err != nil {
		log.Fatal(err)
	}

	// Convert CEL expression to SQL
	celExpr := `status == "published" && age >= 18`
	result, err := converter.Convert(celExpr)
	if err != nil {
		log.Fatal(err)
	}

	// Build complete query with Squirrel
	query := squirrel.Select("*").
		From("prompts").
		Where(result.Where)

	sql, args, err := query.ToSql()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(sql)
	fmt.Printf("Args: %v\n", args)
	// Output:
	// SELECT * FROM prompts WHERE (status = ? AND age >= ?)
	// Args: [published 18]
}

// Example_fieldMappings demonstrates mapping CEL field names to SQL column names
func Example_fieldMappings() {
	config := cel2squirrel.Config{
		FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
			"isDraft": {Type: cel.BoolType, Column: "is_draft"},
			"ownerId": {Type: cel.StringType, Column: "owner_id"},
		},
	}

	converter, err := cel2squirrel.NewConverter(config)
	if err != nil {
		log.Fatal(err)
	}

	// Column mappings are now part of the config
	celExpr := `isDraft == false && ownerId == "user123"`
	result, err := converter.Convert(celExpr)
	if err != nil {
		log.Fatal(err)
	}

	sql, args, _ := result.Where.ToSql()
	fmt.Println(sql)
	fmt.Printf("Args: %v\n", args)
	// Output:
	// (is_draft = ? AND owner_id = ?)
	// Args: [false user123]
}

// Example_postgreSQL demonstrates using PostgreSQL-style placeholders
func Example_postgreSQL() {
	config := cel2squirrel.Config{
		FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
		},
	}

	converter, err := cel2squirrel.NewConverter(config)
	if err != nil {
		log.Fatal(err)
	}

	celExpr := `status == "published" && age >= 18`
	result, err := converter.Convert(celExpr)
	if err != nil {
		log.Fatal(err)
	}

	// Use PostgreSQL placeholder format
	query := squirrel.Select("*").
		From("prompts").
		Where(result.Where).
		PlaceholderFormat(squirrel.Dollar)

	sql, args, _ := query.ToSql()
	fmt.Println(sql)
	fmt.Printf("Args: %v\n", args)
	// Output:
	// SELECT * FROM prompts WHERE (status = $1 AND age >= $2)
	// Args: [published 18]
}

// Example_stringOperations demonstrates CEL string methods
func Example_stringOperations() {
	config := cel2squirrel.Config{
		FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
			"label": {Type: cel.StringType, Column: "label"},
		},
	}

	converter, err := cel2squirrel.NewConverter(config)
	if err != nil {
		log.Fatal(err)
	}

	// Contains
	result, err := converter.Convert(`label.contains("gpt")`)
	if err != nil {
		log.Fatal(err)
	}
	sql, args, _ := result.Where.ToSql()
	fmt.Println("Contains:", sql)
	fmt.Printf("Args: %v\n", args)

	// Starts with
	result, err = converter.Convert(`label.startsWith("prod-")`)
	if err != nil {
		log.Fatal(err)
	}
	sql, args, _ = result.Where.ToSql()
	fmt.Println("Starts with:", sql)
	fmt.Printf("Args: %v\n", args)

	// Ends with
	result, err = converter.Convert(`label.endsWith("-v2")`)
	if err != nil {
		log.Fatal(err)
	}
	sql, args, _ = result.Where.ToSql()
	fmt.Println("Ends with:", sql)
	fmt.Printf("Args: %v\n", args)

	// Output:
	// Contains: label LIKE ?
	// Args: [%gpt%]
	// Starts with: label LIKE ?
	// Args: [prod-%]
	// Ends with: label LIKE ?
	// Args: [%-v2]
}

// Example_complexExpression demonstrates nested boolean logic
func Example_complexExpression() {
	config := cel2squirrel.Config{
		FieldDeclarations: map[string]cel2squirrel.ColumnMapping{
			"status": {Type: cel.StringType, Column: "status"},
			"age":    {Type: cel.IntType, Column: "age"},
			"rating": {Type: cel.DoubleType, Column: "rating"},
		},
	}

	converter, err := cel2squirrel.NewConverter(config)
	if err != nil {
		log.Fatal(err)
	}

	celExpr := `(status == "published" || status == "featured") && age >= 18 && rating > 4.0`
	result, err := converter.Convert(celExpr)
	if err != nil {
		log.Fatal(err)
	}

	query := squirrel.Select("id", "label", "rating").
		From("prompts").
		Where(result.Where).
		OrderBy("rating DESC").
		Limit(10)

	sql, args, _ := query.ToSql()
	fmt.Println(sql)
	fmt.Printf("Args: %v\n", args)
	// Output:
	// SELECT id, label, rating FROM prompts WHERE (((status = ? OR status = ?) AND age >= ?) AND rating > ?) ORDER BY rating DESC LIMIT 10
	// Args: [published featured 18 4]
}
