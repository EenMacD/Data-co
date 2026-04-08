package database

import (
	"fmt"
	"reflect"
	"strings"
	"sync"

	"data-co/api/models"
)

var (
	companyProjectionOnce sync.Once
	companyProjection     string
	companyProjectionErr  error
)

var companyProjectionOverrides = map[string]string{
	"sic_code": "array_to_string(c.sic_codes, ',')",
}

// CompanyProjection returns the shared company SELECT projection derived from models.Company.
func CompanyProjection() string {
	companyProjectionOnce.Do(func() {
		companyProjection, companyProjectionErr = buildSelectProjection(
			reflect.TypeOf(models.Company{}),
			"c",
			companyProjectionOverrides,
		)
	})

	if companyProjectionErr != nil {
		panic(companyProjectionErr)
	}

	return companyProjection
}

func buildSelectProjection(modelType reflect.Type, alias string, overrides map[string]string) (string, error) {
	if modelType.Kind() == reflect.Pointer {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected struct type, got %s", modelType.Kind())
	}

	if alias == "" {
		return "", fmt.Errorf("alias is required")
	}

	columns := make([]string, 0, modelType.NumField())
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		if field.PkgPath != "" {
			continue
		}

		dbTag := field.Tag.Get("db")
		if dbTag == "" || dbTag == "-" {
			continue
		}

		expr, ok := overrides[dbTag]
		if !ok {
			expr = fmt.Sprintf("%s.%s", alias, dbTag)
		}

		columns = append(columns, fmt.Sprintf("%s AS %s", expr, dbTag))
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("no db-tagged fields found on %s", modelType.Name())
	}

	return strings.Join(columns, ",\n\t"), nil
}
