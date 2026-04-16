package database

import (
	"reflect"
	"strings"
	"testing"

	"data-co/api/models"
)

type projectionFixture struct {
	CompanyNumber string `db:"company_number"`
	Ignored       string
	Hidden        string `db:"-"`
	SICCode       string `db:"sic_code"`
}

func TestBuildSelectProjectionUsesTagsAndOverrides(t *testing.T) {
	projection, err := buildSelectProjection(
		reflect.TypeOf(projectionFixture{}),
		"c",
		map[string]string{"sic_code": "array_to_string(c.sic_codes, ',')"},
	)
	if err != nil {
		t.Fatalf("buildSelectProjection returned error: %v", err)
	}

	expected := "c.company_number AS company_number,\n\tarray_to_string(c.sic_codes, ',') AS sic_code"
	if projection != expected {
		t.Fatalf("unexpected projection:\nexpected:\n%s\n\ngot:\n%s", expected, projection)
	}
}

func TestCompanyProjectionUsesModelFieldOrder(t *testing.T) {
	expected := strings.Join([]string{
		"c.company_number AS company_number",
		"\tc.company_name AS company_name",
		"\tc.company_status AS company_status",
		"\tc.locality AS locality",
		"\tc.region AS region",
		"\tc.postal_code AS postal_code",
		"\tarray_to_string(c.sic_codes, ',') AS sic_code",
		"\tc.incorporation_date AS incorporation_date",
	}, ",\n")

	if CompanyProjection() != expected {
		t.Fatalf("unexpected company projection:\nexpected:\n%s\n\ngot:\n%s", expected, CompanyProjection())
	}
}

func TestBuildCompanyQueryUsesSharedFragmentsAndProjection(t *testing.T) {
	query, args := BuildCompanyQuery(models.CompanySearchFilters{
		Limit:   25,
		Offset:  50,
		OrderBy: "turnover",
	})

	for _, fragment := range []string{
		"WITH latest_financials AS (",
		"LEFT JOIN latest_financials latest_fin ON c.company_number = latest_fin.company_number",
		"LEFT JOIN officer_counts ON c.company_number = officer_counts.company_number",
		"array_to_string(c.sic_codes, ',') AS sic_code",
		"ORDER BY latest_fin.turnover",
		"LIMIT $1 OFFSET $2",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing fragment %q:\n%s", fragment, query)
		}
	}

	expectedArgs := []interface{}{25, 50}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected args: expected %#v, got %#v", expectedArgs, args)
	}
}

func TestBuildCompanyQueryFiltersIndustryPrefixes(t *testing.T) {
	query, args := BuildCompanyQuery(models.CompanySearchFilters{
		Industry: []string{"01", "02", "03"},
		Limit:    20,
		Offset:   0,
	})

	expectedFragment := "WHERE EXISTS (SELECT 1 FROM unnest(c.sic_codes) AS sic WHERE sic ILIKE $1 OR sic ILIKE $2 OR sic ILIKE $3)"
	if !strings.Contains(query, expectedFragment) {
		t.Fatalf("query missing fragment %q:\n%s", expectedFragment, query)
	}

	if !strings.Contains(query, "LIMIT $4 OFFSET $5") {
		t.Fatalf("query should place limit and offset after industry args:\n%s", query)
	}

	expectedArgs := []interface{}{"01%", "02%", "03%", 20, 0}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected args: expected %#v, got %#v", expectedArgs, args)
	}
}

func TestBuildCompanyCountQueryUsesSharedFragmentsAndLocationCondition(t *testing.T) {
	query, args := BuildCompanyCountQuery(models.CompanySearchFilters{
		Locations: []string{"London", "Manchester"},
	})

	for _, fragment := range []string{
		"WITH latest_financials AS (",
		"SELECT\n\tCOUNT(*) AS total",
		"LEFT JOIN latest_financials latest_fin ON c.company_number = latest_fin.company_number",
		"LEFT JOIN officer_counts ON c.company_number = officer_counts.company_number",
		"WHERE ((c.locality ILIKE $1 OR c.region ILIKE $2 OR c.country ILIKE $3) OR (c.locality ILIKE $4 OR c.region ILIKE $5 OR c.country ILIKE $6))",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing fragment %q:\n%s", fragment, query)
		}
	}

	expectedArgs := []interface{}{"%London%", "%London%", "%London%", "%Manchester%", "%Manchester%", "%Manchester%"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected args: expected %#v, got %#v", expectedArgs, args)
	}
}

func TestBuildCompanyCountQueryNormalizesHierarchicalLocations(t *testing.T) {
	query, args := BuildCompanyCountQuery(models.CompanySearchFilters{
		Locations: []string{"England/London", "England/West Midlands/Birmingham"},
	})

	expectedFragments := []string{
		"WHERE ((c.locality ILIKE $1 OR c.region ILIKE $2 OR c.country ILIKE $3) OR ((c.locality ILIKE $4 OR c.region ILIKE $5 OR c.country ILIKE $6) OR (c.locality ILIKE $7 OR c.region ILIKE $8 OR c.country ILIKE $9)))",
	}
	for _, fragment := range expectedFragments {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing fragment %q:\n%s", fragment, query)
		}
	}

	expectedArgs := []interface{}{"%London%", "%London%", "%London%", "%West Midlands%", "%West Midlands%", "%West Midlands%", "%Birmingham%", "%Birmingham%", "%Birmingham%"}
	if !reflect.DeepEqual(args, expectedArgs) {
		t.Fatalf("unexpected args: expected %#v, got %#v", expectedArgs, args)
	}
}

func TestBuildCompanyByNumberQueryUsesSharedProjection(t *testing.T) {
	query := BuildCompanyByNumberQuery()

	for _, fragment := range []string{
		"SELECT\n\tc.company_number AS company_number",
		"array_to_string(c.sic_codes, ',') AS sic_code",
		"FROM production_companies c",
		"WHERE c.company_number = $1",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing fragment %q:\n%s", fragment, query)
		}
	}

	if strings.Contains(query, "latest_financials") {
		t.Fatalf("single-company query should not include search joins:\n%s", query)
	}
}
