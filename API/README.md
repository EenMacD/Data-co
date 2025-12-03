# Data Co API - Go Backend

Go REST API for querying UK Companies House data with advanced filtering.

## Features

- Filter companies by industry, location, revenue, employees, profitability
- Advanced filters: company size, age, status, net assets, debt level
- Full-text search on company names
- Pagination support
- PostgreSQL database with connection pooling
- CORS enabled for Next.js frontend

## Setup

### Prerequisites

- Go 1.21 or higher
- PostgreSQL database (production) with data loaded
- `.env` file in root directory with database credentials

### Installation

1. **Install dependencies:**
   ```bash
   cd API
   go mod download
   ```

2. **Configure environment variables:**

   Ensure your `.env` file (in the root `data-co` directory) contains:
   ```
   PRODUCTION_DB_HOST=localhost
   PRODUCTION_DB_PORT=5432
   PRODUCTION_DB_NAME=production
   PRODUCTION_DB_USER=your_user
   PRODUCTION_DB_PASSWORD=your_password
   API_PORT=8080
   ```

3. **Run the API server:**
   ```bash
   go run main.go
   ```

   The server will start on `http://localhost:8080`

### Building for Production

```bash
go build -o data-co-api main.go
./data-co-api
```

## API Endpoints

### POST /api/companies/search

Search companies with filters.

**Request Body:**
```json
{
  "industry": "tech",
  "location": "london",
  "revenue": "1m-10m",
  "employees": "11-50",
  "profitability": "profitable",
  "companySize": "small",
  "companyAge": "3-5",
  "companyStatus": "active",
  "netAssets": "100k-1m",
  "debtLevel": "low",
  "searchTerm": "software",
  "limit": 100,
  "offset": 0,
  "orderBy": "c.company_name"
}
```

All fields are optional. Defaults:
- `limit`: 100
- `offset`: 0
- `companyStatus`: "active"

**Response:**
```json
{
  "companies": [
    {
      "id": 1,
      "company_number": "12345678",
      "company_name": "Example Ltd",
      "company_status": "active",
      "locality": "London",
      "region": "Greater London",
      "postal_code": "SW1A 1AA",
      "primary_sic_code": "62011",
      "industry_category": "Technology",
      "incorporation_date": "2018-01-15T00:00:00Z",
      "turnover": 5000000,
      "profit_after_tax": 500000,
      "total_assets": 2000000,
      "net_worth": 1500000,
      "profit_margin": 0.10,
      "latest_accounts_date": "2023-12-31T00:00:00Z",
      "active_officers_count": 5
    }
  ],
  "total": 1,
  "limit": 100,
  "offset": 0,
  "has_more": false
}
```

### POST /api/companies/count

Get count of companies matching filters.

**Request Body:** Same as `/search` endpoint

**Response:**
```json
{
  "total": 1234
}
```

### GET /api/companies/:id

Get single company by ID.

**Response:** Single company object (same structure as in search results)

### GET /api/health

Health check endpoint.

**Response:**
```json
{
  "status": "ok",
  "service": "data-co-api"
}
```

## Filter Options

### Industry
- `tech` - Technology
- `finance` - Finance
- `retail` - Retail
- `manufacturing` - Manufacturing
- `professional` - Professional Services

### Location
- `london` - London
- `manchester` - Manchester
- `birmingham` - Birmingham
- `edinburgh` - Edinburgh
- `bristol` - Bristol

### Revenue
- `0-1m` - Up to £1M
- `1m-10m` - £1M - £10M
- `10m-50m` - £10M - £50M
- `50m-100m` - £50M - £100M
- `100m+` - £100M+

### Employees
- `1-10` - 1-10 employees
- `11-50` - 11-50 employees
- `51-250` - 51-250 employees
- `251+` - 251+ employees

### Profitability
- `profitable` - Profitable companies
- `loss_making` - Loss making companies
- `breakeven` - Break-even (±£10k)

### Company Size
- `micro` - Micro (1-10 employees)
- `small` - Small (11-50 employees)
- `medium` - Medium (51-250 employees)
- `large` - Large (251+ employees)

### Company Age
- `0-2` - 0-2 years
- `3-5` - 3-5 years
- `6-10` - 6-10 years
- `11-20` - 11-20 years
- `21+` - 21+ years

### Company Status
- `active` - Active companies (default)
- `all` - All statuses
- `dissolved` - Dissolved companies
- `liquidation` - In liquidation

### Net Assets
- `negative` - Negative net assets
- `0-100k` - £0 - £100K
- `100k-1m` - £100K - £1M
- `1m-10m` - £1M - £10M
- `10m+` - £10M+

### Debt Level
- `none` - No debt (<1% of assets)
- `low` - Low (1-30% of assets)
- `medium` - Medium (30-60% of assets)
- `high` - High (60%+ of assets)

## Database Schema

The API queries the production PostgreSQL database with the following main tables:
- `companies` - Company master data
- `officers` - Company officers/directors
- `financials` - Financial statements

See [schema_production.sql](../Data/database/schema_production.sql) for full schema.

## Development

### Project Structure

```
API/
├── main.go              # Entry point
├── config/
│   └── config.go        # Configuration loader
├── database/
│   ├── connection.go    # DB connection
│   └── queries.go       # Query builder
├── handlers/
│   └── companies.go     # HTTP handlers
├── models/
│   └── company.go       # Data models
├── go.mod               # Go dependencies
└── README.md            # This file
```

### Testing

```bash
# Test health endpoint
curl http://localhost:8080/api/health

# Test company search
curl -X POST http://localhost:8080/api/companies/search \
  -H "Content-Type: application/json" \
  -d '{"industry":"tech","location":"london","limit":10}'

# Test company count
curl -X POST http://localhost:8080/api/companies/count \
  -H "Content-Type: application/json" \
  -d '{"industry":"tech","location":"london"}'
```

## Connecting Frontend

Update your Next.js frontend to call the Go API:

```typescript
// In your Next.js app
const API_BASE_URL = 'http://localhost:8080/api';

async function searchCompanies(filters) {
  const response = await fetch(`${API_BASE_URL}/companies/search`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(filters),
  });

  return await response.json();
}
```

## Troubleshooting

### Connection Refused
- Check that PostgreSQL is running
- Verify database credentials in `.env`
- Ensure production database exists and has data

### No Results
- Check that data has been merged from staging to production
- Verify filters are correctly formatted
- Check API logs for query errors

### CORS Errors
- Ensure frontend URL is in allowed origins (main.go)
- Check that API server is running on correct port
