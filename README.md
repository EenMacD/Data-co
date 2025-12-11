# Data Co Project

This repository contains the complete system for the Data Co platform. The project is organized into distinct applications, each with its own documentation.

## 🚀 Quick Start

To get the entire project up and running immediately:

### 1. Prerequisites
Ensure you have the following installed:
*   **PostgreSQL**: Running and accessible.
*   **Node.js**: (v18+) for the UI.
*   **Go**: (v1.21+) for the API.
*   **Python**: (v3.8+) for the Data component.

### 2. Configuration
Create a `.env` file in the project root with your database credentials:

```bash
# Copy the example file and update with your credentials
cp .env.example .env
```

Then edit `.env` with your actual database credentials:
- Update `STAGING_DB_USER` and `STAGING_DB_PASSWORD` with your PostgreSQL username/password
- Update `PRODUCTION_DB_USER` and `PRODUCTION_DB_PASSWORD` with the same credentials
- Update `COMPANIES_HOUSE_API_KEY` if you have one

> **Note:** The `.env` file is git-ignored and will not be overwritten when you pull new code. Each developer maintains their own local `.env` file.

### 3. Start Everything
Run the master script from the project root:

```bash
./run_project.sh
```

This will open **3 separate terminal windows**, one for each component:

| Component | URL | Description |
|-----------|-----|-------------|
| **UI** | [http://localhost:{UI_PORT}](http://localhost:{UI_PORT}) | The main frontend website (Next.js) |
| **API** | [http://localhost:{API_PORT}](http://localhost:{API_PORT}) | The backend API (Go) |
| **Data UI** | [http://localhost:{DATA_UI_PORT}](http://localhost:{DATA_UI_PORT}) | Internal tool for data ingestion (Flask) |

---

---

## 🐳 Running with Docker

This project can be easily run using Docker Desktop. This will start the API, UI, Database, and a Data Worker container.

### Prerequisites
- Docker Desktop installed

### Quick Start
1. **Configure environment (first time only):**
   ```bash
   cp .env.example .env
   # Edit .env if you want to customize POSTGRES_USER/POSTGRES_PASSWORD
   ```

2. **Start the application stack:**
   ```bash
   docker-compose up --build
   ```

3. **Initialize the database:**
   Open a new terminal and run:
   ```bash
   docker exec -it data-co-worker /app/setup_databases.sh
   ```

4. **Access the application:**
   - **UI:** [http://localhost:{UI_PORT}](http://localhost:{UI_PORT})
   - **API:** [http://localhost:{API_PORT}/api/health](http://localhost:{API_PORT}/api/health)
   - **Data UI:** [http://localhost:{DATA_UI_PORT}](http://localhost:{DATA_UI_PORT})

### Component Details
- **API**: Go application running on port `{API_PORT}`.
- **UI**: Next.js application running on port `{UI_PORT}`.
- **Database**: Postgres 15 running on port `{STAGING_DB_PORT}` (Staging) and `{PRODUCTION_DB_PORT}` (Production).
- **Data Worker**: Python environment for running data ingestion scripts.
- **Data UI**: Flask application for managing data ingestion (port `{DATA_UI_PORT}`).

### Running Data Ingestion
To run data ingestion scripts, execute them inside the `data-worker` container:
```bash
docker exec -it data-co-worker python Data-injestion-workflows/Api-request-workflow/api-main-db.py
```

---

## Project Components

For detailed development instructions, refer to the specific READMEs:

### 1. [Database & Deployment](Data/database/README.md)
*   **Location**: `Data/database/`
*   **Purpose**: PostgreSQL database setup (Staging & Production), schemas, and deployment instructions.

### 2. [Data Ingestion UI](Data/ui/README.md)
*   **Location**: `Data/ui/`
*   **Purpose**: A Python/Flask web interface for managing data ingestion workflows.

### 3. [API (Backend)](API/README.md)
*   **Location**: `API/`
*   **Purpose**: Go-based REST API that serves data from the Production database.

### 4. [User Interface (Frontend)](UI/README.md)
*   **Location**: `UI/`
*   **Purpose**: Next.js application for end-users to search and view company data.

---

## System Overview

The system follows this data flow:
1.  **Ingestion**: Data is fetched from Companies House into the **Staging DB** using the Data Ingestion UI.
2.  **Processing**: Data is validated and merged from Staging to the **Production DB**.
3.  **Serving**: The **API** queries the Production DB.
4.  **Display**: The **UI** consumes the API to display data to users.
