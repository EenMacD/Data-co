# Data Co Project

This repository contains the complete system for the Data Co platform. The project is organized into distinct applications, each with its own documentation.

## Project Components

Please refer to the specific READMEs for detailed setup and usage instructions:

### 1. [Database & Deployment](Data/database/README.md)
*   **Location**: `Data/database/`
*   **Purpose**: PostgreSQL database setup (Staging & Production), schemas, and deployment instructions.
*   **Key Docs**: [Data/database/README.md](Data/database/README.md)

### 2. [Data Ingestion UI](Data/ui/README.md)
*   **Location**: `Data/ui/`
*   **Purpose**: A Python/Flask web interface for managing data ingestion workflows.
*   **Key Docs**: [Data/ui/README.md](Data/ui/README.md)

### 3. [API (Backend)](API/README.md)
*   **Location**: `API/`
*   **Purpose**: Go-based REST API that serves data from the Production database.
*   **Key Docs**: [API/README.md](API/README.md)

### 4. [User Interface (Frontend)](UI/README.md)
*   **Location**: `UI/`
*   **Purpose**: Next.js application for end-users to search and view company data.
*   **Key Docs**: [UI/README.md](UI/README.md)

---

## Quick Overview

The system follows this data flow:
1.  **Ingestion**: Data is fetched from Companies House into the **Staging DB** using the Data Ingestion UI.
2.  **Processing**: Data is validated and merged from Staging to the **Production DB**.
3.  **Serving**: The **API** queries the Production DB.
4.  **Display**: The **UI** consumes the API to display data to users.
