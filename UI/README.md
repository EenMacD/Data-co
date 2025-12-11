# Data Co UI

The frontend application for the Data Co platform, built with Next.js. This application allows users to search, filter, and view UK Companies House data served by the API.

## Features

*   **Company Search**: Search for companies by name.
*   **Advanced Filtering**: Filter by industry, location, revenue, employees, etc.
*   **Company Details**: View detailed information about specific companies.
*   **Responsive Design**: Optimized for desktop and mobile.

## Prerequisites

*   **Node.js**: v18+
*   **npm**: v9+
*   **API**: The Data Co API must be running (usually on port `{API_PORT}`).

## Setup

1.  **Install dependencies:**
    ```bash
    npm install
    ```

2.  **Configure Environment:**
    Create a `.env.local` file if you need to override default settings (e.g., API URL).
    ```env
    NEXT_PUBLIC_API_URL=http://localhost:{API_PORT}/api
    ```

## Development

Run the development server:

```bash
npm run dev
```

Open [http://localhost:{UI_PORT}](http://localhost:{UI_PORT}) with your browser to see the result.

## Build for Production

To build the application for production:

```bash
npm run build
```

To start the production server:

```bash
npm start
```

## Project Structure

*   `app/`: App router pages and layouts.
*   `components/`: Reusable UI components.
*   `services/`: API integration services.
*   `public/`: Static assets.
