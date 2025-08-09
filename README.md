# FastAPI Server for Connecting LLMs, AI Tools, or Frontends to Any Database

## Live App
Live Apps and GPTs using this repo as backend available at [app.tigzig.com](https://app.tigzig.com)

This FastAPI server provides a simple way to connect LLMs, AI tools, or frontends to any database. It includes multiple endpoints tailored for various use cases. For detailed explanations of each endpoint, visit the 'Build' section at [app.tigzig.com](https://app.tigzig.com).

---

### Build Command

To install the required dependencies, run:

```bash
pip install -r requirements.txt
```

---

### Environment Variable Setup

Before running the application, you need to set up your environment variables.

1.  In the root directory, make a copy of the `.env.example` file and rename it to `.env`.
2.  Edit the `.env` file to add your specific credentials and endpoints.

---

### Run Command

To start the server for production, execute:

```bash
uvicorn app:app --host 0.0.0.0 --port $PORT
```

For local development, you can also run:
```bash
python app.py
```
This will start the server on `http://localhost:8000` by default.

---

### Environment Variables and Database Connections

Proper configuration of environment variables is crucial for the server to function correctly.

1.  **`RT_ENDPOINT` for LLM Integration**
    *   The `RT_ENDPOINT` is required for file upload endpoints that use an LLM for schema inference (`/upload-file-llm-pg/`, `/upload-file-llm-mysql/`, `/upload-file-custom-db-pg/`, `/upload-file-custom-db-mysql/`).
    *   This variable should contain the URL of a running proxy server that securely handles API calls to OpenAI, Gemini, or other LLM providers without exposing API keys.

2.  **Database Credentials for Fixed Connections**
    *   The server can be configured with a set of fixed database connections using environment variables. This is useful when you have databases that you connect to frequently.
    *   The following variables are supported:
        *   **AWS (MySQL):** `AWS_DATABASE_NAME`, `AWS_HOST`, `AWS_USER`, `AWS_PASSWORD`
        *   **Azure (MySQL):** `AZURE_DATABASE_NAME`, `AZURE_HOST`, `AZURE_USER`, `AZURE_PASSWORD`
        *   **Neon (PostgreSQL):** `NEON_HOST`, `NEON_DATABASE`, `NEON_USER`, `NEON_PASSWORD`
        *   **Filessio (MySQL):** `FILESSIO_HOST`, `FILESSIO_DATABASE`, `FILESSIO_USER`, `FILESSIO_PASSWORD`, `FILESSIO_PORT`

    *   **A Note on Flexibility**: The names `AWS`, `AZURE`, `NEON`, and `FILESSIO` are used as convenient identifiers within the code for the `/sqlquery/` endpoint. **You are not restricted to these specific providers.** For example, you can store your Google Cloud SQL credentials in the `AWS_*` variables and it will work, as long as it's a compatible MySQL database. These variables simply provide a way to have pre-configured, fixed database connections for repeated use.

*   **Note on `FLOWISE_API_ENDPOINT`**: This variable is present in the `.env.example` file but is not used by this application. It may be required for other tools or integrations, such as REX.

---

## Security Measures

This section outlines the basic security measures implemented in this server and provides recommendations for production environments.

### Rate Limiting

To protect the server from being overloaded by too many requests, the API has a configurable rate limit.

*   **Environment Variable**: `RATE_LIMIT`
*   **Default**: `"300/hour"`
*   **Action**: You can adjust this value in your `.env` file to suit your needs.

### CORS Whitelisting

The application uses CORS (Cross-Origin Resource Sharing) to control which domains can access the API. You **must** add your frontend application's URL to the whitelist in `app.py` for it to be able to connect.

*   **Location**: Find the `app.add_middleware(CORSMiddleware, ...)` section in `app.py`.
*   **Action**: Add your domains to the `allow_origins` list.

**Example Configuration:**

```python
# Example allow_origins list in app.py
allow_origins=[
    "https://your-frontend-app.com",
    "https://your-ai-tool.com",
    "http://localhost:1234", # Example for local development
    "http://localhost:5678"  # Example for another local service
],
```

### Further Security Recommendations

The security measures in this public version are designed to be straightforward (rate limiting and origin whitelisting). For a production environment, it is highly advisable to implement additional, more robust security measures. These can include:

*   **Authentication/Authorization**: Implement API keys, OAuth tokens, or other authentication mechanisms to ensure only authorized clients can access the endpoints.
*   **Resource Limiting**: Configure your deployment environment to limit the server's access to CPU, RAM, and other system resources to prevent abuse.
*   **General Server Hardening**: Follow standard best practices for securing your server and network infrastructure.

---

## Endpoints Overview

This section provides a quick overview of the available endpoints, what they do, and the parameters they require.

---

#### 1. **SQL Query Execution Endpoint**
**`GET /sqlquery/`**
-   **Description**: Executes a SQL query on a pre-configured database specified by the `cloud` parameter. It uses credentials from environment variables to connect to a fixed database.
-   **Parameters**:
    -   `sqlquery` (string): The SQL query to execute.
    -   `cloud` (string): The pre-configured database connection to use (`azure`, `aws`, `neon`, `filessio`). As noted above, these names are flexible labels for the connections defined in your environment variables.
-   **Authentication**: Uses credentials from environment variables for the specified database provider.

---

#### 2. **Connect to Custom Database Endpoint**
**`GET /connect-db/`**
-   **Description**: Connects to any custom MySQL or PostgreSQL database using credentials provided as parameters. This endpoint is ideal for dynamic connections.
-   **Parameters**:
    -   `host` (string): Database host address.
    -   `database` (string): Name of the database to connect to.
    -   `user` (string): Database user name.
    -   `password` (string): Database password.
    -   `port` (integer, default=3306 for MySQL, 5432 for PostgreSQL): Port number for the database.
    -   `db_type` (string, default=`mysql`): Database type (`mysql` or `postgresql`).
    -   `sqlquery` (string, optional): SQL query to execute.
-   **Authentication**: Accepts credentials as parameters.

---

#### 3. **Upload File to LLM and PostgreSQL (Fixed Connection)**
**`POST /upload-file-llm-pg/`**
-   **Description**: Uploads a file to the pre-configured Neon PostgreSQL database, using an LLM for schema inference.
-   **Parameters**:
    -   `file` (file): The file to be uploaded.
-   **Additional Notes**:
    -   Requires `RT_ENDPOINT` and `NEON_*` environment variables to be configured.

---

#### 4. **Upload File to LLM and MySQL (Fixed Connection)**
**`POST /upload-file-llm-mysql/`**
-   **Description**: Uploads a file to the pre-configured Filessio MySQL database, using an LLM for schema inference.
-   **Parameters**:
    -   `file` (file): The file to be uploaded.
-   **Additional Notes**:
    -   Requires `RT_ENDPOINT` and `FILESSIO_*` environment variables to be configured.

---

#### 5. **Upload File to Custom PostgreSQL Database**
**`POST /upload-file-custom-db-pg/`**
-   **Description**: Uploads a file to any custom PostgreSQL database, using an LLM for schema inference.
-   **Parameters**:
    -   `host`, `database`, `user`, `password`, `port` (default=5432): Database connection details.
    -   `schema` (string, default=`public`): PostgreSQL schema for the table.
    -   `file` (file): The file to upload.
-   **Authentication**: Accepts database credentials as parameters.
-   **Additional Notes**:
    -   Requires `RT_ENDPOINT` to be configured.

---

#### 6. **Upload File to Custom MySQL Database**
**`POST /upload-file-custom-db-mysql/`**
-   **Description**: Uploads a file to any custom MySQL database, using an LLM for schema inference.
-   **Parameters**:
    -   `host`, `database`, `user`, `password`, `port` (default=3306): Database connection details.
    -   `sslmode` (string, optional): SSL mode for secure connections.
    -   `file` (file): The file to upload.
-   **Authentication**: Accepts database credentials as parameters.
-   **Additional Notes**:
    -   Requires `RT_ENDPOINT` to be configured.

---

#### 7. **Export Table from Custom Database**
**`GET /connect-db-export/`**
-   **Description**: Streams the contents of a database table as a text file from any custom database.
-   **Parameters**:
    -   `host` (string): Database host address.
    -   `database` (string): Name of the database to connect to.
    -   `user` (string): Database user name.
    -   `password` (string): Database password.
    -   `port` (integer, default=3306 for MySQL, 5432 for PostgreSQL): Port number for the database.
    -   `db_type` (string, default=`mysql`): Database type (`mysql` or `postgresql`).
    -   `table` (string): The name of the table to export.
-   **Authentication**: Accepts credentials as parameters.

---
## Middlewares

The application uses the following custom middlewares:

-   **`LargeUploadMiddleware`**: Increases the maximum request body size to 1.5GB for file upload endpoints.
-   **`OriginLoggingMiddleware`**: Logs the origin, user-agent, referer, and host headers of incoming requests for debugging purposes.
-   **`remove_temp_file`**: Automatically removes the temporary file created by the `/sqlquery/` endpoint after the response is sent.

---

## Connecting to ChatGPT / CustomGPT configuration

This section provides instructions for configuring a Custom GPT to connect to any MySQL or PostgreSQL database using this FastAPI server.

### Instructions

Copy the following text into the 'Instructions' field of your Custom GPT configuration:

```
Use this tool to connect to database . The user will provide host, database, username, password, and port as separate details or a URI (extract if needed). Use default ports (5432 for PostgreSQL, 3306 for MySQL) if unspecified. Use the database connection details shared by the user for all tool calls.

If any required information is missing, tell user what's missing. 

If all information is present, just go ahead and try to connect and check for available schemas. No need to check with user first. That's the purpose of user sharing the connection.



IMPORTANT
1. Convert user questions into SQL queries (Postgres or MySQL compliant as per database type specified by user) and pass them as parameters in API calls. 
2. Ensure NO SCHEMA USED for MySQL queries as MySQL database does not have schemas. FOR MYSQL QUERIES DO NOT USE 'PUBLIC' OR ANY SCHEMA NAME. Queries to be MySQL compliant.
3. PostgreSQL has schemas. So postgres queries to always include schemas (use 'public' if unspecified). 

For errors, always share the query and connection details for debugging. Allow up to 180 seconds for query responses due to possible server delays. Always execute the query and share actual results, not fabricated data.
```

### OpenAPI JSON Schema

The OpenAPI JSON schema for the `/connect-db` endpoint is located in the `docs/gptJson.json` file. You will need to replace the server URL with your own deployed FastAPI server URL. Step by step guide to setup custom GPT connected to databaes:

*   [Analytics Assistant CustomGPT Implementation Guide](https://medium.com/@amarharolikar/analytics-assistant-customgpt-implementation-guide-9382887e95b5)


-------
Built by Amar Harolikar // More tools at [app.tigzig.com](https://app.tigzig.com)  // [LinkedIn Profile](https://www.linkedin.com/in/amarharolikar)
