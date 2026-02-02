# FastAPI Database Backend

FastAPI server for connecting LLMs, AI tools, or frontends to PostgreSQL and MySQL databases. Handles large file uploads (tested up to 1.6GB compressed).

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env  # Edit with your credentials
```

## Run

```bash
# Production
uvicorn app:app --host 0.0.0.0 --port $PORT

# Development
python app.py  # Runs on localhost:8000
```

## Environment Variables

### Required for LLM Schema Detection
- `RT_ENDPOINT` - URL of proxy server for OpenAI/LLM API calls

### Database Connections (Fixed)
These are labels for pre-configured connections used by `/sqlquery/` endpoint. You can store any compatible database credentials in these variables.

```
NEON_HOST, NEON_DATABASE, NEON_USER, NEON_PASSWORD          # PostgreSQL
FILESSIO_HOST, FILESSIO_DATABASE, FILESSIO_USER, FILESSIO_PASSWORD, FILESSIO_PORT  # MySQL
AWS_DATABASE_NAME, AWS_HOST, AWS_USER, AWS_PASSWORD         # MySQL
AZURE_DATABASE_NAME, AZURE_HOST, AZURE_USER, AZURE_PASSWORD # MySQL
```

### Rate Limiting
- `RATE_LIMIT` - Default: `300/hour`

## Endpoints

### SQL Query on Fixed Database
`GET /sqlquery/`
- `sqlquery` (string) - SQL query
- `cloud` (string) - Connection label: `azure`, `aws`, `neon`, `filessio`

### SQL Query on Custom Database
`GET /connect-db/`
- `host`, `database`, `user`, `password`, `port`, `db_type` (mysql/postgresql)
- `sqlquery` (string, optional)

### File Upload - Fixed PostgreSQL (Neon)
`POST /upload-file-llm-pg/`
- `file` - CSV/gzip file
- Requires: `RT_ENDPOINT`, `NEON_*` env vars

### File Upload - Fixed MySQL (Filessio)
`POST /upload-file-llm-mysql/`
- `file` - CSV/gzip file
- Requires: `RT_ENDPOINT`, `FILESSIO_*` env vars

### File Upload - Custom PostgreSQL
`POST /upload-file-custom-db-pg/`
- `host`, `database`, `user`, `password`, `port` (default 5432), `schema` (default `public`)
- `file` - CSV/gzip file
- Requires: `RT_ENDPOINT`

### File Upload - Custom MySQL
`POST /upload-file-custom-db-mysql/`
- `host`, `database`, `user`, `password`, `port` (default 3306), `sslmode` (optional)
- `file` - CSV/gzip file
- Requires: `RT_ENDPOINT`

### Export Table
`GET /connect-db-export/`
- `host`, `database`, `user`, `password`, `port`, `db_type`, `table`
- Streams table contents as text file

## Architecture

### Connection Pooling
`PostgresPoolManager` class manages connection pools per database. Pools are keyed by `host:port:database:user`. Stale connections detected with `SELECT 1` and retried up to 3 times before creating fresh connection. Uses `asyncio.Lock()` for thread safety.

### File Upload Processing
- File size detection from Content-Length header
- Threshold: 100MB - below uses memory, above streams to disk
- Disk write chunk size: 32MB
- Gzip files: streaming decompression (32MB chunks)
- Delimiter auto-detection: tab > comma > pipe
- Column name sanitization for SQL compatibility

### Database Insertion
- PostgreSQL: `COPY` command via `copy_expert()`
- MySQL: Batch insert with Polars `scan_csv()` + `collect_chunks()`, 100k rows per chunk
- Polars fails: falls back to Pandas with `on_bad_lines='skip'`
- MySQL optimizations: disables foreign_key_checks, unique_checks, autocommit during bulk insert

### Async Handling
- All endpoints are `async def`
- Blocking DB operations wrapped with `asyncio.get_event_loop().run_in_executor()`
- File reads use `await file.read(chunk_size)`

### Timeouts
- Connection: 30 seconds
- Statement (PostgreSQL): 15 minutes (900000ms)
- Export queries: 10 minutes

### Temporary Files
- Created with `tempfile.NamedTemporaryFile(delete=False)`
- Cleaned up in `finally` blocks
- Middleware cleans up query result files after response

## Security

### CORS
Whitelist your domains in `app.py` under `CORSMiddleware` configuration.

### Rate Limiting
IP-based via slowapi. Hits logged to `static/rate_limit_hits.txt`.

### Production Recommendations
- Add API key authentication
- Configure resource limits (CPU, RAM)
- Use SSL for database connections (Neon requires `sslmode='require'`)

## Middlewares

- `LargeUploadMiddleware` - Allows up to 1.5GB request body for upload endpoints
- `OriginLoggingMiddleware` - Logs origin, user-agent, referer headers
- `remove_temp_file` - Cleans up temp files after FileResponse
- `rate_limit_logging` - Logs rate limit hits

## Custom GPT Integration

Use `docs/gptJson.json` schema for Custom GPT actions. Replace server URL with your deployment.

Instructions for GPT:
```
Use this tool to connect to a database. User provides host, database, username, password, port.
Default ports: 5432 (PostgreSQL), 3306 (MySQL).
Convert questions to SQL. PostgreSQL uses schemas (default 'public'). MySQL has no schemas.
```

## Files

```
app.py                    # Main FastAPI application
requirements.txt          # Dependencies
.env.example             # Environment variable template
docs/gptJson.json        # OpenAPI schema for Custom GPT
docs/POST_FILE_UPLOAD_PROCESS.md  # Large file upload documentation
```

## Dependencies

Core: fastapi, uvicorn, psycopg2-binary, mysql-connector-python, polars, pandas, python-multipart, slowapi, aiohttp

---
Built by Amar Harolikar

## Author

Built by [Amar Harolikar](https://www.linkedin.com/in/amarharolikar/)

Explore 30+ open source AI tools for analytics, databases & automation at [tigzig.com](https://tigzig.com)
