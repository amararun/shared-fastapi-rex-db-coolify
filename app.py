from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Request, Depends, UploadFile, File, status
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import FileResponse, StreamingResponse
import mysql.connector
import psycopg2
from psycopg2 import pool
import os
import tempfile
import logging
import io
import gzip
import random
import string
import pandas as pd
import polars as pl
from typing import Dict, Any, List
import aiohttp
import httpx
from datetime import datetime
import json
import re
import asyncio
from functools import partial
from io import StringIO
import aiofiles
import aiofiles.os
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
# Add this line to reduce multipart logging
logging.getLogger("multipart.multipart").setLevel(logging.INFO)

# Database type mappings
type_mapping = {
    'int64': 'INTEGER',
    'float64': 'NUMERIC',
    'bool': 'BOOLEAN',
    'datetime64[ns]': 'TIMESTAMP',
    'object': 'TEXT'
}

# PostgreSQL Connection Pool Manager
class PostgresPoolManager:
    """
    Manages connection pools for PostgreSQL databases.
    Creates one pool per unique (host, database, user) combination.
    """
    def __init__(self):
        self._pools: Dict[str, pool.ThreadedConnectionPool] = {}
        self._lock = asyncio.Lock()

    def _get_pool_key(self, host: str, database: str, user: str, port: int) -> str:
        """Generate a unique key for the pool based on connection parameters."""
        return f"{host}:{port}:{database}:{user}"

    def _is_connection_alive(self, conn) -> bool:
        """Check if a connection is still alive by running a simple query."""
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False

    async def get_connection(self, host: str, database: str, user: str, password: str, port: int = 5432):
        """
        Get a connection from the pool, creating the pool if it doesn't exist.
        Validates connection is alive before returning. If stale, creates a new one.
        Returns a connection that should be returned via put_connection().
        """
        pool_key = self._get_pool_key(host, database, user, port)

        async with self._lock:
            if pool_key not in self._pools:
                logger.info(f"[POOL] Creating new connection pool for {host}:{database}")
                try:
                    # Create a threaded connection pool
                    # minconn=1: Keep 1 connection ready (reduced to minimize stale connections)
                    # maxconn=10: Allow up to 10 concurrent connections per pool
                    self._pools[pool_key] = pool.ThreadedConnectionPool(
                        minconn=1,
                        maxconn=10,
                        host=host,
                        database=database,
                        user=user,
                        password=password,
                        port=port,
                        connect_timeout=30,
                        options='-c statement_timeout=900000'  # 15 min timeout
                    )
                    logger.info(f"[POOL] Pool created successfully for {host}:{database}")
                except Exception as e:
                    logger.error(f"[POOL] Failed to create pool for {host}:{database}: {e}")
                    raise

        # Get connection from pool (this is thread-safe)
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = self._pools[pool_key].getconn()

                # Validate connection is still alive
                if self._is_connection_alive(conn):
                    logger.debug(f"[POOL] Got valid connection from pool {pool_key}")
                    return conn, pool_key
                else:
                    # Connection is stale, close it and try again
                    logger.warning(f"[POOL] Stale connection detected, closing and retrying (attempt {attempt + 1}/{max_retries})")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    # Mark connection as closed in pool
                    self._pools[pool_key].putconn(conn, close=True)

            except pool.PoolError as e:
                logger.error(f"[POOL] Pool exhausted for {pool_key}: {e}")
                raise

        # If all retries failed, create a fresh connection outside the pool
        logger.warning(f"[POOL] All pool connections stale, creating fresh connection for {pool_key}")
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            connect_timeout=30,
            options='-c statement_timeout=900000'
        )
        return conn, pool_key

    def put_connection(self, conn, pool_key: str):
        """Return a connection to the pool."""
        if pool_key in self._pools:
            try:
                self._pools[pool_key].putconn(conn)
                logger.debug(f"[POOL] Returned connection to pool {pool_key}")
            except Exception as e:
                logger.error(f"[POOL] Error returning connection: {e}")

    def close_pool(self, pool_key: str):
        """Close a specific pool."""
        if pool_key in self._pools:
            self._pools[pool_key].closeall()
            del self._pools[pool_key]
            logger.info(f"[POOL] Closed pool {pool_key}")

    def close_all_pools(self):
        """Close all pools (for shutdown)."""
        for key in list(self._pools.keys()):
            self._pools[key].closeall()
            logger.info(f"[POOL] Closed pool {key}")
        self._pools.clear()

# Global pool manager instance
pg_pool_manager = PostgresPoolManager()

# Add this import at the top with other imports
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

# Add this class before app initialization
class LargeUploadMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/upload-file"):
            # Increase the body limit for upload endpoints
           request._body_size_limit = 1.5 * 1024 * 1024 * 1024  # 1.5GB
        return await call_next(request)

# Add a middleware to log request headers including Origin
class OriginLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Log headers that help identify where the request is coming from
        origin = request.headers.get("origin", "No Origin header")
        user_agent = request.headers.get("user-agent", "No User-Agent header")
        referer = request.headers.get("referer", "No Referer header")
        host = request.headers.get("host", "No Host header")
        
        logger.info(f"Request to {request.url.path} from Origin: {origin}")
        logger.info(f"User-Agent: {user_agent}")
        logger.info(f"Referer: {referer}")
        logger.info(f"Host: {host}")
        
        # Log all headers for complete information (optional - can be verbose)
        logger.debug(f"All request headers: {dict(request.headers)}")
        
        # Continue processing the request
        response = await call_next(request)
        return response

# Initialize FastAPI and add middlewares in correct order
app = FastAPI()

# Custom rate limit error handler
async def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": "Rate limit exceeded. Please try again later or contact your administrator."}
    )

# Function to log rate limit hits to file
def log_rate_limit_hit(request: Request):
    """Log rate limit hits to a static file with timestamp and request details."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ip = request.headers.get("x-forwarded-for", request.client.host)
    path = request.url.path
    
    # Create static directory if it doesn't exist
    os.makedirs("static", exist_ok=True)
    
    log_entry = f"{timestamp} | IP: {ip} | Path: {path} | Headers: {dict(request.headers)}\n"
    try:
        with open("static/rate_limit_hits.txt", "a") as f:
            f.write(log_entry)
    except Exception as e:
        logger.error(f"Failed to log rate limit hit to file: {e}")

# Get rate limit from environment variable or use default
RATE_LIMIT = os.getenv("RATE_LIMIT", "300/hour")
logger.info(f"Using rate limit: {RATE_LIMIT}")

# Initialize rate limiter with simple IP detection
limiter = Limiter(key_func=get_remote_address)  # Remove default_limits as we'll use per-endpoint limits
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, custom_rate_limit_exceeded_handler)

# Add rate limit logging middleware
@app.middleware("http")
async def rate_limit_logging(request: Request, call_next):
    response = await call_next(request)
    
    # Log rate limit hits to file
    if response.status_code == 429:
        log_rate_limit_hit(request)
        logger.warning(f"[Rate Limit] Rate limit exceeded for IP: {request.headers.get('x-forwarded-for', request.client.host)} on path: {request.url.path}")
    
    return response

# First add the upload size middleware
app.add_middleware(LargeUploadMiddleware)

# Add the origin logging middleware
app.add_middleware(OriginLoggingMiddleware)

# Replace the existing CORS middleware section:
# Simplify CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://tigzig.com",
        "https://realtime.tigzig.com",
        "https://addin.xlwings.org",
        "https://rex.tigzig.com",
        "https://rexdb.tigzig.com",
        "https://rexrc.tigzig.com",
        "https://rexc.tigzig.com",
        "https://mf.tigzig.com",
        "https://rexdb2.tigzig.com",
        "https://n8n-iii2.onrender.com",
        "http://localhost:8123",   # For local development
        "http://localhost:5199"    # For local development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Load environment variables
load_dotenv()

# Replace the hardcoded credentials with environment variables
aws_database_name = os.getenv("AWS_DATABASE_NAME")
aws_host = os.getenv("AWS_HOST")
aws_user = os.getenv("AWS_USER")
aws_password = os.getenv("AWS_PASSWORD")

azure_database_name = os.getenv("AZURE_DATABASE_NAME")
azure_host = os.getenv("AZURE_HOST")
azure_user = os.getenv("AZURE_USER")
azure_password = os.getenv("AZURE_PASSWORD")

neon_host = os.getenv("NEON_HOST")
neon_database = os.getenv("NEON_DATABASE")
neon_user = os.getenv("NEON_USER")
neon_password = os.getenv("NEON_PASSWORD")

filessio_host = os.getenv("FILESSIO_HOST")
filessio_database = os.getenv("FILESSIO_DATABASE")
filessio_user = os.getenv("FILESSIO_USER")
filessio_password = os.getenv("FILESSIO_PASSWORD")
filessio_port = int(os.getenv("FILESSIO_PORT", 3307))

def create_aws_connection():
    connection = mysql.connector.connect(
        host=aws_host,
        user=aws_user,
        password=aws_password,
        database=aws_database_name
    )
    return connection

def create_azure_connection():
    connection = mysql.connector.connect(
        host=azure_host,
        user=azure_user,
        password=azure_password,
        database=azure_database_name,
        ssl_disabled=True
    )
    return connection

def create_neon_connection():
    connection = psycopg2.connect(
        host=neon_host,
        database=neon_database,
        user=neon_user,
        password=neon_password,
        sslmode='require'
    )
    return connection

def create_filessio_connection():
    connection = mysql.connector.connect(
        host=filessio_host,
        user=filessio_user,
        password=filessio_password,
        database=filessio_database,
        port=filessio_port
    )
    return connection

def get_connection(cloud: str):
    if cloud == "azure":
        return create_azure_connection()
    elif cloud == "aws":
        return create_aws_connection()
    elif cloud == "neon":
        return create_neon_connection()
    elif cloud == "filessio":
        return create_filessio_connection()
    else:
        raise ValueError("Invalid cloud provider specified. Valid options are: 'azure', 'aws', 'neon', 'filessio'")

def _execute_sql_query(cloud: str, sql: str):
    """Synchronous helper for database operations - runs in thread pool."""
    connection = get_connection(cloud)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)

        if cursor.description is not None:
            headers = [i[0] for i in cursor.description]
            results = cursor.fetchall()
            cursor.close()
            return {"type": "select", "headers": headers, "results": results}
        else:
            connection.commit()
            cursor.close()
            return {"type": "non_select"}
    finally:
        connection.close()

@app.get("/sqlquery/")
@limiter.limit(RATE_LIMIT)
async def sqlquery(sqlquery: str, cloud: str, request: Request):
    logger.debug(f"Received API call: {request.url} with cloud parameter: {cloud}")
    try:
        # Run blocking DB operations in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, partial(_execute_sql_query, cloud, sqlquery))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

    if result["type"] == "select":
        # Create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt") as temp_file:
            temp_file.write(" | ".join(result["headers"]) + "\n")
            for row in result["results"]:
                temp_file.write(" | ".join(str(item) for item in row) + "\n")
            temp_file_path = temp_file.name

        logger.debug(f"Query executed successfully, results written to {temp_file_path}")
        response = FileResponse(path=temp_file_path, filename="output.txt", media_type='text/plain')
        return response
    else:
        logger.debug("Non-SELECT query executed successfully")
        return {"status": "Query executed successfully"}

@app.middleware("http")
async def remove_temp_file(request, call_next):
    logger.debug(f"Processing request: {request.url}")
    response = await call_next(request)
    if isinstance(response, FileResponse) and os.path.exists(response.path):
        try:
            os.remove(response.path)
            logger.debug(f"Temporary file {response.path} removed successfully")
        except Exception as e:
            logger.error(f"Error removing temp file: {e}")
    return response

def _decompress_gzip_sync(temp_file_path: str, decompressed_path: str) -> dict:
    """
    Synchronous gzip decompression helper - runs in thread pool.
    Returns dict with metrics.
    """
    decompress_start = datetime.now()
    bytes_decompressed = 0

    with gzip.open(temp_file_path, 'rb') as gz_file:
        with open(decompressed_path, 'wb') as output_file:
            while True:
                chunk = gz_file.read(32 * 1024 * 1024)  # 32MB chunks
                if not chunk:
                    break
                output_file.write(chunk)
                bytes_decompressed += len(chunk)

    decompress_end = datetime.now()
    decompress_duration = (decompress_end - decompress_start).total_seconds()

    decompressed_size = os.path.getsize(decompressed_path)
    original_size = os.path.getsize(temp_file_path)

    # Clean up original compressed file
    os.unlink(temp_file_path)

    return {
        "original_size": original_size,
        "decompressed_size": decompressed_size,
        "duration": decompress_duration
    }


async def handle_file_decompression(file: UploadFile, temp_file_path: str) -> tuple[str, bool]:
    """
    Handle automatic .gz file detection and decompression.
    Returns: (final_file_path, is_compressed)
    """
    is_compressed = False
    final_file_path = temp_file_path

    # Check if filename ends with .gz (case insensitive)
    if file.filename and file.filename.lower().endswith('.gz'):
        is_compressed = True
        logger.info(f"[GZ] Detected compressed file: {file.filename}")

        decompressed_path = temp_file_path.replace('.csv', '_decompressed.csv')

        try:
            # Run blocking decompression in thread pool
            loop = asyncio.get_event_loop()
            metrics = await loop.run_in_executor(
                None,
                partial(_decompress_gzip_sync, temp_file_path, decompressed_path)
            )

            compression_ratio = (1 - metrics["original_size"] / metrics["decompressed_size"]) * 100 if metrics["decompressed_size"] > 0 else 0
            decompress_speed = (metrics["decompressed_size"] / (1024 * 1024)) / metrics["duration"] if metrics["duration"] > 0 else 0

            logger.info(f"[TIMER] Decompression completed: {metrics['original_size']/1024/1024:.2f}MB -> {metrics['decompressed_size']/1024/1024:.2f}MB in {metrics['duration']:.2f}s ({decompress_speed:.1f} MB/s)")
            logger.info(f"[GZ] Decompression successful: {metrics['original_size']:,} bytes -> {metrics['decompressed_size']:,} bytes (compression: {compression_ratio:.1f}%)")

            final_file_path = decompressed_path

        except gzip.BadGzipFile:
            logger.error(f"[GZ] Invalid gzip file format: {file.filename}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid gzip file format: {file.filename}"
            )
        except Exception as e:
            logger.error(f"[GZ] Decompression failed: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to decompress file: {str(e)}"
            )
    else:
        logger.info(f"[GZ] Regular file detected: {file.filename}")

    return final_file_path, is_compressed


async def save_upload_to_temp_file(file: UploadFile) -> str:
    """
    Save uploaded file to a temporary file using async I/O.
    Returns the temp file path.
    """
    # Create temp file path (mkstemp is fast, doesn't need executor)
    fd, temp_file_path = tempfile.mkstemp(suffix='.csv')
    os.close(fd)  # Close the file descriptor, we'll use aiofiles

    async with aiofiles.open(temp_file_path, 'wb') as temp_file:
        while True:
            chunk = await file.read(32 * 1024 * 1024)  # 32MB chunks
            if not chunk:
                break
            await temp_file.write(chunk)

    return temp_file_path


async def read_first_lines(file_path: str, num_lines: int = 5) -> list[str]:
    """
    Read first N lines from a file using async I/O.
    """
    lines = []
    async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
        async for line in f:
            lines.append(line.strip())
            if len(lines) >= num_lines:
                break
    return lines


def _pg_create_table_and_copy_sync(
    connection,
    cursor,
    table_name: str,
    schema: str,
    columns_sql: list[str],
    temp_file_path: str,
    delimiter: str
) -> int:
    """
    Synchronous PostgreSQL table creation and COPY - runs in thread pool.
    Returns row count.
    """
    # Create table
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {', '.join(columns_sql)}
        )
    """
    cursor.execute(create_table_query)
    logger.info("Table created successfully with LLM-suggested schema")

    # COPY data from file
    with open(temp_file_path, 'r', encoding='utf-8') as f:
        cursor.copy_expert(
            f"""
            COPY {schema}.{table_name}
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER '{delimiter}', HEADER true)
            """,
            f
        )

    connection.commit()

    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
    return cursor.fetchone()[0]


def _pg_custom_db_operations_sync(
    connection,
    cursor,
    table_name: str,
    schema: str,
    columns_sql: list[str],
    delimiter: str,
    temp_file_path: str = None,
    buffer: StringIO = None
) -> int:
    """
    Synchronous PostgreSQL operations for custom database - runs in thread pool.
    Supports both file-based and buffer-based COPY.
    Returns row count.
    """
    # Create schema if it doesn't exist
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Create table
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
            {', '.join(columns_sql)}
        )
    """
    cursor.execute(create_table_query)
    logger.info("Table created successfully with LLM-suggested schema")

    # COPY data - either from buffer or file
    copy_query = f"""
        COPY {schema}.{table_name}
        FROM STDIN
        WITH (FORMAT CSV, DELIMITER '{delimiter}', HEADER true)
    """

    if buffer is not None:
        buffer.seek(0)
        cursor.copy_expert(copy_query, buffer)
    else:
        with open(temp_file_path, 'r', encoding='utf-8') as f:
            cursor.copy_expert(copy_query, f)

    connection.commit()

    # Get row count
    cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
    return cursor.fetchone()[0]


def _mysql_create_table_and_insert_sync(
    host: str,
    user: str,
    password: str,
    database: str,
    port: int,
    table_name: str,
    columns_sql: list[str],
    temp_file_path: str,
    delimiter: str,
    chunk_size: int = 25000,
    sslmode: str = None,
    disable_binlog: bool = True
) -> dict:
    """
    Synchronous MySQL table creation and data insertion - runs in thread pool.
    Returns dict with row count and connection info.
    """
    # Build connection parameters
    connection_params = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port,
        "allow_local_infile": True,
        "charset": "utf8mb4"
    }

    if sslmode and sslmode.lower() == 'require':
        connection_params.update({
            "ssl_verify_cert": True,
            "ssl_verify_identity": True
        })

    connection = mysql.connector.connect(**connection_params)
    cursor = connection.cursor()
    logger.info("Successfully connected to MySQL database")

    try:
        # Performance settings
        cursor.execute("SET foreign_key_checks = 0")
        cursor.execute("SET unique_checks = 0")
        cursor.execute("SET autocommit = 0")
        if disable_binlog:
            try:
                cursor.execute("SET sql_log_bin = 0")
            except Exception:
                pass  # May not have permission

        # Create table
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {', '.join(columns_sql)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        cursor.execute(create_table_query)
        logger.info("Table created successfully with LLM-suggested schema")

        # Use Polars batched reader
        rows_inserted = 0
        chunk_number = 0

        reader = pl.read_csv_batched(
            temp_file_path,
            separator=delimiter,
            ignore_errors=True,
            try_parse_dates=False,
            quote_char='"',
            infer_schema_length=10000,
            batch_size=chunk_size
        )

        while True:
            batches = reader.next_batches(1)
            if not batches:
                break

            chunk_df = batches[0]
            chunk_number += 1
            logger.info(f"[TIMER] Processing chunk {chunk_number}: {len(chunk_df)} rows")

            values = chunk_df.rows()
            placeholders = ', '.join(['%s'] * len(chunk_df.columns))
            insert_query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
            cursor.executemany(insert_query, values)
            rows_inserted += len(values)

            connection.commit()
            logger.info(f"[TIMER] Inserted chunk {chunk_number}: {len(values)} rows (Total: {rows_inserted:,} rows)")

        # Reset performance settings
        cursor.execute("SET foreign_key_checks = 1")
        cursor.execute("SET unique_checks = 1")
        cursor.execute("SET autocommit = 1")
        if disable_binlog:
            try:
                cursor.execute("SET sql_log_bin = 1")
            except Exception:
                pass

        # Get final count
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        final_count = cursor.fetchone()[0]

        return {
            "rows_inserted": rows_inserted,
            "final_count": final_count
        }

    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")


def detect_delimiter(first_line: str) -> str:
    """
    Detects the delimiter in a data file by analyzing the first line.
    Priority order: tab, comma, pipe (most common to least common)
    """
    if not first_line:
        logger.warning("Empty first line provided for delimiter detection, defaulting to comma")
        return ','
    
    # Count occurrences of each potential delimiter
    tab_count = first_line.count('\t')
    comma_count = first_line.count(',')
    pipe_count = first_line.count('|')
    
    logger.debug(f"Delimiter analysis - Tabs: {tab_count}, Commas: {comma_count}, Pipes: {pipe_count}")
    
    # Choose delimiter based on priority and count
    if tab_count > 0:
        delimiter = '\t'
        delimiter_name = 'tab'
    elif comma_count > 0:
        delimiter = ','
        delimiter_name = 'comma'
    elif pipe_count > 0:
        delimiter = '|'
        delimiter_name = 'pipe'
    else:
        # Fallback to comma if no delimiters found
        logger.warning(f"No common delimiters found in first line: '{first_line[:100]}...', defaulting to comma")
        delimiter = ','
        delimiter_name = 'comma (fallback)'
    
    logger.info(f"Detected delimiter: '{delimiter}' ({delimiter_name})")
    return delimiter

def sanitize_column_name(col_name: str) -> str:
    """
    Sanitizes column names to be SQL-safe and consistent.
    Handles special cases like .1, .2 -> _1, _2
    """
    # First, handle common patterns like .1, .2 etc.
    import re
    col_name = re.sub(r'\.(\d+)', r'_\1', col_name)  # Convert .1 to _1, .2 to _2, etc.
    
    # Remove other special characters, keep only alphanumeric and underscores
    sanitized = ''.join(c for c in col_name if c.isalnum() or c == '_')
    
    # Convert to lowercase for consistency
    sanitized = sanitized.lower()
    
    # Ensure it doesn't start with a number (SQL requirement)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    
    # Ensure it's not empty
    if not sanitized:
        sanitized = "unnamed_column"
    
    return sanitized

def generate_table_name(base_name: str) -> str:
    """Generates a sanitized and unique table name based on base_name."""
    # Remove .gz extension if present for table naming
    if base_name.lower().endswith('.gz'):
        base_name = base_name[:-3]  # Remove .gz
    
    clean_base = ''.join(c for c in base_name if c.isalnum() or c == '_')
    clean_base = clean_base.lower()[:30]  # Convert to lowercase instead of uppercase
    random_suffix = ''.join(random.choices(string.digits, k=5))
    table_name = f"{clean_base}_{random_suffix}"
    logger.debug(f"Generated table name: {table_name}")
    return table_name

@app.post("/upload-file-llm-pg/")
@limiter.limit(RATE_LIMIT)
async def upload_file_llm_pg(
    request: Request,
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    """Handles file uploads using LLM for schema detection and creates PostgreSQL table."""
    overall_start_time = datetime.now()
    logger.info(f"[TIMER] Starting LLM-assisted file upload process for file: {file.filename} at {overall_start_time}")

    # Start timing file processing
    file_processing_start = datetime.now()
    logger.info(f"[TIMER] Starting file processing at {file_processing_start}")

    # Save file to temporary location for memory-efficient processing
    temp_file_path = None
    try:
        # Save upload to temp file using async I/O
        temp_file_path = await save_upload_to_temp_file(file)
        logger.info(f"File saved to temporary location: {temp_file_path}")

        # Handle .gz decompression if needed
        temp_file_path, is_compressed = await handle_file_decompression(file, temp_file_path)

        # Read first 5 lines for schema detection using async I/O
        first_lines = await read_first_lines(temp_file_path, 5)

        # Detect delimiter from first line
        first_line = first_lines[0] if first_lines else ''
        delimiter = detect_delimiter(first_line)

        # Prepare sample data for schema analysis
        sample_data_str = '\n'.join(first_lines)

        # Log file processing completion
        file_processing_end = datetime.now()
        file_processing_duration = (file_processing_end - file_processing_start).total_seconds()
        logger.info(f"[TIMER] File processing completed in {file_processing_duration:.2f} seconds")

        # Get schema from OpenAI
        logger.info(f"[TIMER] Starting schema detection at {datetime.now()}")
        try:
            parsed_schema = await get_schema_from_openai(sample_data_str, delimiter, "postgresql")
            logger.info(f"Received schema suggestion from OpenAI: {parsed_schema}")

            if 'columns' not in parsed_schema:
                raise ValueError("Schema does not contain 'columns' key")

        except Exception as e:
            logger.error(f"Failed to get schema from OpenAI: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get schema from OpenAI: {str(e)}"
            )

        schema_detection_end = datetime.now()
        schema_duration = (schema_detection_end - file_processing_end).total_seconds()
        logger.info(f"[TIMER] Schema detection completed in {schema_duration:.2f} seconds")

        # Generate table name and create table
        table_name = generate_table_name(os.path.splitext(file.filename.replace('.gz', ''))[0])
        logger.info(f"Generated table name: {table_name}")

        # Database operations using connection pool
        logger.info(f"[TIMER] Starting database operations at {datetime.now()}")
        pool_key = None
        try:
            connection, pool_key = await pg_pool_manager.get_connection(
                host=neon_host,
                database=neon_database,
                user=neon_user,
                password=neon_password,
                port=5432
            )
            cursor = connection.cursor()
            logger.info(f"[POOL] Got connection from pool for Neon database")

            # Prepare columns SQL
            columns_sql = []
            for col in parsed_schema['columns']:
                col_name = sanitize_column_name(col['name'])
                columns_sql.append(f'"{col_name}" {col["type"]}')

            # Run blocking database operations in thread pool
            logger.info(f"[TIMER] Starting data insertion at {datetime.now()}")
            start_time = datetime.now()

            loop = asyncio.get_event_loop()
            final_count = await loop.run_in_executor(
                None,
                partial(
                    _pg_create_table_and_copy_sync,
                    connection,
                    cursor,
                    table_name,
                    "public",
                    columns_sql,
                    temp_file_path,
                    delimiter
                )
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            logger.info(f"[TIMER] Data insertion completed in {duration:.2f} seconds")

            db_operations_end = datetime.now()
            db_duration = (db_operations_end - schema_detection_end).total_seconds()
            logger.info(f"[TIMER] Database operations completed in {db_duration:.2f} seconds")

            overall_duration = (db_operations_end - overall_start_time).total_seconds()
            rows_per_sec = final_count / overall_duration if overall_duration > 0 else 0
            logger.info(f"[TIMER] *** OVERALL COMPLETION: {final_count} rows processed in {overall_duration:.2f} seconds ({rows_per_sec:.0f} rows/sec) ***")
            logger.info(f"[TIMER] Breakdown - File: {file_processing_duration:.2f}s, Schema: {schema_duration:.2f}s, Database: {db_duration:.2f}s")

            return {
                "status": "success",
                "message": f"File uploaded and {final_count} rows inserted in {duration:.2f} seconds",
                "table_name": table_name,
                "rows_inserted": final_count,
                "duration_seconds": duration,
                "rows_per_second": final_count / duration if duration > 0 else 0,
                "columns": parsed_schema['columns'],
                "llm_schema_used": True
            }

        except Exception as e:
            logger.error(f"Database operation error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Database operation failed: {str(e)}"
            )
        finally:
            if 'cursor' in locals():
                cursor.close()
            # Return connection to pool instead of closing
            if 'connection' in locals() and pool_key:
                pg_pool_manager.put_connection(connection, pool_key)
                logger.info("[POOL] Connection returned to pool")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )
    finally:
        # Clean up temporary file
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.info(f"Temporary file {temp_file_path} cleaned up successfully")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary file: {cleanup_error}")

@app.post("/upload-file-llm-mysql/")
@limiter.limit(RATE_LIMIT)
async def upload_file_llm_mysql(
    request: Request,
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    """Handles file uploads using LLM for schema detection and creates MySQL table."""
    overall_start_time = datetime.now()
    logger.info(f"[TIMER] Starting LLM-assisted MySQL file upload process for file: {file.filename} at {overall_start_time}")

    # Start timing file processing
    file_processing_start = datetime.now()
    logger.info(f"[TIMER] Starting file processing at {file_processing_start}")

    # Save file to temporary location for memory-efficient processing
    temp_file_path = None
    try:
        # Save upload to temp file using async I/O
        temp_file_path = await save_upload_to_temp_file(file)
        logger.info(f"File saved to temporary location: {temp_file_path}")

        # Handle .gz decompression if needed
        temp_file_path, is_compressed = await handle_file_decompression(file, temp_file_path)

        # Read first 5 lines for schema detection using async I/O
        first_lines = await read_first_lines(temp_file_path, 5)

        # Detect delimiter from first line
        first_line = first_lines[0] if first_lines else ''
        delimiter = detect_delimiter(first_line)

        # Prepare sample data for schema analysis
        sample_data = '\n'.join(first_lines)

        # Log file processing completion
        file_processing_end = datetime.now()
        file_processing_duration = (file_processing_end - file_processing_start).total_seconds()
        logger.info(f"[TIMER] File processing completed in {file_processing_duration:.2f} seconds")

        # Start timing schema detection
        schema_start = datetime.now()
        logger.info(f"[TIMER] Starting schema detection at {schema_start}")
        
        # Get schema from OpenAI
        try:
            parsed_schema = await get_schema_from_openai(sample_data, delimiter, "mysql")
            logger.info(f"Received schema suggestion from OpenAI: {parsed_schema}")
            
            if 'columns' not in parsed_schema:
                raise ValueError("Schema does not contain 'columns' key")
                
        except Exception as e:
            logger.error(f"Failed to get schema from OpenAI: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get schema from OpenAI: {str(e)}"
            )

        # Log schema detection completion
        schema_end = datetime.now()
        schema_duration = (schema_end - schema_start).total_seconds()
        logger.info(f"[TIMER] Schema detection completed in {schema_duration:.2f} seconds")

        # Start timing database operations
        db_start = datetime.now()
        logger.info(f"[TIMER] Starting database operations at {db_start}")

        try:
            # Generate table name
            table_name = generate_table_name(os.path.splitext(file.filename)[0])
            logger.info(f"Generated table name: {table_name}")

            # Prepare columns SQL
            columns_sql = []
            for col in parsed_schema['columns']:
                col_name = sanitize_column_name(col['name'])
                columns_sql.append(f"`{col_name}` {col['type']}")

            # Run blocking MySQL + Polars operations in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                partial(
                    _mysql_create_table_and_insert_sync,
                    filessio_host,
                    filessio_user,
                    filessio_password,
                    filessio_database,
                    filessio_port,
                    table_name,
                    columns_sql,
                    temp_file_path,
                    delimiter,
                    25000,   # chunk_size (reduced for cloud MySQL stability)
                    None,    # sslmode
                    True     # disable_binlog
                )
            )

            final_count = result["final_count"]

            # Log database operations completion
            db_end = datetime.now()
            db_duration = (db_end - db_start).total_seconds()
            logger.info(f"[TIMER] Database operations completed in {db_duration:.2f} seconds")

            # Log overall completion
            overall_end = datetime.now()
            overall_duration = (overall_end - overall_start_time).total_seconds()
            logger.info(f"[TIMER] *** OVERALL COMPLETION: {final_count} rows processed in {overall_duration:.2f} seconds ({final_count/overall_duration:.0f} rows/sec) ***")
            logger.info(f"[TIMER] Breakdown - File: {file_processing_duration:.2f}s, Schema: {schema_duration:.2f}s, Database: {db_duration:.2f}s")

            return {
                "status": "success",
                "message": f"File uploaded and {final_count} rows inserted in {overall_duration:.2f} seconds",
                "table_name": table_name,
                "rows_inserted": final_count,
                "duration_seconds": overall_duration,
                "rows_per_second": final_count / overall_duration if overall_duration > 0 else 0,
                "columns": parsed_schema['columns'],
                "llm_schema_used": True,
                "connection_details": {
                    "host": filessio_host,
                    "database": filessio_database,
                    "user": filessio_user
                }
            }

        except Exception as e:
            if 'connection' in locals():
                connection.rollback()
            logger.error(f"Error processing data: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process data: {str(e)}"
            )
        finally:
            if 'connection' in locals():
                cursor.close()
                connection.close()
                logger.info("Database connection closed")
            # Clean up temporary file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Temporary file {temp_file_path} cleaned up successfully")
                except Exception as cleanup_error:
                    logger.error(f"Error cleaning up temporary file: {cleanup_error}")

    except Exception as e:
        logger.error(f"Unexpected error during file upload: {str(e)}")
        logger.exception("Full traceback:")
        # Clean up temporary file in case of error
        if 'temp_file_path' in locals() and temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.info(f"Temporary file {temp_file_path} cleaned up after error")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary file after error: {cleanup_error}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )

def _connect_db_execute(host: str, database: str, user: str, password: str, port: int, db_type: str, sql: str = None):
    """Synchronous helper for custom database connection and query - runs in thread pool."""
    connection = None
    try:
        if db_type.lower() == "mysql":
            connection = mysql.connector.connect(
                host=host, database=database, user=user, password=password, port=port
            )
        elif db_type.lower() == "postgresql":
            connection = psycopg2.connect(
                host=host, database=database, user=user, password=password, port=port
            )
        else:
            raise ValueError("Invalid database type. Supported types are 'mysql' and 'postgresql'")

        # Connection-only test
        if not sql:
            return {"type": "connection_test", "success": True}

        # Execute query
        cursor = connection.cursor()
        cursor.execute(sql)

        if cursor.description is not None:
            headers = [i[0] for i in cursor.description]
            results = cursor.fetchall()
            cursor.close()
            return {"type": "select", "headers": headers, "results": results}
        else:
            connection.commit()
            cursor.close()
            return {"type": "non_select"}
    finally:
        if connection:
            connection.close()

@app.get("/connect-db/")
@limiter.limit(RATE_LIMIT)
async def connect_db(
    request: Request,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 3306,
    db_type: str = "mysql",
    sqlquery: str = None,
) -> Any:
    """
    Execute SQL query using provided database connection credentials.
    """
    logger.debug(f"Received API call: {request.url}")
    logger.info(f"Attempting to connect to {db_type} database at {host}")

    try:
        # Run blocking DB operations in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            partial(_connect_db_execute, host, database, user, password, port, db_type, sqlquery)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except (mysql.connector.Error, psycopg2.Error) as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    logger.info("Database connection established successfully")

    if result["type"] == "connection_test":
        return {
            "status": "success",
            "message": "Database connection successful",
            "connection_details": {
                "host": host,
                "database": database,
                "user": user,
                "db_type": db_type,
                "port": port
            }
        }
    elif result["type"] == "select":
        with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt") as temp_file:
            temp_file.write(" | ".join(result["headers"]) + "\n")
            for row in result["results"]:
                temp_file.write(" | ".join(str(item) for item in row) + "\n")
            temp_file_path = temp_file.name

        logger.debug(f"Query executed successfully, results written to {temp_file_path}")
        return FileResponse(path=temp_file_path, filename="output.txt", media_type='text/plain')
    else:
        logger.debug("Non-SELECT query executed successfully")
        return {"status": "Query executed successfully"}

@app.post("/upload-file-custom-db-pg/")
@limiter.limit(RATE_LIMIT)
async def upload_file_custom_db_pg(
    request: Request,
    host: str,
    database: str,
    user: str,
    password: str,
    file: UploadFile = File(...),
    schema: str = "public",
    port: int = 5432
) -> Dict[str, Any]:
    """Handles file uploads using LLM for schema detection and creates PostgreSQL table in a custom database."""
    import os as os_module
    import random
    request_id = f"REQ-{random.randint(10000, 99999)}"
    overall_start_time = datetime.now()
    logger.info(f"[{request_id}] [TIMER] Starting LLM-assisted file upload process for file: {file.filename} to custom PostgreSQL database at {overall_start_time}")
    logger.info(f"[{request_id}] Worker PID: {os_module.getpid()}")
    
    try:
        # Detect file size reliably from request headers
        file_size_bytes = 0
        content_length = request.headers.get("content-length")
        if content_length:
            # Content-length includes form data overhead, so actual file is smaller
            # Rough estimate: actual file is ~70-80% of content-length for multipart
            estimated_file_size = int(content_length) * 0.75
            file_size_bytes = estimated_file_size
        
        # Define size threshold: 100MB = 100 * 1024 * 1024 bytes
        SIZE_THRESHOLD = 100 * 1024 * 1024  # 100MB
        is_gz_file = file.filename and file.filename.lower().endswith('.gz')

        # For .gz files, ALWAYS use chunked method (compressed files expand significantly)
        if is_gz_file:
            use_fast_method = False
            logger.info(f"[{request_id}] Estimated file size: {file_size_bytes/1024/1024:.2f}MB, Using chunked method (compressed .gz file)")
        else:
            use_fast_method = file_size_bytes < SIZE_THRESHOLD and file_size_bytes > 0
            logger.info(f"[{request_id}] Estimated file size: {file_size_bytes/1024/1024:.2f}MB, Using {'fast' if use_fast_method else 'chunked'} method")

        # Start timing file processing
        file_processing_start = datetime.now()
        logger.info(f"[{request_id}] [TIMER] Starting file processing at {file_processing_start}")
        
        if use_fast_method:
            # FAST METHOD: Original memory-based approach for small uncompressed files < 100MB
            # Note: .gz files always use chunked method, so no decompression logic needed here
            try:
                contents = await file.read()
                buffer = StringIO(contents.decode('utf-8'))
                logger.info(f"Successfully read file contents using fast method, size: {len(contents)} bytes")
                
                # Detect delimiter
                first_line = contents.decode('utf-8').split('\n')[0]
                delimiter = detect_delimiter(first_line)
                
                # Get first 5 lines for schema analysis
                lines = contents.decode('utf-8').split('\n')[:5]
                sample_data = '\n'.join(lines)
                
            except UnicodeDecodeError as e:
                logger.error(f"File decoding error: {str(e)}")
                raise HTTPException(
                    status_code=400,
                    detail=f"File encoding error: {str(e)}. Please ensure the file is UTF-8 encoded."
                )
            except Exception as e:
                logger.error(f"File reading error: {str(e)}")
                raise HTTPException(
                    status_code=400,
                    detail=f"File reading error: {str(e)}"
                )
        else:
            # CHUNKED METHOD: Memory-efficient approach for files >= 100MB
            temp_file_path = None
            try:
                # Save upload to temp file using async I/O
                disk_write_start = datetime.now()
                temp_file_path = await save_upload_to_temp_file(file)

                # Calculate write metrics
                bytes_written = os.path.getsize(temp_file_path)
                disk_write_end = datetime.now()
                disk_write_duration = (disk_write_end - disk_write_start).total_seconds()
                mb_written = bytes_written / (1024 * 1024)
                write_speed = mb_written / disk_write_duration if disk_write_duration > 0 else 0
                logger.info(f"[{request_id}] [TIMER] Disk write completed: {mb_written:.2f}MB in {disk_write_duration:.2f}s ({write_speed:.1f} MB/s)")
                logger.info(f"[{request_id}] File saved to temporary location: {temp_file_path}")

                # Handle .gz decompression if needed
                temp_file_path, is_compressed = await handle_file_decompression(file, temp_file_path)

                # Read first 5 lines for schema detection using async I/O
                first_lines = await read_first_lines(temp_file_path, 5)

                # Detect delimiter from first line
                first_line = first_lines[0] if first_lines else ''
                delimiter = detect_delimiter(first_line)

                # Prepare sample data for schema analysis
                sample_data = '\n'.join(first_lines)
                
            except UnicodeDecodeError as e:
                logger.error(f"File decoding error: {str(e)}")
                raise HTTPException(
                    status_code=400,
                    detail=f"File encoding error: {str(e)}. Please ensure the file is UTF-8 encoded."
                )
            except Exception as e:
                logger.error(f"[{request_id}] File processing error: {str(e)}")
                raise HTTPException(
                    status_code=400,
                    detail=f"File processing error: {str(e)}"
                )

        # Log file processing completion
        file_processing_end = datetime.now()
        file_processing_duration = (file_processing_end - file_processing_start).total_seconds()
        logger.info(f"[{request_id}] [TIMER] File processing completed in {file_processing_duration:.2f} seconds")

        # Start timing schema detection
        schema_start = datetime.now()
        logger.info(f"[{request_id}] [TIMER] Starting schema detection at {schema_start}")

        # Get schema from OpenAI
        try:
            parsed_schema = await get_schema_from_openai(sample_data, delimiter, "postgresql")
            logger.info(f"[{request_id}] Received schema suggestion from OpenAI")

            if 'columns' not in parsed_schema:
                raise ValueError("Schema does not contain 'columns' key")

            # Log column count and names for debugging
            logger.info(f"[{request_id}] LLM detected {len(parsed_schema['columns'])} columns")

        except Exception as e:
            logger.error(f"[{request_id}] Failed to get schema from OpenAI: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get schema from OpenAI: {str(e)}"
            )

        # Log schema detection completion
        schema_end = datetime.now()
        schema_duration = (schema_end - schema_start).total_seconds()
        logger.info(f"[{request_id}] [TIMER] Schema detection completed in {schema_duration:.2f} seconds")

        # Generate table name
        table_name = generate_table_name(os.path.splitext(file.filename)[0])
        logger.info(f"[{request_id}] Generated table name: {table_name}")

        # Start timing database operations
        db_start = datetime.now()
        logger.info(f"[{request_id}] [TIMER] Starting database operations at {db_start}")

        # Connect to custom PostgreSQL database using connection pool
        pool_key = None
        try:
            connection, pool_key = await pg_pool_manager.get_connection(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            cursor = connection.cursor()
            logger.info(f"[{request_id}] [POOL] Got connection from pool for {host}:{database}")
            
        except psycopg2.OperationalError as e:
            logger.error(f"Database connection error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to connect to database: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected database connection error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected database connection error: {str(e)}"
            )

        try:
            # Prepare columns SQL
            columns_sql = []
            for col in parsed_schema['columns']:
                col_name = sanitize_column_name(col['name'])
                columns_sql.append(f'"{col_name}" {col["type"]}')

            # Start timing data insertion
            insertion_start = datetime.now()
            logger.info(f"[TIMER] Starting data insertion at {insertion_start}")

            if use_fast_method:
                logger.info("[TIMER] Using fast method - direct StringIO to COPY")
            else:
                logger.info(f"[{request_id}] [TIMER] Using chunked method - temp file to COPY")

            # Run blocking database operations in thread pool
            loop = asyncio.get_event_loop()
            final_count = await loop.run_in_executor(
                None,
                partial(
                    _pg_custom_db_operations_sync,
                    connection,
                    cursor,
                    table_name,
                    schema,
                    columns_sql,
                    delimiter,
                    temp_file_path if not use_fast_method else None,
                    buffer if use_fast_method else None
                )
            )

            # Log data insertion completion
            insertion_end = datetime.now()
            insertion_duration = (insertion_end - insertion_start).total_seconds()
            logger.info(f"[{request_id}] [TIMER] Data insertion (COPY) completed in {insertion_duration:.2f} seconds")

            # Log database operations completion
            db_end = datetime.now()
            db_duration = (db_end - db_start).total_seconds()
            logger.info(f"[{request_id}] [TIMER] Database operations completed in {db_duration:.2f} seconds")

            # Log overall completion
            overall_end = datetime.now()
            overall_duration = (overall_end - overall_start_time).total_seconds()
            logger.info(f"[{request_id}] [TIMER] *** OVERALL COMPLETION: {final_count} rows processed in {overall_duration:.2f} seconds ({final_count/overall_duration:.0f} rows/sec) ***")
            logger.info(f"[{request_id}] [TIMER] Breakdown - File: {file_processing_duration:.2f}s, Schema: {schema_duration:.2f}s, Database: {db_duration:.2f}s")
            
            return {
                "status": "success",
                "message": f"File uploaded and {final_count} rows inserted in {overall_duration:.2f} seconds",
                "table_name": f"{schema}.{table_name}",
                "rows_inserted": final_count,
                "duration_seconds": overall_duration,
                "rows_per_second": final_count / overall_duration if overall_duration > 0 else 0,
                "columns": parsed_schema['columns'],
                "llm_schema_used": True,
                "connection_details": {
                    "host": host,
                    "database": database,
                    "schema": schema,
                    "user": user
                }
            }

        except psycopg2.Error as e:
            logger.error(f"[{request_id}] PostgreSQL error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Database operation failed: {str(e)}"
            )
        finally:
            if 'cursor' in locals():
                cursor.close()
            # Return connection to pool instead of closing
            if 'connection' in locals() and pool_key:
                pg_pool_manager.put_connection(connection, pool_key)
                logger.info(f"[{request_id}] [POOL] Connection returned to pool")
            # Clean up temporary file
            if 'temp_file_path' in locals() and temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Temporary file {temp_file_path} cleaned up successfully")
                except Exception as cleanup_error:
                    logger.error(f"Error cleaning up temporary file: {cleanup_error}")
            
    except HTTPException:
        raise  # Re-raise HTTP exceptions as they already have proper error messages
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}", exc_info=True)  # Added exc_info=True for full traceback
        # Clean up temporary file in case of error
        if 'temp_file_path' in locals() and temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.info(f"Temporary file {temp_file_path} cleaned up after error")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary file after error: {cleanup_error}")
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )

@app.post("/upload-file-custom-db-mysql/")
@limiter.limit(RATE_LIMIT)
async def upload_file_custom_db_mysql(
    request: Request,
    host: str,
    database: str,
    user: str,
    password: str,
    file: UploadFile = File(...),
    port: int = 3306,
    sslmode: str = None
) -> Dict[str, Any]:
    """Handles file uploads using LLM for schema detection and creates MySQL table in a custom database."""
    overall_start_time = datetime.now()
    logger.info(f"[TIMER] Starting LLM-assisted file upload process for file: {file.filename} to custom MySQL database at {overall_start_time}")

    # Start timing file processing
    file_processing_start = datetime.now()
    logger.info(f"[TIMER] Starting file processing at {file_processing_start}")

    # Save file to temporary location for memory-efficient processing
    temp_file_path = None
    try:
        # Save upload to temp file using async I/O
        temp_file_path = await save_upload_to_temp_file(file)
        logger.info(f"File saved to temporary location: {temp_file_path}")

        # Handle .gz decompression if needed
        temp_file_path, is_compressed = await handle_file_decompression(file, temp_file_path)

        # Read first 5 lines for schema detection using async I/O
        first_lines = await read_first_lines(temp_file_path, 5)

        # Detect delimiter from first line
        first_line = first_lines[0] if first_lines else ''
        delimiter = detect_delimiter(first_line)

        # Prepare sample data for schema analysis
        sample_data = '\n'.join(first_lines)

        # Log file processing completion
        file_processing_end = datetime.now()
        file_processing_duration = (file_processing_end - file_processing_start).total_seconds()
        logger.info(f"[TIMER] File processing completed in {file_processing_duration:.2f} seconds")

        # Start timing schema detection
        schema_start = datetime.now()
        logger.info(f"[TIMER] Starting schema detection at {schema_start}")
        
        # Get schema from OpenAI
        try:
            parsed_schema = await get_schema_from_openai(sample_data, delimiter, "mysql")
            logger.info(f"Received schema suggestion from OpenAI: {parsed_schema}")
            
            if 'columns' not in parsed_schema:
                raise ValueError("Schema does not contain 'columns' key")
                
        except Exception as e:
            logger.error(f"Failed to get schema from OpenAI: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get schema from OpenAI: {str(e)}"
            )

        # Log schema detection completion
        schema_end = datetime.now()
        schema_duration = (schema_end - schema_start).total_seconds()
        logger.info(f"[TIMER] Schema detection completed in {schema_duration:.2f} seconds")

        # Start timing database operations
        db_start = datetime.now()
        logger.info(f"[TIMER] Starting database operations at {db_start}")

        # Generate table name
        table_name = generate_table_name(os.path.splitext(file.filename)[0])
        logger.info(f"Generated table name: {table_name}")

        try:
            # Prepare columns SQL
            columns_sql = []
            for col in parsed_schema['columns']:
                col_name = sanitize_column_name(col['name'])
                columns_sql.append(f"`{col_name}` {col['type']}")

            # Run blocking MySQL + Polars operations in thread pool
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                partial(
                    _mysql_create_table_and_insert_sync,
                    host,
                    user,
                    password,
                    database,
                    port,
                    table_name,
                    columns_sql,
                    temp_file_path,
                    delimiter,
                    25000,   # chunk_size (reduced for cloud MySQL stability)
                    sslmode,
                    False    # disable_binlog - may not have permission on custom db
                )
            )

            final_count = result["final_count"]

            # Log database operations completion
            db_end = datetime.now()
            db_duration = (db_end - db_start).total_seconds()
            logger.info(f"[TIMER] Database operations completed in {db_duration:.2f} seconds")

            # Log overall completion
            overall_end = datetime.now()
            overall_duration = (overall_end - overall_start_time).total_seconds()
            logger.info(f"[TIMER] *** OVERALL COMPLETION: {final_count} rows processed in {overall_duration:.2f} seconds ({final_count/overall_duration:.0f} rows/sec) ***")
            logger.info(f"[TIMER] Breakdown - File: {file_processing_duration:.2f}s, Schema: {schema_duration:.2f}s, Database: {db_duration:.2f}s")

            return {
                "status": "success",
                "message": f"File uploaded and {final_count} rows inserted in {overall_duration:.2f} seconds",
                "table_name": table_name,
                "rows_inserted": final_count,
                "duration_seconds": overall_duration,
                "rows_per_second": final_count / overall_duration if overall_duration > 0 else 0,
                "columns": parsed_schema['columns'],
                "llm_schema_used": True,
                "connection_details": {
                    "host": host,
                    "database": database,
                    "user": user
                }
            }

        finally:
            # Clean up temporary file
            if 'temp_file_path' in locals() and temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Temporary file {temp_file_path} cleaned up successfully")
                except Exception as cleanup_error:
                    logger.error(f"Error cleaning up temporary file: {cleanup_error}")
            
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        # Clean up temporary file in case of error
        if 'temp_file_path' in locals() and temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
                logger.info(f"Temporary file {temp_file_path} cleaned up after error")
            except Exception as cleanup_error:
                logger.error(f"Error cleaning up temporary file after error: {cleanup_error}")
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )

# Add this function near the top of the file, after the imports
async def try_openrouter_fallback(request_body: dict) -> Dict[str, Any]:
    """Helper function to try the OpenRouter fallback"""
    logger.warning("Falling back to OpenRouter")
    
    # Create OpenRouter request
    openrouter_request = request_body.copy()
    fallback_model = os.getenv('OPENROUTER_MODEL', 'google/gemini-2.0-flash-001')
    openrouter_request["model"] = fallback_model
    
    # Get OpenRouter URL and API key
    openrouter_url = os.getenv('OPENROUTER_URL', 'https://openrouter.ai/api/v1/chat/completions')
    openrouter_api_key = os.getenv('OPENROUTER_API_KEY')
    
    if not openrouter_api_key:
        raise HTTPException(
            status_code=500,
            detail="OpenRouter API key not configured for fallback"
        )
    
    # Make request to OpenRouter
    async with httpx.AsyncClient() as client:
        openrouter_response = await client.post(
            openrouter_url,
            headers={
                "Authorization": f"Bearer {openrouter_api_key}",
                "Content-Type": "application/json"
            },
            json=openrouter_request,
            timeout=60
        )
        
        if not openrouter_response.is_success:
            raise HTTPException(
                status_code=500,
                detail=f"OpenRouter API error: {openrouter_response.text}"
            )
        
        openrouter_data = openrouter_response.json()
        logger.info(f"Fallback to OpenRouter successful with status code: {openrouter_response.status_code}")
        return openrouter_data

async def get_schema_from_openai(sample_data: str, delimiter: str, db_type: str = "postgresql") -> Dict[str, Any]:
    """
    Get schema suggestion from OpenAI via proxy server.
    """
    # Determine the system message based on database type
    if db_type.lower() == "postgresql":
        system_message = """You are a PostgreSQL schema generator. Analyze the data and output ONLY a JSON schema.
        Use standard PostgreSQL types (TEXT, NUMERIC, DATE, TIMESTAMP. Don't use INTEGER).
        Column names must be SQL-safe (lowercase, alphanumeric and underscores only).
        Convert dots to underscores (e.g. 'COLUMN.1' becomes 'column_1').
        Use NUMERIC for all values that can have decimal points. Don't use INTEGER.
        Use DATE or TIMESTAMP for dates. Use TEXT when unsure."""
    else:  # mysql
        system_message = """You are a MySQL schema generator. Analyze the data and output ONLY a JSON schema.
        Use standard MySQL types (VARCHAR(255), DECIMAL(20,6), DATE, TIMESTAMP. Don't use INT).
        Column names must be SQL-safe (lowercase, alphanumeric and underscores only).
        Convert dots to underscores (e.g. 'COLUMN.1' becomes 'column_1').
        Use DECIMAL(20,6) for numeric values that can have decimal points. Don't use INT.
        Use VARCHAR(255) for text fields. Use DATE for dates.
        Use TIMESTAMP for datetime fields. Use VARCHAR(255) when in doubt."""

    request_body = {
        "model": os.getenv("RT_MODEL", "gpt-4.1"),
        "messages": [
            {
                "role": "system",
                "content": system_message
            },
            {
                "role": "user",
                "content": f"""Analyze this data sample and provide a JSON schema.
                Data sample (delimiter: '{delimiter}'):
                {sample_data}
                
                Required output format:
                {{
                    "columns": [
                        {{"name": "column_name", "type": "sql_type", "description": "brief description"}}
                    ]
                }}
                
                Output only the JSON schema, nothing else."""
            }
        ],
        "response_format": {"type": "json_object"}
    }

    rt_endpoint = os.getenv("RT_ENDPOINT")
    if not rt_endpoint:
        raise ValueError("RT_ENDPOINT environment variable not set")

    try:
        # Try OpenAI first
        async with aiohttp.ClientSession() as session:
            async with session.post(
                rt_endpoint,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                json=request_body
            ) as response:
                if not response.ok:
                    logger.warning(f"OpenAI API failed with status {response.status}, trying OpenRouter fallback")
                    # Try OpenRouter fallback
                    response_data = await try_openrouter_fallback(request_body)
                else:
                    response_data = await response.json()
                    logger.debug(f"OpenAI API response: {response_data}")
                
                try:
                    schema_content = response_data["choices"][0]["message"]["content"]
                    return json.loads(schema_content)
                except (KeyError, json.JSONDecodeError) as e:
                    logger.error(f"Failed to parse API response: {e}")
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to parse schema from API response: {str(e)}"
                    )
    
    except HTTPException as e:
        # Re-raise HTTPException from fallback
        raise e
    except Exception as e:
        logger.error(f"Unexpected error in schema detection: {str(e)}")
        # Try OpenRouter fallback as last resort
        try:
            logger.warning("Trying OpenRouter fallback due to unexpected error")
            response_data = await try_openrouter_fallback(request_body)
            schema_content = response_data["choices"][0]["message"]["content"]
            return json.loads(schema_content)
        except Exception as fallback_error:
            logger.error(f"OpenRouter fallback also failed: {str(fallback_error)}")
            raise HTTPException(
                status_code=500,
                detail=f"Both OpenAI and OpenRouter failed: {str(e)}"
            )

def stream_db_query(cursor, connection, delimiter=" | ", chunk_size=10000):
    """Stream database query results in chunks and close connection when done."""
    try:
        # Header row
        yield delimiter.join([desc[0] for desc in cursor.description]) + "\n"

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                yield delimiter.join(str(item) if item is not None else "" for item in row) + "\n"
    finally:
        # Clean up resources when streaming is complete
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
            logger.debug("Database connection closed after streaming completed")
        except Exception as cleanup_error:
            logger.warning(f"Cleanup error during streaming: {cleanup_error}")

def _connect_db_export_setup(host: str, database: str, user: str, password: str, port: int, db_type: str, table_name: str):
    """Synchronous helper for export connection setup - runs in thread pool."""
    if db_type.lower() == "mysql":
        connection = mysql.connector.connect(
            host=host, database=database, user=user, password=password, port=port, charset='utf8mb4'
        )
    elif db_type.lower() == "postgresql":
        connection = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
    else:
        raise ValueError("Invalid database type. Supported types: 'mysql', 'postgresql'.")

    cursor = connection.cursor()

    # Set query timeout
    if db_type.lower() == "postgresql":
        cursor.execute("SET statement_timeout = 600000")  # 10 minutes
    elif db_type.lower() == "mysql":
        cursor.execute("SET SESSION MAX_EXECUTION_TIME = 600000")  # 10 minutes

    sqlquery = f"SELECT * FROM {table_name}"
    cursor.execute(sqlquery)

    return cursor, connection

@app.get("/connect-db-export/")
@limiter.limit(RATE_LIMIT)
async def connect_db_export(
    request: Request,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 3306,
    db_type: str = "mysql",
    table: str = None,
):
    """Stream database table contents as a text file."""
    logger.debug(f"Received API call: {request.url}")
    logger.info(f"Request to /connect-db-export/ from Origin: {request.headers.get('origin')}")
    logger.info(f"Attempting to connect to {db_type} database at {host}")

    if not table:
        raise HTTPException(status_code=400, detail="Parameter 'table' is required.")

    # Sanitize table name to prevent SQL injection
    table_name = ''.join(c for c in table if c.isalnum() or c in ['_', '.'])

    try:
        # Run blocking connection setup in thread pool
        loop = asyncio.get_event_loop()
        cursor, connection = await loop.run_in_executor(
            None,
            partial(_connect_db_export_setup, host, database, user, password, port, db_type, table_name)
        )
        logger.info("Database connection established successfully")
        logger.debug(f"Executing query: SELECT * FROM {table_name}")

        filename = f"{table_name}_export.txt"

        # Return streaming response - connection will be closed by the generator
        return StreamingResponse(
            stream_db_query(cursor, connection),
            media_type="text/plain",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/plain; charset=utf-8"
            }
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except (mysql.connector.Error, psycopg2.Error) as e:
        logger.error(f"Database error during export: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during export: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Export failed: {str(e)}")

# Shutdown event handler to clean up connection pools
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("[POOL] Shutting down - closing all connection pools")
    pg_pool_manager.close_all_pools()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, timeout=900)

