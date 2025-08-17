from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Request, Depends, UploadFile, File, status
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi.responses import FileResponse, StreamingResponse
import mysql.connector
import psycopg2
import os
import tempfile
import logging
import io
import random
import string
import pandas as pd
from typing import Dict, Any, List
import aiohttp
from datetime import datetime
import json
import re
from io import StringIO
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
    "https://your-frontend-app.com",
    "https://your-ai-tool.com",
    "http://localhost:1234", # Example for local development
    "http://localhost:5678"  # Example for another local service
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

@app.get("/sqlquery/")
@limiter.limit(RATE_LIMIT)
async def sqlquery(sqlquery: str, cloud: str, request: Request):
    logger.debug(f"Received API call: {request.url} with cloud parameter: {cloud}")
    try:
        connection = get_connection(cloud)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        cursor = connection.cursor()
        cursor.execute(sqlquery)
        
        # Handle queries that return results
        if cursor.description is not None:
            headers = [i[0] for i in cursor.description]
            results = cursor.fetchall()
            cursor.close()

            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt") as temp_file:
                temp_file.write(" | ".join(headers) + "\n")
                for row in results:
                    temp_file.write(" | ".join(str(item) for item in row) + "\n")
                temp_file_path = temp_file.name

            logger.debug(f"Query executed successfully, results written to {temp_file_path}")
            # Return the file response
            response = FileResponse(path=temp_file_path, filename="output.txt", media_type='text/plain')
            return response
        
        # Handle non-SELECT queries
        else:
            connection.commit()
            cursor.close()
            logger.debug("Non-SELECT query executed successfully")
            return {"status": "Query executed successfully"}

    finally:
        connection.close()
        logger.debug("Database connection closed")

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

def generate_table_name(base_name: str) -> str:
    """Generates a sanitized and unique table name based on base_name."""
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
    logger.info(f"Starting LLM-assisted file upload process for file: {file.filename}")
    
    try:
        # Read file in chunks and analyze first chunk for schema
        CHUNK_SIZE = 1024 * 1024  # 1MB chunks
        first_chunk = await file.read(CHUNK_SIZE)
        
        # Ensure proper string decoding with error handling
        try:
            sample_data = first_chunk.decode('utf-8').split('\n')[:5]
        except UnicodeDecodeError:
            # Try with a different encoding if UTF-8 fails
            sample_data = first_chunk.decode('latin-1').split('\n')[:5]
        
        # Detect delimiter from first line
        first_line = sample_data[0]
        delimiter = ',' if ',' in first_line else '|'
        logger.info(f"Detected delimiter: '{delimiter}'")
        
        # Reset file pointer for later reading
        await file.seek(0)
        
        # Convert sample data to string for LLM prompt
        sample_data_str = '\n'.join(sample_data)
        
        # Get schema from OpenAI
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

        # Generate table name and create table
        table_name = generate_table_name(os.path.splitext(file.filename)[0])
        logger.info(f"Generated table name: {table_name}")
        
        # Create Neon connection with error handling
        try:
            connection = create_neon_connection()
            cursor = connection.cursor()
            
            # Create table using LLM-suggested schema
            columns = []
            for col in parsed_schema['columns']:
                col_name = ''.join(c for c in col['name'] if c.isalnum() or c == '_')
                columns.append(f'"{col_name}" {col["type"]}')
            
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS public.{table_name} (
                    {', '.join(columns)}
                )
            """
            cursor.execute(create_table_query)
            logger.info("Table created successfully with LLM-suggested schema")

            # Create a temporary file for efficient COPY
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', newline='', delete=False) as temp_file:
                # Read and write file content in chunks
                while True:
                    chunk = await file.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    try:
                        chunk_str = chunk.decode('utf-8')
                    except UnicodeDecodeError:
                        chunk_str = chunk.decode('latin-1')
                    temp_file.write(chunk_str)
                
                temp_file_path = temp_file.name

            # Use COPY command with the temporary file
            start_time = datetime.now()
            
            with open(temp_file_path, 'r', encoding='utf-8') as f:
                cursor.copy_expert(
                    f"""
                    COPY public.{table_name}
                    FROM STDIN
                    WITH (FORMAT CSV, DELIMITER '{delimiter}', HEADER true)
                    """,
                    f
                )
            
            connection.commit()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Get final count
            cursor.execute(f"SELECT COUNT(*) FROM public.{table_name}")
            final_count = cursor.fetchone()[0]
            
            logger.info(f"Bulk insert completed: {final_count} rows in {duration:.2f} seconds")
            
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
            if 'connection' in locals():
                connection.close()
            # Clean up temporary file
            if 'temp_file_path' in locals():
                try:
                    os.unlink(temp_file_path)
                except Exception as e:
                    logger.error(f"Error removing temp file: {str(e)}")
            logger.info("Database connection closed and temporary files cleaned up")
            
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )

@app.post("/upload-file-llm-mysql/")
@limiter.limit(RATE_LIMIT)
async def upload_file_llm_mysql(
    request: Request,
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    """Handles file uploads using LLM for schema detection and creates MySQL table."""
    logger.info(f"Starting LLM-assisted MySQL file upload process for file: {file.filename}")
    
    try:
        # Read file contents
        contents = await file.read()
        logger.info(f"Successfully read file contents, size: {len(contents)} bytes")

        # Detect delimiter
        first_line = contents.decode('utf-8').split('\n')[0]
        delimiter = ',' if ',' in first_line else '|'
        logger.info(f"Detected delimiter: '{delimiter}'")

        # Get first 5 lines for schema analysis
        lines = contents.decode('utf-8').split('\n')[:5]
        sample_data = '\n'.join(lines)
        
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

        try:
            df = pd.read_csv(
                io.StringIO(contents.decode('utf-8')), 
                delimiter=delimiter
            )
            logger.info(f"Successfully created DataFrame: {len(df)} rows, {len(df.columns)} columns")

            # Generate table name
            table_name = generate_table_name(os.path.splitext(file.filename)[0])
            logger.info(f"Generated table name: {table_name}")
            
            # Connect to MySQL (FILESSIO)
            connection = mysql.connector.connect(
                host=filessio_host,
                user=filessio_user,
                password=filessio_password,
                database=filessio_database,
                port=filessio_port,
                allow_local_infile=True,
                charset='utf8mb4',
                use_pure=False,
                pool_size=32,
                max_allowed_packet=1073741824  # 1GB
            )
            cursor = connection.cursor()
            logger.info("Successfully connected to MySQL database")

            # Add these performance settings right after creating the cursor
            cursor.execute("SET foreign_key_checks = 0")
            cursor.execute("SET unique_checks = 0")
            cursor.execute("SET autocommit = 0")
            cursor.execute("SET sql_log_bin = 0")  # Disable binary logging if possible
            
            # Create table using LLM-suggested schema
            columns = []
            for col in parsed_schema['columns']:
                col_name = ''.join(c for c in col['name'] if c.isalnum() or c == '_')
                columns.append(f"`{col_name}` {col['type']}")
            
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {', '.join(columns)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_query)
            logger.info("Table created successfully with LLM-suggested schema")

            # Modify the batch insert section
            batch_size = 25000  # Increased from 1000
            total_batches = (len(df) + batch_size - 1) // batch_size
            rows_inserted = 0
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                values = [tuple(row) for _, row in batch.iterrows()]
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_query = f"""
                    INSERT INTO `{table_name}` 
                    VALUES ({placeholders})
                """
                cursor.executemany(insert_query, values)
                
                # Commit every 10 batches or at the end
                if (i // batch_size) % 10 == 0 or (i + batch_size) >= len(df):
                    connection.commit()
                
                rows_inserted += len(values)
                logger.info(f"Progress: inserted batch {(i//batch_size)+1}/{total_batches} ({rows_inserted}/{len(df)} rows)")

            # Reset the performance settings before closing
            cursor.execute("SET foreign_key_checks = 1")
            cursor.execute("SET unique_checks = 1")
            cursor.execute("SET autocommit = 1")
            cursor.execute("SET sql_log_bin = 1")  # Re-enable binary logging if it was disabled

            logger.info(f"All data inserted successfully into {table_name}")

            # Verify row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            final_count = cursor.fetchone()[0]
            logger.info(f"Final row count in table: {final_count}")

            return {
                "status": "success",
                "message": "File uploaded and data inserted successfully",
                "table_name": table_name,
                "rows_inserted": rows_inserted,
                "final_row_count": final_count,
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

    except Exception as e:
        logger.error(f"Unexpected error during file upload: {str(e)}")
        logger.exception("Full traceback:")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )

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

    connection = None
    try:
        # Create connection based on database type
        if db_type.lower() == "mysql":
            connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
        elif db_type.lower() == "postgresql":
            connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid database type. Supported types are 'mysql' and 'postgresql'"
            )

        logger.info("Database connection established successfully")

        # If no query provided, just test connection and return success
        if not sqlquery:
            if connection:
                connection.close()
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

        # Execute query
        try:
            cursor = connection.cursor()
            cursor.execute(sqlquery)
            
            # Handle queries that return results
            if cursor.description is not None:
                headers = [i[0] for i in cursor.description]
                results = cursor.fetchall()
                cursor.close()

                # Create a temporary file
                with tempfile.NamedTemporaryFile(delete=False, mode='w', suffix=".txt") as temp_file:
                    temp_file.write(" | ".join(headers) + "\n")
                    for row in results:
                        temp_file.write(" | ".join(str(item) for item in row) + "\n")
                    temp_file_path = temp_file.name

                logger.debug(f"Query executed successfully, results written to {temp_file_path}")
                # Return the file response
                response = FileResponse(path=temp_file_path, filename="output.txt", media_type='text/plain')
                return response
            
            # Handle non-SELECT queries
            else:
                connection.commit()
                cursor.close()
                logger.debug("Non-SELECT query executed successfully")
                return {"status": "Query executed successfully"}

        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Query execution failed: {str(e)}"
            )
        finally:
            if 'cursor' in locals():
                cursor.close()

    except (mysql.connector.Error, psycopg2.Error) as e:
        logger.error(f"Database connection error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Database connection failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )
    finally:
        if connection:
            try:
                connection.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")

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
    logger.info(f"Starting LLM-assisted file upload process for file: {file.filename} to custom PostgreSQL database")
    
    try:
        # Read file contents
        try:
            contents = await file.read()
            buffer = StringIO(contents.decode('utf-8'))
            logger.info(f"Successfully read file contents, size: {len(contents)} bytes")
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

        # Detect delimiter
        try:
            first_line = contents.decode('utf-8').split('\n')[0]
            delimiter = ',' if ',' in first_line else '|'
            logger.info(f"Detected delimiter: '{delimiter}'")
        except Exception as e:
            logger.error(f"Delimiter detection error: {str(e)}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to detect delimiter: {str(e)}"
            )

        # Get first 5 lines for schema analysis
        lines = contents.decode('utf-8').split('\n')[:5]
        sample_data = '\n'.join(lines)
        
        # Get schema from OpenAI
        try:
            parsed_schema = await get_schema_from_openai(sample_data, delimiter, "postgresql")
            logger.info(f"Received schema suggestion from OpenAI: {parsed_schema}")
            
            if 'columns' not in parsed_schema:
                raise ValueError("Schema does not contain 'columns' key")
                
        except Exception as e:
            logger.error(f"Failed to get schema from OpenAI: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get schema from OpenAI: {str(e)}"
            )

        # Generate table name
        table_name = generate_table_name(os.path.splitext(file.filename)[0])
        logger.info(f"Generated table name: {table_name}")
        
        # Connect to custom PostgreSQL database
        try:
            connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
                options='-c statement_timeout=900000',  # 15 minutes
                connect_timeout=30  # 30 seconds connection timeout
            )
            cursor = connection.cursor()
            logger.info("Successfully connected to PostgreSQL database")
            
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
            # Create schema if it doesn't exist
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            
            # Create table using LLM-suggested schema
            columns = []
            for col in parsed_schema['columns']:
                col_name = ''.join(c for c in col['name'] if c.isalnum() or c == '_')
                columns.append(f'"{col_name}" {col["type"]}')
            
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                    {', '.join(columns)}
                )
            """
            cursor.execute(create_table_query)
            logger.info("Table created successfully with LLM-suggested schema")

            # Use COPY command for bulk insert
            buffer.seek(0)  # Reset buffer to start
            start_time = datetime.now()
            
            cursor.copy_expert(
                f"""
                COPY {schema}.{table_name}
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER '{delimiter}', HEADER true)
                """,
                buffer
            )
            
            connection.commit()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Get final count
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
            final_count = cursor.fetchone()[0]
            
            logger.info(f"Bulk insert completed: {final_count} rows in {duration:.2f} seconds")
            
            return {
                "status": "success",
                "message": f"File uploaded and {final_count} rows inserted in {duration:.2f} seconds",
                "table_name": f"{schema}.{table_name}",
                "rows_inserted": final_count,
                "duration_seconds": duration,
                "rows_per_second": final_count / duration if duration > 0 else 0,
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
            logger.error(f"PostgreSQL error: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Database operation failed: {str(e)}"
            )
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
            logger.info("Database connection closed")
            
    except HTTPException:
        raise  # Re-raise HTTP exceptions as they already have proper error messages
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}", exc_info=True)  # Added exc_info=True for full traceback
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
    logger.info(f"Starting LLM-assisted file upload process for file: {file.filename} to custom MySQL database")
    
    try:
        # Read file contents
        contents = await file.read()
        buffer = StringIO(contents.decode('utf-8'))
        logger.info(f"Successfully read file contents, size: {len(contents)} bytes")

        # Detect delimiter
        first_line = contents.decode('utf-8').split('\n')[0]
        delimiter = ',' if ',' in first_line else '|'
        logger.info(f"Detected delimiter: '{delimiter}'")

        # Get first 5 lines for schema analysis
        lines = contents.decode('utf-8').split('\n')[:5]
        sample_data = '\n'.join(lines)
        
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

        # Generate table name
        table_name = generate_table_name(os.path.splitext(file.filename)[0])
        logger.info(f"Generated table name: {table_name}")
        
        try:
            # Connect to custom MySQL database with SSL if specified
            connection_params = {
                "host": host,
                "database": database,
                "user": user,
                "password": password,
                "port": port,
                "allow_local_infile": True
            }
            
            # Add SSL if specified
            if sslmode and sslmode.lower() == 'require':
                connection_params.update({
                    "ssl_verify_cert": True,
                    "ssl_verify_identity": True
                })
                
            connection = mysql.connector.connect(**connection_params)
            cursor = connection.cursor()
            
            # Create table using LLM-suggested schema
            columns = []
            for col in parsed_schema['columns']:
                col_name = ''.join(c for c in col['name'] if c.isalnum() or c == '_')
                columns.append(f"`{col_name}` {col['type']}")
            
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {', '.join(columns)}
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
            cursor.execute(create_table_query)
            logger.info("Table created successfully with LLM-suggested schema")

            # Instead of LOAD DATA LOCAL INFILE, use batch insert since some MySQL servers restrict LOAD DATA
            start_time = datetime.now()
            
            df = pd.read_csv(
                buffer,
                delimiter=delimiter,
                quoting=3  # QUOTE_NONE
            )
            
            # Batch insert
            batch_size = 15000
            total_rows = len(df)
            total_batches = (total_rows + batch_size - 1) // batch_size
            rows_inserted = 0
            
            cursor.execute("SET foreign_key_checks = 0")
            cursor.execute("SET unique_checks = 0")
            cursor.execute("SET autocommit = 0")
            
            try:
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i:min(i + batch_size, total_rows)]
                    values = list(map(tuple, batch.values))
                    cursor.executemany(insert_query, values)
                    
                    rows_inserted += len(values)
                    
                    # Add commit logging
                    if (i // batch_size) % 10 == 0 or (i + batch_size) >= total_rows:
                        connection.commit()
                        logger.info(f"Committed rows to database. Total rows committed so far: {rows_inserted:,}")
                    
                    logger.info(f"Progress: inserted batch {(i//batch_size)+1}/{total_batches} ({rows_inserted:,}/{total_rows:,} rows)")
            
            finally:
                # Final commit with logging
                if rows_inserted % (batch_size * 10) != 0:
                    connection.commit()
                    logger.info(f"Final commit completed. Total rows committed: {rows_inserted:,}")

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Get final count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            final_count = cursor.fetchone()[0]
            
            logger.info(f"Bulk insert completed: {final_count} rows in {duration:.2f} seconds")
            
            return {
                "status": "success",
                "message": f"File uploaded and {final_count} rows inserted in {duration:.2f} seconds",
                "table_name": table_name,
                "rows_inserted": final_count,
                "duration_seconds": duration,
                "rows_per_second": final_count / duration if duration > 0 else 0,
                "columns": parsed_schema['columns'],
                "llm_schema_used": True,
                "connection_details": {
                    "host": host,
                    "database": database,
                    "user": user
                }
            }

        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'connection' in locals():
                connection.close()
            logger.info("Database connection closed")
            
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )

# Add this function near the top of the file, after the imports
async def get_schema_from_openai(sample_data: str, delimiter: str, db_type: str = "postgresql") -> Dict[str, Any]:
    """
    Get schema suggestion from OpenAI via proxy server.
    """
    # Determine the system message based on database type
    if db_type.lower() == "postgresql":
        system_message = """You are a PostgreSQL schema generator. Analyze the data and output ONLY a JSON schema.
        Use standard PostgreSQL types (TEXT, NUMERIC, DATE, TIMESTAMP. Don't use INTEGER).
        Column names must be SQL-safe (alphanumeric and underscores only).
        Use NUMERIC for all values that can have decimal points. Don't use INTEGER.
        Use DATE or TIMESTAMP for dates. Use TEXT when unsure."""
    else:  # mysql
        system_message = """You are a MySQL schema generator. Analyze the data and output ONLY a JSON schema.
        Use standard MySQL types (VARCHAR(255), DECIMAL(20,6), DATE, TIMESTAMP. Don't use INT).
        Column names must be SQL-safe (alphanumeric and underscores only).
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
                raise HTTPException(
                    status_code=500,
                    detail=f"OpenAI API error: {await response.text()}"
                )
            
            response_data = await response.json()
            logger.debug(f"OpenAI API response: {response_data}")
            
            try:
                schema_content = response_data["choices"][0]["message"]["content"]
                return json.loads(schema_content)
            except (KeyError, json.JSONDecodeError) as e:
                logger.error(f"Failed to parse OpenAI response: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to parse schema from OpenAI response: {str(e)}"
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

    connection = None
    cursor = None
    try:
        # Connect to database based on type
        if db_type.lower() == "mysql":
            connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
                charset='utf8mb4'
            )
        elif db_type.lower() == "postgresql":
            connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
                options='-c statement_timeout=600000'  # 10 minutes timeout
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="Invalid database type. Supported types: 'mysql', 'postgresql'."
            )

        logger.info("Database connection established successfully")
        cursor = connection.cursor()

        # Set query timeout
        if db_type.lower() == "postgresql":
            cursor.execute("SET statement_timeout = 600000")  # 10 minutes
        elif db_type.lower() == "mysql":
            cursor.execute("SET SESSION MAX_EXECUTION_TIME = 600000")  # 10 minutes

        # Sanitize table name to prevent SQL injection
        table_name = ''.join(c for c in table if c.isalnum() or c in ['_', '.'])
        sqlquery = f"SELECT * FROM {table_name}"
        logger.debug(f"Executing query: {sqlquery}")
        cursor.execute(sqlquery)

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

    except (mysql.connector.Error, psycopg2.Error) as e:
        logger.error(f"Database error during export: {str(e)}")
        # Clean up on error
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        except Exception:
            pass
        raise HTTPException(
            status_code=500,
            detail=f"Database operation failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error during export: {str(e)}")
        # Clean up on error
        try:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        except Exception:
            pass
        raise HTTPException(
            status_code=500,
            detail=f"Export failed: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, timeout=900)

