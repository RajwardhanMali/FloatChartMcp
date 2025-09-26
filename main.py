#!/usr/bin/env python3
"""
FloatChat FastMCP Server for Argo Ocean Data

A simplified MCP server using FastMCP that provides database introspection
and querying tools for FloatChat to interact with Argo ocean data.

Usage:
    python http_mcp_server.py
"""

import asyncio
import logging
import os
import re
from typing import Any, Dict, Optional

import asyncpg
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastMCP instance
mcp = FastMCP("FloatChat-Argo",
              port=8050,
    stateless_http=True)

# Global database pool
db_pool: Optional[asyncpg.Pool] = None


async def ensure_connection():
    """Ensure database connection is established."""
    global db_pool
    if not db_pool:
        db_url = os.getenv("NEON_DB_URL")
        if not db_url:
            raise ValueError("NEON_DB_URL environment variable is required")

        try:
            db_pool = await asyncpg.create_pool(
                db_url,
                min_size=1,
                max_size=10,
                command_timeout=60
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            raise


def validate_query_safety(query: str) -> bool:
    """Validate that the query is safe (read-only)."""
    # Remove comments and normalize whitespace
    cleaned_query = re.sub(r'--.*?\n', '', query, flags=re.MULTILINE)
    cleaned_query = re.sub(r'/\*.*?\*/', '', cleaned_query, flags=re.DOTALL)
    cleaned_query = ' '.join(cleaned_query.split()).upper()

    # Check for dangerous operations
    dangerous_keywords = [
        'DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE',
        'TRUNCATE', 'REPLACE', 'MERGE', 'CALL', 'EXEC', 'EXECUTE',
        'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'SAVEPOINT'
    ]

    for keyword in dangerous_keywords:
        if keyword in cleaned_query:
            return False

    # Must start with SELECT or WITH (for CTEs)
    if not (cleaned_query.strip().startswith('SELECT') or cleaned_query.strip().startswith('WITH')):
        return False

    return True


@mcp.tool()
async def run_query(query: str) -> Dict[str, Any]:
    """Execute a safe, read-only SQL query on the Argo database. Only SELECT statements are allowed."""
    await ensure_connection()

    # Validate query safety
    if not validate_query_safety(query):
        raise ValueError(
            "Query not allowed. Only SELECT statements (and WITH clauses) are permitted. "
            "DROP, DELETE, UPDATE, INSERT, ALTER, CREATE, TRUNCATE, and other "
            "modification operations are not allowed."
        )

    # Limit query length for safety
    if len(query) > 10000:
        raise ValueError("Query too long. Maximum length is 10,000 characters.")

    async with db_pool.acquire() as conn:
        try:
            # Set a statement timeout for safety
            await conn.execute("SET statement_timeout = '60s'")

            rows = await conn.fetch(query)

            if rows:
                result = [dict(row) for row in rows]
                return {
                    "query": query,
                    "row_count": len(result),
                    "data": result,
                    "success": True
                }
            else:
                return {
                    "query": query,
                    "row_count": 0,
                    "data": [],
                    "message": "Query executed successfully but returned no results",
                    "success": True
                }

        except asyncpg.PostgresError as e:
            logger.error(f"PostgreSQL error in query: {e}")
            raise ValueError(f"Database error: {str(e)}")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise


@mcp.tool()
async def list_tables() -> Dict[str, Any]:
    """Returns all table names in the database"""
    await ensure_connection()

    query = """
            SELECT schemaname, \
                   tablename, \
                   tableowner, \
                   hasindexes, \
                   hasrules, \
                   hastriggers
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename; \
            """

    async with db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(query)
            tables = [dict(row) for row in rows]

            return {
                "tables": tables,
                "table_count": len(tables),
                "table_names": [table["tablename"] for table in tables]
            }
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            raise


@mcp.tool()
async def get_schema(table_name: str) -> Dict[str, Any]:
    """Returns the schema of a given table including columns, types, constraints, and indexes"""
    await ensure_connection()

    # Validate table name to prevent SQL injection
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        raise ValueError("Invalid table name format")

    # Get column information
    columns_query = """
                    SELECT column_name, \
                           data_type, \
                           is_nullable, \
                           column_default, \
                           character_maximum_length, \
                           numeric_precision, \
                           numeric_scale, \
                           ordinal_position
                    FROM information_schema.columns
                    WHERE table_schema = 'public' \
                      AND table_name = $1
                    ORDER BY ordinal_position; \
                    """

    # Get constraints
    constraints_query = """
                        SELECT tc.constraint_name, \
                               tc.constraint_type, \
                               kcu.column_name, \
                               ccu.table_name  AS foreign_table_name, \
                               ccu.column_name AS foreign_column_name
                        FROM information_schema.table_constraints tc
                                 LEFT JOIN information_schema.key_column_usage kcu
                                           ON tc.constraint_name = kcu.constraint_name
                                 LEFT JOIN information_schema.constraint_column_usage ccu
                                           ON ccu.constraint_name = tc.constraint_name
                        WHERE tc.table_schema = 'public' \
                          AND tc.table_name = $1; \
                        """

    async with db_pool.acquire() as conn:
        try:
            # Check if table exists
            table_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
                table_name
            )

            if not table_exists:
                return {
                    "error": f"Table '{table_name}' does not exist",
                    "table_name": table_name
                }

            columns = await conn.fetch(columns_query, table_name)
            constraints = await conn.fetch(constraints_query, table_name)

            return {
                "table_name": table_name,
                "columns": [dict(col) for col in columns],
                "constraints": [dict(const) for const in constraints],
                "column_count": len(columns)
            }
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {e}")
            raise


@mcp.tool()
async def describe_database() -> Dict[str, Any]:
    """Returns a comprehensive overview of the database structure with all tables and their column definitions"""
    await ensure_connection()

    # Get all tables with their column information
    query = """
            SELECT t.tablename, \
                   c.column_name, \
                   c.data_type, \
                   c.is_nullable, \
                   c.column_default, \
                   c.character_maximum_length, \
                   c.numeric_precision, \
                   c.numeric_scale, \
                   c.ordinal_position
            FROM pg_tables t
                     LEFT JOIN information_schema.columns c
                               ON t.tablename = c.table_name
                                   AND c.table_schema = 'public'
            WHERE t.schemaname = 'public'
            ORDER BY t.tablename, c.ordinal_position; \
            """

    async with db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(query)

            # Organize data by table
            database_structure = {}
            for row in rows:
                table_name = row['tablename']
                if table_name not in database_structure:
                    database_structure[table_name] = {
                        "columns": [],
                        "column_count": 0
                    }

                if row['column_name']:  # Skip tables without columns (shouldn't happen)
                    column_info = {
                        "name": row['column_name'],
                        "type": row['data_type'],
                        "nullable": row['is_nullable'] == 'YES',
                        "default": row['column_default'],
                        "position": row['ordinal_position']
                    }

                    # Add length/precision info if available
                    if row['character_maximum_length']:
                        column_info['max_length'] = row['character_maximum_length']
                    if row['numeric_precision']:
                        column_info['precision'] = row['numeric_precision']
                    if row['numeric_scale']:
                        column_info['scale'] = row['numeric_scale']

                    database_structure[table_name]["columns"].append(column_info)
                    database_structure[table_name]["column_count"] += 1

            return {
                "database_structure": database_structure,
                "table_count": len(database_structure),
                "total_columns": sum(table["column_count"] for table in database_structure.values())
            }
        except Exception as e:
            logger.error(f"Error describing database: {e}")
            raise


@mcp.tool()
async def get_indexes(table_name: str) -> Dict[str, Any]:
    """Returns index definitions for a given table (useful for query optimization)"""
    await ensure_connection()

    # Validate table name
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
        raise ValueError("Invalid table name format")

    query = """
            SELECT i.indexname, \
                   i.indexdef, \
                   am.amname                                                          as index_method, \
                   idx.indisunique                                                    as is_unique, \
                   idx.indisprimary                                                   as is_primary, \
                   array_agg(a.attname ORDER BY array_position(idx.indkey, a.attnum)) as columns
            FROM pg_indexes i
                     JOIN pg_class c ON c.relname = i.tablename
                     JOIN pg_index idx ON idx.indexrelid = (SELECT oid \
                                                            FROM pg_class \
                                                            WHERE relname = i.indexname)
                     JOIN pg_am am ON am.oid = (SELECT relam \
                                                FROM pg_class \
                                                WHERE oid = idx.indexrelid)
                     JOIN pg_attribute a ON a.attrelid = c.oid
                AND a.attnum = ANY (idx.indkey)
            WHERE i.schemaname = 'public'
              AND i.tablename = $1
            GROUP BY i.indexname, i.indexdef, am.amname, idx.indisunique, idx.indisprimary
            ORDER BY i.indexname; \
            """

    async with db_pool.acquire() as conn:
        try:
            # Check if table exists
            table_exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
                table_name
            )

            if not table_exists:
                return {
                    "error": f"Table '{table_name}' does not exist",
                    "table_name": table_name
                }

            rows = await conn.fetch(query, table_name)
            indexes = [dict(row) for row in rows]

            return {
                "table_name": table_name,
                "indexes": indexes,
                "index_count": len(indexes)
            }
        except Exception as e:
            logger.error(f"Error getting indexes for table {table_name}: {e}")
            raise


async def cleanup():
    """Clean up database connections."""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Database connection pool closed")


if __name__ == "__main__":
    print("Running server with Streamable HTTP transport")
    try:
        mcp.run(transport="streamable-http")
    finally:
        asyncio.run(cleanup())
