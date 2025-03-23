import pyodbc
import os
import time
import random
import traceback
import re
import sys
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
# For logging, PPE built on multiprocessing
from collections import defaultdict
import logging
import logging.handlers
from MergeTracker import *


# MergeToStaging - A Database Migration and Merging ETL tool w/ SQL Server
# Copyright (C) 2025 Jeffrey Hu
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


# Current Ver V8
# Read ImportToStaging for base changes
# Tablewise ProcessPooling for merges (don't add PPE for merges, no chunking needed)
# Chunk big tables
# Added mergetracker (Tracks rows that are being added/merged in on a two-day basis)
# Add Logger, same as V8.1


def configure_logging():
    """
    Root Logger Configuration, for the multiple processes we create, so we can pass the handler to PPE
    """

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear any existing handlers to avoid duplication (Not necessarily needed on init, just good sanitization)
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Add file handler
    file_handler = logging.FileHandler("logfileM.log", mode='w')
    file_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(file_formatter)
    root_logger.addHandler(console_handler)

    return root_logger


def get_week_ago_timestamp():
    # Called get week_ago
    # Changing to only two days for now

    return (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d %H:%M:%S')


def read_table_names(file_path):
    """Reads table names from a file, our txt with all of our staging tables"""
    with open(file_path, 'r') as file:
        return [line.strip() for line in file]


def get_table_info(connection, table_name, source_schema_name, timestamp_col_names=None):
    # Default timestamp columns if none provided
    if timestamp_col_names is None:
        timestamp_col_names = ['TIME_STAMP', 'DATE_ADDED']

    # Format the timestamp columns for SQL IN clause
    timestamp_placeholders = ", ".join(f"'{col}'" for col in timestamp_col_names)

    with connection.cursor() as cursor:
        # Check for timestamp columns
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = '{source_schema_name}'
            AND COLUMN_NAME IN ({timestamp_placeholders})
        """)
        timestamp_columns = [row.COLUMN_NAME for row in cursor.fetchall()]

        # Check for primary key
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
            AND TABLE_NAME = '{table_name}'
            AND TABLE_SCHEMA = '{source_schema_name}'
        """)
        primary_key = [row.COLUMN_NAME for row in cursor.fetchall()]

    return timestamp_columns, primary_key


def generate_create_table_sql(connection, source_table_name, target_table_name,
                              source_schema_name, target_schema_name, contains_image):
    """
    This version splits up the old generate_create_table_sql to two functions (apply_constraints below)
    Helps with time complexity in theory, should trim a few hours off the full import
    and a few minutes off the merge
    Causes a bunch of warnings in merge, doesn't affect time and is good for checking
    """

    columns = []
    with connection.cursor() as cursor:
        # Query to pull SQL dtypes from (older) source server
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?""", (source_table_name, source_schema_name))

        # Handle dtypes, including legacy dtypes
        for row in cursor:
            column_name, data_type, is_nullable, char_max_length, numeric_precision, numeric_scale = row
            sql_type = data_type  # Default to use the original type, can sometimes break things

            if data_type in 'varchar':
                if 9999 >= char_max_length > 0:
                    sql_type = f'varchar({char_max_length})'
                else:
                    sql_type = 'varchar(max)'
            elif data_type == 'decimal':
                sql_type = f'decimal({numeric_precision}, {numeric_scale})'
            elif data_type == 'numeric':
                sql_type = f'numeric({numeric_precision}, {numeric_scale})'
            elif data_type == 'image':
                sql_type = 'varbinary(max)'
                if source_table_name not in contains_image:
                    contains_image.append(source_table_name)
            elif data_type in 'nvarchar':
                if 255 >= char_max_length > 0:
                    sql_type = f'nvarchar({char_max_length})'
                else:
                    sql_type = 'nvarchar(max)'
            elif data_type in ['ntext', 'text']:
                # Convert legacy text types to nvarchar(max) for better support and handling
                # Currently these types are causing issues
                sql_type = 'nvarchar(max)'

            column_def = f"{column_name} {sql_type} {'NULL' if is_nullable == 'YES' else 'NOT NULL'}"
            columns.append(column_def)

        if not columns:
            raise Exception(
                f"No columns found for table {source_table_name} in schema {source_schema_name}. Check table name and schema.")

        create_table_sql = f"CREATE TABLE [{target_schema_name}].[{target_table_name}] (\n    {',\n    '.join(columns)}\n);"

        return create_table_sql


def generate_constraint_sql(connection, source_table_name, target_table_name, source_schema_name, target_schema_name):
    # Alter statement for constraint application
    # Like adding foreign keys at the end but for other constraints with less separate table dependencies
    # Grabs a few other things, like fill_factor, ordinal_position for multi-column constraints
    # For multi-column constraints, also handles them by grouping them into the same type
    constraint_statements = []

    with connection.cursor() as cursor:
        constraint_query = """
            SELECT DISTINCT
                kcu.COLUMN_NAME,
                tc.CONSTRAINT_TYPE,
                tc.CONSTRAINT_NAME,
                OBJECT_NAME(i.object_id) AS TableName,
                i.fill_factor AS Fill_Factor,
                kcu.ORDINAL_POSITION
            FROM
                INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            LEFT JOIN
                sys.indexes i
                ON OBJECT_NAME(i.object_id) = tc.TABLE_NAME
                AND i.is_primary_key = CASE WHEN tc.CONSTRAINT_TYPE = 'PRIMARY KEY' THEN 1 ELSE 0 END
            WHERE
                tc.TABLE_NAME = ? AND tc.TABLE_SCHEMA = ?
            ORDER BY
                tc.CONSTRAINT_NAME,
                kcu.ORDINAL_POSITION
        """

        cursor.execute(constraint_query, (source_table_name, source_schema_name))

        constraint_columns = defaultdict(list)

        for row in cursor:
            column_name, constraint_type, constraint_name, table_name, fill_factor, ordinal_position = row
            constraint_columns[constraint_name].append((column_name, constraint_type, fill_factor))

        for constraint_name, columns_info in constraint_columns.items():
            column_names = list(dict.fromkeys(col[0] for col in columns_info))
            constraint_type = columns_info[0][1]  # All columns in a constraint have the same type
            fill_factor = columns_info[0][2]

            new_constraint_name = f"{target_table_name}_{constraint_name}"

            if constraint_type == 'PRIMARY KEY':
                constraint = (f"ALTER TABLE [{target_schema_name}].[{target_table_name}] "
                              f"ADD CONSTRAINT [{new_constraint_name}] PRIMARY KEY ([{'], ['.join(column_names)}])")
                if fill_factor is not None and fill_factor != 0 and fill_factor != 100:
                    constraint += f" WITH (FILLFACTOR = {fill_factor})"
                constraint_statements.append(constraint)
            elif constraint_type == 'UNIQUE':
                constraint = (f"ALTER TABLE [{target_schema_name}].[{target_table_name}] "
                              f"ADD CONSTRAINT [{new_constraint_name}] UNIQUE ([{'], ['.join(column_names)}])")
                constraint_statements.append(constraint)

    return constraint_statements


def get_all_foreign_keys(connection, schema_name):
    # Just grabs foreign keys/references with their parent table, called for removal in function below
    query = """
    SELECT 
        OBJECT_NAME(f.parent_object_id) AS TableName,
        f.name AS ForeignKeyName
    FROM 
        sys.foreign_keys AS f
    INNER JOIN 
        sys.tables AS t 
        ON t.object_id = f.parent_object_id
    WHERE 
        SCHEMA_NAME(t.schema_id) = ?
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name,))
        return cursor.fetchall()


def remove_all_foreign_keys(connection, schema_name):
    # Removes foreign keys, needs to be called to get rid of constraints during the insert-into process
    foreign_keys = get_all_foreign_keys(connection, schema_name)

    with connection.cursor() as cursor:
        for table_name, fk_name in foreign_keys:
            drop_fk_query = f"""
            ALTER TABLE {schema_name}.{table_name} 
            DROP CONSTRAINT {fk_name}
            """
            logging.info(drop_fk_query)
            try:
                cursor.execute(drop_fk_query)
                logging.info(f"Dropped foreign key {fk_name} from table {table_name}")
            except pyodbc.Error as e:
                logging.info(f"Error dropping foreign key {fk_name} from table {table_name}: {str(e)}")

    connection.commit()


def get_foreign_key_info(connection, schema_name, table_name):
    # Grabs more than the foreign keys for reproducing, including cascade actions
    # and parent/referenced table + column names
    # Called in function below
    query = """
    SELECT 
        OBJECT_NAME(f.parent_object_id) AS TableName,
        COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
        OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName,
        COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName,
        f.name AS ForeignKeyName,
        f.delete_referential_action_desc AS DeleteAction,
        f.update_referential_action_desc AS UpdateAction
    FROM 
        sys.foreign_keys AS f
    INNER JOIN 
        sys.foreign_key_columns AS fc 
        ON f.object_id = fc.constraint_object_id
    INNER JOIN 
        sys.tables AS t 
        ON t.object_id = f.parent_object_id
    WHERE 
        SCHEMA_NAME(t.schema_id) = ? AND OBJECT_NAME(f.parent_object_id) = ?
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        return cursor.fetchall()


def apply_foreign_key_constraints(target_conn, source_conn, table_names, source_schema_name, target_schema_name):
    # Creates the alter table query to add foreign keys and their actions
    for table_name in table_names:
        fk_info = get_foreign_key_info(source_conn, source_schema_name, table_name)

        for fk in fk_info:
            table_name, column_name, ref_table_name, ref_column_name, fk_name, delete_action, update_action = fk

            # Check if the referenced table exists in our copied tables
            if ref_table_name in table_names:
                add_fk_query = f"""
                        ALTER TABLE {target_schema_name}.{table_name} 
                        ADD CONSTRAINT {fk_name} FOREIGN KEY ({column_name}) 
                        REFERENCES {target_schema_name}.{ref_table_name}({ref_column_name})
                        """

                # Add cascade actions if they exist
                if delete_action and delete_action != 'NO_ACTION':
                    add_fk_query += f" ON DELETE {delete_action}"
                if update_action and update_action != 'NO_ACTION':
                    add_fk_query += f" ON UPDATE {update_action}"

                with target_conn.cursor() as cursor:
                    try:
                        cursor.execute(add_fk_query)
                        target_conn.commit()
                        logging.info(f"Added foreign key {fk_name} to table {table_name}")
                    except pyodbc.Error as e:
                        logging.info(f"Error adding foreign key {fk_name} to table {table_name}: {str(e)}")
                        target_conn.rollback()

                        # Attempt to add the constraint with NOCHECK
                        try:
                            nocheck_fk_query = f"""
                            ALTER TABLE {target_schema_name}.{table_name} 
                            WITH NOCHECK ADD CONSTRAINT {fk_name} FOREIGN KEY ({column_name}) 
                            REFERENCES {target_schema_name}.{ref_table_name}({ref_column_name})
                            """

                            # Add cascade actions if they exist
                            if delete_action and delete_action != 'NO_ACTION':
                                nocheck_fk_query += f" ON DELETE {delete_action}"
                            if update_action and update_action != 'NO_ACTION':
                                nocheck_fk_query += f" ON UPDATE {update_action}"

                            cursor.execute(nocheck_fk_query)
                            target_conn.commit()
                            logging.info(f"Added NOCHECK foreign key {fk_name} to table {table_name}")
                        except pyodbc.Error as e2:
                            logging.info(
                                f"Error adding NOCHECK foreign key {fk_name} to table {table_name}: {str(e2)}")
                            target_conn.rollback()


def create_table(source_conn, target_conn, table_name, source_schema_name, target_schema_name, contains_image,
                 timestamp_col_names):
    # Table creation function, grabs DDL from source to create the basic table structure with dtypes
    # Also needs to grab PK and timestamp columns for merge

    timestamp_columns, primary_key = get_table_info(source_conn, table_name, source_schema_name,
                                                    timestamp_col_names=timestamp_col_names)

    with target_conn.cursor() as cursor:
        cursor.execute(f"SELECT OBJECT_ID('{target_schema_name}.{table_name}', 'U')")
        table_exists = cursor.fetchone()[0] is not None

    if timestamp_columns and primary_key:
        if not table_exists:
            create_table_sql = generate_create_table_sql(source_conn, table_name, table_name)
            with target_conn.cursor() as cursor:
                try:
                    cursor.execute(create_table_sql)
                    target_conn.commit()
                    logging.info(f"Created table {table_name} for merge operations.")
                except pyodbc.Error as e:
                    handle_create_table_error(e, table_name, create_table_sql, target_conn)
        else:
            logging.info(f"Table {table_name} exists and will be used for merge operations.")

    else:
        if table_exists:
            drop_sql = f"DROP TABLE {target_schema_name}.{table_name};"
            with target_conn.cursor() as cursor:
                cursor.execute(drop_sql)
                target_conn.commit()
                logging.info(f"Dropped existing table {table_name}.")

        create_table_sql = generate_create_table_sql(source_conn, table_name, table_name, source_schema_name,
                                                     target_schema_name, contains_image)

        with target_conn.cursor() as cursor:
            try:
                cursor.execute(create_table_sql)
                target_conn.commit()
                logging.info(f"Created table {table_name}.")
            except pyodbc.Error as e:
                handle_create_table_error(e, table_name, create_table_sql, target_conn)


def apply_constraint_wrapper(args):
    """
    Wrapper function for applying constraints to a table in a separate process
    Replaces original apply_constraint function (removed in these versions)

    Args:
        args: Tuple containing (table_name, source_conn_str, target_conn_str,
                               source_schema_name, target_schema_name)
    """

    # Unpack args
    table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name = args

    # Set up logging for this process
    process_logger = logging.getLogger()
    process_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    for handler in process_logger.handlers:
        process_logger.removeHandler(handler)

    # Add a file handler that writes to the same log file
    file_handler = logging.FileHandler("logfileM.log", mode='a')
    file_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    process_logger.addHandler(file_handler)

    process_logger.info(f"Starting constraint application for table {table_name}")

    try:
        # Create new connections for this process
        source_conn = pyodbc.connect(source_conn_str)
        target_conn = pyodbc.connect(target_conn_str)

        # Apply constraints
        constraint_sql = generate_constraint_sql(source_conn, table_name, table_name,
                                                 source_schema_name, target_schema_name)

        if not constraint_sql:
            process_logger.info(f'Constraint_sql was empty for {table_name}, no constraint applied')
            return

        process_logger.info(f'Number of constraints for table {table_name}: {len(constraint_sql)}')

        with target_conn.cursor() as cursor:
            for constraint in constraint_sql:
                try:
                    cursor.execute(constraint)
                    target_conn.commit()
                    process_logger.info(f"Added constraint to {table_name}: {constraint[:100]}...")
                except pyodbc.Error as e:
                    target_conn.rollback()
                    process_logger.warning(f"Error applying constraint to {table_name}: {str(e)}")
                    process_logger.warning(f"Failed constraint: {constraint}")
                    raise

        process_logger.info(f"Completed applying all constraints to {table_name}")
        return table_name

    except Exception as e:
        # Catches the raised error from prior code, this is fine for merging since some tables
        # already will have their constraints (from the ImportToStaging call)
        if "already" in str(e).lower():
            process_logger.warning(f"Constraints may already exist for {table_name}: {str(e)}")
            process_logger.debug(traceback.format_exc())
        else:
            # For other errors, raise as errors
            process_logger.error(f"Error in applying constraints to {table_name}: {str(e)}")
            process_logger.error(traceback.format_exc())
            raise
    finally:
        # Close connections
        source_conn.close()
        target_conn.close()


def handle_create_table_error(e, table_name, create_table_sql, target_conn):
    # Handles errors from create_table statement, just to make sure the process isn't stopped
    logging.error(f"Error creating table {table_name}: {str(e)}")
    logging.info(f"Create Table SQL: {create_table_sql}")

    if "There are no primary or candidate keys in the referenced table" in str(e):
        logging.warning(f"Warning: Foreign key constraint issue detected for table {table_name}. "
                        f"This will be handled later in the foreign key application step.")
    else:
        target_conn.rollback()
        raise


def batch_insert(target_conn_str, insert_query, rows, columns):
    # Batch insert, same as ImportToStaging function
    # In the case that there are some columns in source that are not visible (depending on privileges)
    # Thus, the rows need to be filtered to only specified columns

    try:
        target_conn = pyodbc.connect(target_conn_str)
        target_cursor = target_conn.cursor()
        target_cursor.fast_executemany = True

        # Ensure insert_query has the correct number of placeholders
        # (Doesn't matter for this query, matters for batch_merge)
        placeholders = ', '.join(['?' for _ in columns])
        formatted_query = insert_query.format(placeholders)

        # Filter rows to only include specified columns
        filtered_rows = [[row[columns.index(col)] for col in columns] for row in rows]

        target_cursor.executemany(formatted_query, filtered_rows)
        target_conn.commit()

        # return f"Inserted {len(rows)} rows"
        # print(f"Inserted {len(filtered_rows)} rows")

    except pyodbc.Error as e:
        logging.error(f"Error in batch_insert: {e}")
        logging.error(traceback.format_exc())
        target_conn.rollback()
    finally:
        target_cursor.close()
        target_conn.close()


def get_row_count(connection, schema_name, table_name):
    """Gets the approximate row count of a table from sys"""
    query = f"""
        SELECT SUM(p.rows) AS row_count
        FROM sys.partitions p
        JOIN sys.tables t ON p.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE p.index_id IN (0, 1) 
          AND s.name = ?
          AND t.name = ?
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        row = cursor.fetchone()
        return row[0] if row and row[0] is not None else 0


def get_table_primary_key(connection, schema_name, table_name):
    """Get the primary key column(s) for a table"""
    query = """
    SELECT 
        c.name AS column_name
    FROM 
        sys.indexes i
    INNER JOIN 
        sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    INNER JOIN 
        sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
    INNER JOIN 
        sys.tables t ON i.object_id = t.object_id
    INNER JOIN 
        sys.schemas s ON t.schema_id = s.schema_id
    WHERE 
        i.is_primary_key = 1
        AND s.name = ?
        AND t.name = ?
    ORDER BY 
        ic.key_ordinal
    """

    with connection.cursor() as cursor:
        cursor.execute(query, (schema_name, table_name))
        primary_keys = [row[0] for row in cursor.fetchall()]

    return primary_keys


def get_guid_boundaries(connection, schema_name, table_name, primary_key, num_chunks):
    """
        Get boundary values for GUID-based chunking
        For GUID/uniqueidentifier columns, we need evenly spaced values
        We approximate the chunks and chunksize, so every chunk should be the same size for even work distribution
    """
    query = f"""
    WITH OrderedRows AS (
        SELECT {primary_key}, ROW_NUMBER() OVER (ORDER BY {primary_key}) AS RowNum
        FROM {schema_name}.{table_name} WITH (NOLOCK)
    )
    SELECT {primary_key}
    FROM OrderedRows
    WHERE RowNum % @chunk_size = 0
    ORDER BY RowNum
    """

    row_count = get_row_count(connection, schema_name, table_name)
    chunk_size = max(1, row_count // num_chunks)

    boundaries = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, {'chunk_size': chunk_size})
            for row in cursor:
                boundaries.append(row[0])
        return boundaries
    except pyodbc.Error:
        # If the CTE approach fails (perhaps due to timeout on very large tables),
        # try a different approach with OFFSET-FETCH
        fallback_query = f"""
        SELECT {primary_key}
        FROM {schema_name}.{table_name} WITH (NOLOCK)
        ORDER BY {primary_key}
        OFFSET ? ROWS
        FETCH NEXT 1 ROWS ONLY
        """

        boundaries = []
        with connection.cursor() as cursor:
            for i in range(1, num_chunks):
                offset = i * chunk_size
                cursor.execute(fallback_query, offset)
                row = cursor.fetchone()
                if row:
                    boundaries.append(row[0])

        return boundaries


def create_table_chunks(connection, schema_name, table_name, chunk_size=100000, row_threshold=1500000, max_chunks=20):
    """
    Creates chunks for a table based on its primary key
    Returns a list of WHERE clauses that can be used to filter the table
    Increase the chunk size and max chunks based on CPU threads available
        Our SQL Server also multithreads for some sql query executions, so keep that in mind when load balancing
    """
    row_count = get_row_count(connection, schema_name, table_name)

    # If table is small enough, don't chunk it
    if row_count <= row_threshold:
        return None, 0  # No WHERE clauses needed

    # Calculate how many chunks we need
    num_chunks = min(max_chunks, (row_count + chunk_size - 1) // chunk_size)

    # Get primary key column(s)
    primary_keys = get_table_primary_key(connection, schema_name, table_name)

    if not primary_keys:
        logging.warning(f"No primary key found for table {schema_name}.{table_name}. Cannot chunk.")
        return None, 0

    # For simplicity, we'll use the first primary key column
    primary_key = primary_keys[0]

    # Get data type of the primary key
    type_query = f"""
    SELECT DATA_TYPE 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
    """
    with connection.cursor() as cursor:
        cursor.execute(type_query, (schema_name, table_name, primary_key))
        data_type = cursor.fetchone()[0].lower()

    # For uniqueidentifier (GUID) type
    # This is our main primary key for most tables
    if data_type == 'uniqueidentifier':
        boundaries = get_guid_boundaries(connection, schema_name, table_name, primary_key, num_chunks)

        if not boundaries:
            return None, 0

        chunks = []

        # Need to create three different where clauses for chunks
        # First chunk: everything up to the first boundary
        chunks.append(f"{primary_key} < '{boundaries[0]}'")

        # Middle chunks
        for i in range(len(boundaries) - 1):
            chunks.append(f"{primary_key} >= '{boundaries[i]}' AND {primary_key} < '{boundaries[i + 1]}'")

        # Last chunk: everything from the last boundary onwards
        chunks.append(f"{primary_key} >= '{boundaries[-1]}'")

        return chunks, len(chunks)


    # For integer/numerical dtype primary keys
    elif data_type in ('int', 'bigint', 'smallint', 'tinyint'):
        # Get min and max values
        minmax_query = f"""
        SELECT MIN({primary_key}), MAX({primary_key})
        FROM {schema_name}.{table_name} WITH (NOLOCK)
        """
        with connection.cursor() as cursor:
            cursor.execute(minmax_query)
            min_val, max_val = cursor.fetchone()

            if min_val is None or max_val is None:
                return None, 0

            chunk_width = max(1, (max_val - min_val + 1) // num_chunks)

            chunks = []
            for i in range(num_chunks):
                start = min_val + i * chunk_width
                end = min_val + (i + 1) * chunk_width if i < num_chunks - 1 else max_val + 1

                if i == 0:
                    chunks.append(f"{primary_key} < {end}")
                elif i == num_chunks - 1:
                    chunks.append(f"{primary_key} >= {start}")
                else:
                    chunks.append(f"{primary_key} >= {start} AND {primary_key} < {end}")

            return chunks, len(chunks)

    else:
        logging.warning(f"Primary key {primary_key} has type {data_type}, "
                        f"which doesn't support chunking. Using full table.")
        return None, 0


def copy_table(source_conn, target_conn, table_name, source_schema, target_schema, target_conn_str,
               where_clause=None, batch_size=10000, tracker_handler=None, timestamp_col_names=None):
    """
        Big function that does the heavy lifting for the table migration
        Allows for batch_sizing, iterates rows from cursor to fill up threads and batch_size
        Reduce batch_size if at ram limits or adding more workers
            may slow down migration if batch_size becomes too small
        GIL locks out the i/o problem, which is weird, but we PPE at table/chunk level to avoid GIL
        TPE doesn't do as much here, but it does help speed it up the processes, use at least 2 threads
            but more than 3/4 probably won't help at all, or do anything
        Swaps between batch_insert/merge depending on timestamp/PK availability.
    """

    # Track time
    table_start_time = time.time()

    timestamp_columns, primary_key = get_table_info(source_conn, table_name, source_schema, timestamp_col_names)

    # Get columns (names) from source table
    source_cursor = source_conn.cursor()
    source_cursor.execute(f"SELECT * FROM {source_schema}.{table_name} WHERE 1=0")
    source_columns = [column[0] for column in source_cursor.description]

    # Get columns from target table
    target_cursor = target_conn.cursor()
    target_cursor.execute(f"SELECT * FROM {target_schema}.{table_name} WHERE 1=0")
    target_columns = [column[0] for column in target_cursor.description]

    # Find common columns
    columns = [col for col in source_columns if col in target_columns]

    missing_in_target = set(source_columns) - set(target_columns)
    missing_in_source = set(target_columns) - set(source_columns)

    # Log missing columns
    if missing_in_target:
        logging.warning(
            f"Warning: The following columns exist in the source table but not in the target table for {table_name}: {', '.join(missing_in_target)}")
    if missing_in_source:
        logging.warning(
            f"Warning: The following columns exist in the target table but not in the source table for {table_name}: {', '.join(missing_in_source)}")

    columns_str = ", ".join(columns)
    source_columns_str = ", ".join([f"source.{col}" for col in columns])
    update_set = ", ".join([f"target.{col} = source.{col}" for col in columns if col not in primary_key])

    if timestamp_columns and primary_key:
        # Selective copy with upsert
        week_ago = get_week_ago_timestamp()
        where_clause = " OR ".join([f"{col} >= '{week_ago}'" for col in timestamp_columns])
        select_query = f"SELECT * FROM {source_schema}.{table_name} WHERE {where_clause}"
        chunk_info = ""
    elif where_clause:
        select_query = f"SELECT * FROM {source_schema}.{table_name} WHERE {where_clause}"
        chunk_info = f" (with WHERE: {where_clause})"
    else:
        # Full table copy
        select_query = f"SELECT * FROM {source_schema}.{table_name}"
        chunk_info = ""

    logging.info(f"Executing select query for {table_name}{chunk_info}")

    logging.info(select_query)

    source_cursor = source_conn.cursor()
    source_cursor.execute(select_query)

    select_end_time = time.time()
    logging.info(
        f"Time taken to execute select query for {table_name}{chunk_info}: {select_end_time - table_start_time:.2f} seconds")

    target_table_name = table_name

    if timestamp_columns and primary_key:
        # Upsert query using MERGE
        merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in primary_key])
        merge_query = f"""
        MERGE INTO {target_schema}.{target_table_name} AS target
        USING (VALUES ({{}})) AS source ({columns_str})
        ON {merge_condition}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({columns_str})
            VALUES ({source_columns_str});
        """

        logging.info(merge_query)

        count_query = f"SELECT COUNT(*) FROM ({select_query}) AS count_query"
        source_cursor.execute(count_query)
        total_rows = source_cursor.fetchone()[0]

        optimal_batch_size = max(min(total_rows // 20, batch_size), 1)
        logging.info(f"Total rows to merge: {total_rows}")
        logging.info(f"Optimal batch size: {optimal_batch_size}")

        batch_size = optimal_batch_size

        # source_cursor was overwritten, select query needs to be reexecuted.
        # This bug haunted me for a little bit haha, didn't realize I was using the same cursor that I read later

        source_cursor.execute(select_query)

    else:
        # Regular insert query
        insert_query = f"INSERT INTO {target_schema}.{target_table_name} ({columns_str}) VALUES ({', '.join(['?' for _ in columns])})"
        logging.info(insert_query)

    min_batch_size = 1
    rows_copied = 0

    while True:
        try:
            rows = []
            futures = []
            with ThreadPoolExecutor(max_workers=3) as batch_executor:
                for row in source_cursor:
                    # Filter the rows on selected columns
                    filtered_row = [row[source_columns.index(col)] for col in columns]
                    rows.append(filtered_row)
                    if len(rows) == batch_size:
                        if timestamp_columns and primary_key:
                            futures.append(batch_executor.submit(batch_merge, target_conn_str, merge_query, rows,
                                                                 columns, table_name, target_schema,
                                                                 timestamp_columns, primary_key, tracker_handler))
                            rows_copied += len(rows)
                        else:
                            futures.append(batch_executor.submit(batch_insert, target_conn_str, insert_query, rows,
                                                                 columns))
                            rows_copied += len(rows)
                        rows = []

                if rows:
                    if timestamp_columns and primary_key:
                        futures.append(batch_executor.submit(batch_merge, target_conn_str, merge_query, rows, columns,
                                                             table_name, target_schema, timestamp_columns, primary_key,
                                                             tracker_handler))
                    else:
                        futures.append(batch_executor.submit(batch_insert, target_conn_str, insert_query, rows,
                                                             columns))
                    rows_copied += len(rows)

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result is not None:
                            logging.info(f"Future result: {result}")
                    except Exception as e:
                        logging.error(f"Error in future: {e}")
                        logging.error(traceback.format_exc())
            break

        # Specific error checking here, for some of the errors initially witness
        except pyodbc.Error as e:
            if isinstance(e, pyodbc.ProgrammingError) and e.args[0] == '42000':
                logging.error(f"Syntax error in SQL query for table {table_name}. Please check the query.")
                logging.error(traceback.format_exc())
                break
            elif isinstance(e, pyodbc.DataError) and 'String data, right truncation' in str(e):
                logging.warning(f"String truncation error in table {table_name}. Adjusting batch size.")
                if batch_size > min_batch_size:
                    batch_size = max(min_batch_size, batch_size // 2)
                    logging.info(f"New batch size: {batch_size}")
                else:
                    logging.error("Batch size is already at the minimum. Cannot reduce further.")
                    logging.error("Skipping table and continue with the next table.")
                    logging.error(traceback.format_exc())
                    break
            else:
                logging.error(f"Unexpected error: {e}")
                logging.error(traceback.format_exc())
                break

    source_cursor.close()
    logging.info(f"Done with table {table_name}")

    table_end_time = time.time()
    logging.info(f"Time taken for {table_name}{chunk_info}: {table_end_time - table_start_time} seconds")

    return rows_copied


def prepare_table_chunks(source_conn_str, target_conn_str, source_schema_name, target_schema_name, table_names,
                         chunk_size=100000, row_count_threshold=1500000, timestamp_col_names=None):
    """
        Analyzes tables and prepares task arguments for chunked processing.
        Returns a list of task arguments with chunking information
        Don't make row_count too low, definitely not lower than batch/chunk, set it depending on DB knowledge
        Generally:
            row_count_threshold = 10 * chunk_size
            chunk_size = 10 * batch_size
    """
    source_conn = pyodbc.connect(source_conn_str)

    task_args = []

    for table_name in table_names:
        try:
            # First, check if the table has timestamp columns and primary key
            # (tables that will be merged instead of fully copied)
            timestamp_columns, primary_key = get_table_info(source_conn, table_name, source_schema_name,
                                                            timestamp_col_names)

            if timestamp_columns and primary_key:
                # This table will be processed with a merge operation - no chunking
                logging.info(
                    f"Table {table_name} has timestamp columns and primary key. Will be processed with merge (no chunking).")
                task_args.append(
                    (table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name, None))
                continue

            # Check if the other tables are large enough to need chunking
            row_count = get_row_count(source_conn, source_schema_name, table_name)

            if row_count < row_count_threshold:
                # Small table - process as a single unit
                task_args.append(
                    (table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name, None))
                continue

            # Calculate appropriate chunk count based on table size
            # More chunks for larger tables, but not too many
            # I may just set it to 20 (or equal to workers) instead of dynamically approaching it like this,
            # but I'll have to set row_count threshold higher
            max_chunks = min(20, max(4, row_count // chunk_size))

            # Get chunk WHERE clauses
            chunks, actual_chunk_count = create_table_chunks(source_conn, source_schema_name, table_name,
                                                             chunk_size=chunk_size, row_threshold=row_count_threshold,
                                                             max_chunks=max_chunks)

            if not chunks:
                # Chunking wasn't possible, process as a single unit
                task_args.append(
                    (table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name, None))
                continue

            # Add a separate task for each chunk
            logging.info(f"Created {actual_chunk_count} chunks for large table {table_name} ({row_count} rows)")

            for i, where_clause in enumerate(chunks):
                task_args.append((table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name,
                                  where_clause))

        except Exception as e:
            logging.error(f"Error preparing chunks for table {table_name}: {str(e)}")
            logging.error(traceback.format_exc())
            # If chunking preparation fails, add the table as a single unit
            task_args.append(
                (table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name, None))

    source_conn.close()
    return task_args


def execute_with_retry(cursor, query, params=None, max_retries=5, initial_delay=0.1, max_delay=30):
    # In the case that we hit deadlock errors, adding longer delays as well
    retries = 0
    while True:
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return
        except pyodbc.Error as e:
            if isinstance(e, pyodbc.ProgrammingError):
                # Don't retry on programming errors, it's cooked
                raise
            if '40001' in str(e) and retries < max_retries:
                # This is a deadlock error, let's retry
                # We don't get deadlock errors anymore, resolved as a separate problem
                # We're also not multiprocessing on merges, that's left to one thread per table
                retries += 1
                # Random delay after deadlocking
                delay = min(initial_delay * (2 ** retries) + random.uniform(0, 0.1), max_delay)
                logging.warning(f"Deadlock encountered. Retrying in {delay:.2f} seconds... (Attempt {retries}/{max_retries})")
                time.sleep(delay)
            else:
                # If it's not a deadlock or we've exhausted our retries, re-raise the exception
                raise


def batch_merge(target_conn_str, merge_query, rows, columns, table_name, target_schema_name,
                timestamp_columns=None, primary_key=None, tracker_handler=None):
    """
        Big batch_merge function
        This is where the tracker is filled, every row in every new merge needs to be tracked for now
            Tracks PK, timestamp, and the table for basic handles
        Retries on failed merges, if an existing row needs to be updated
        Creates an upsert statement for merges using PK and timestamp columns
        Checks for constraint violation, searches for the old conflicting row to replace
    """

    # LEGB
    def fill_tracker(update_list):
        if timestamp_indices:
            # Get primary key values as concatenated string
            pk_values = []
            for idx in primary_key_indices:
                if idx < len(row):
                    pk_values.append(str(row[idx]))

            pk_value_str = "|".join(pk_values)

            # Get the latest timestamp
            latest_timestamp = None
            for idx in timestamp_indices:
                if idx < len(row):
                    if row[idx] is not None:
                        if latest_timestamp is None or row[idx] > latest_timestamp:
                            latest_timestamp = row[idx]

            if pk_value_str and latest_timestamp:
                update_list.append({
                    'table_name': table_name,
                    'primary_key': pk_value_str,
                    'timestamp': latest_timestamp
                })
        return update_list

    target_conn = pyodbc.connect(target_conn_str)
    target_cursor = target_conn.cursor()

    successful_merges = 0
    errors = 0

    # Initialize tracker data for this batch
    tracked_rows = []

    try:
        for row in rows:
            placeholders = ', '.join(['?' for _ in columns])
            formatted_query = merge_query.format(placeholders)
            try:
                execute_with_retry(target_cursor, formatted_query, row)
                successful_merges += 1

                # Track modified/inserted rows for tables with timestamp columns
                primary_key_indices = []
                timestamp_indices = []

                # Get indices of primary key and timestamp columns
                for i, col in enumerate(columns):
                    if col in primary_key:
                        primary_key_indices.append(i)
                    if col in timestamp_columns:
                        timestamp_indices.append(i)

                # Only track for tables with proper timestamps
                tracked_rows = fill_tracker(tracked_rows)


            except pyodbc.IntegrityError as e:
                error_message = str(e)
                logging.error(error_message)
                if "Violation of UNIQUE KEY constraint" in error_message:
                    # Extract the constraint name
                    constraint_match = re.search(r"Violation of UNIQUE KEY constraint '(.+?)'", error_message)
                    if constraint_match:
                        constraint_name = constraint_match.group(1)
                    else:
                        logging.error(f"Could not extract constraint name from error message: {error_message}")
                        errors += 1
                        continue

                    # Extract the conflicting values
                    match = re.search(r'The duplicate key value is \((.*?)\)', error_message)
                    if match:
                        conflicting_values_str = match.group(1)

                        conflicting_values = []
                        for value in conflicting_values_str.split(', '):
                            value = value.strip()
                            logging.info(f'Value: {value}')
                            if value == '<NULL>':
                                conflicting_values.append(None)
                            elif value.startswith("'") and value.endswith("'"):
                                conflicting_values.append(value[1:-1])  # Remove quotes
                            else:
                                try:
                                    conflicting_values.append(int(value))
                                except ValueError:
                                    try:
                                        conflicting_values.append(float(value))
                                    except ValueError:
                                        conflicting_values.append(value)

                    # Get the column names for the unique constraint
                    # There was an error here due to ordering something that was not in the Distinct Select,
                    # after Distinct was added to stop adding duplicate constraints on the same columns
                    constraint_columns_query = f"""
                    SELECT DISTINCT COL_NAME(ic.object_id, ic.column_id) AS column_name,
                        ic.key_ordinal
                    FROM sys.indexes AS i
                    INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                    WHERE i.name = '{constraint_name}'
                    ORDER BY ic.key_ordinal;
                    """
                    execute_with_retry(target_cursor, constraint_columns_query)
                    constraint_columns = [row.column_name for row in target_cursor.fetchall()]

                    if len(constraint_columns) != len(conflicting_values):
                        logging.error(
                            f"Mismatch in number of constraint columns and conflicting values "
                            f"for constraint {constraint_name}")
                        errors += 1
                        logging.info(f"Number of constraint columns: {len(constraint_columns)}, "
                                     f"Number of conflicting values: {len(conflicting_values)}")
                        logging.info(f"constraint_columns: {constraint_columns}")
                        logging.info(f"conflicting_values: {conflicting_values}")
                        continue

                    logging.info(f"constraint_columns: {constraint_columns}")
                    logging.info(f"conflicting_values: {conflicting_values}")

                    # Construct a DELETE query based on the conflicting values
                    delete_conditions = []
                    delete_values = []
                    for col, val in zip(constraint_columns, conflicting_values):
                        if val is not None:
                            delete_conditions.append(f"{col} = ?")
                            delete_values.append(val)
                        else:
                            delete_conditions.append(f"{col} IS NULL")

                    logging.info(f"delete_values: {delete_values}")
                    logging.info(f"delete_conditions: {delete_conditions}")

                    delete_query = (f"DELETE FROM {target_schema_name}.{table_name} WHERE " +
                                    " AND ".join(delete_conditions))

                    logging.info(f"delete_query: {delete_query}")

                    # Execute the DELETE query
                    try:
                        execute_with_retry(target_cursor, delete_query, delete_values)
                        logging.info(f"Deleted conflicting row for constraint {constraint_name}")
                        try:
                            # Try the MERGE again
                            execute_with_retry(target_cursor, formatted_query, row)
                            logging.info(f"Second merge successful")
                            successful_merges += 1

                            # Track successful merge after deletion
                            tracked_rows = fill_tracker(tracked_rows)

                        except pyodbc.Error as merge_error_2:
                            logging.error(f"Second merge error for row: {merge_error_2}")
                            errors += 1
                    except pyodbc.Error as delete_error:
                        logging.error(f"Error deleting conflicting row: {delete_error}")
                        errors += 1
                else:
                    logging.error(f"Integrity error for row: {e}")
                    errors += 1
            except pyodbc.Error as e:
                logging.error(f"Error merging row: {e}")
                errors += 1

        target_conn.commit()
        logging.info(f"Successfully merged {successful_merges} out of {len(rows)} rows. Errors: {errors}")

        # Add tracked rows to the global tracker
        if tracked_rows and tracker_handler:
            try:
                update_tracker(tracked_rows, tracker_handler)
            except Exception as track_error:
                logging.error(f"Error updating tracker: {track_error}")
    except pyodbc.Error as e:
        logging.error(f"Error in batch_merge: {e}")
        target_conn.rollback()
    finally:
        target_cursor.close()
        target_conn.close()


def copy_table_wrapper(args):
    """
        Enhanced wrapper function that supports both full table and chunked processing
            Replaces the very basic copy_table_wrapper in version 6
        Works with PPE, packing/unpacking args for PPE
    """

    # Unpack arguments
    (table_name, source_conn_str, target_conn_str, source_schema_name,
     target_schema_name, where_clause, timestamp_col_names, tracker_handler) = args

    # Set up logging for this worker process
    process_logger = logging.getLogger()
    process_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    for handler in process_logger.handlers:
        process_logger.removeHandler(handler)

    # Add a file handler that writes to the same log file
    # Hopefully, this will append the processes as the threads input/output with no collision and interruption
    file_handler = logging.FileHandler("logfileM.log", mode='a')
    file_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    process_logger.addHandler(file_handler)

    chunk_info = f" chunk with WHERE: {where_clause}" if where_clause else ""
    process_logger.info(f"Starting copy process for table {table_name}{chunk_info}")

    source_conn = pyodbc.connect(source_conn_str)
    target_conn = pyodbc.connect(target_conn_str)

    try:
        rows_copied = copy_table(source_conn, target_conn, table_name,
                                 source_schema_name, target_schema_name,
                                 target_conn_str, where_clause,
                                 tracker_handler=tracker_handler,
                                 timestamp_col_names=timestamp_col_names)
        process_logger.info(f"Completed copy process for table {table_name}{chunk_info}, copied {rows_copied} rows")
        return rows_copied
    except Exception as e:
        process_logger.error(f"Failed to copy table {table_name}{chunk_info}: {str(e)}")
        process_logger.error(traceback.format_exc())
        raise
    finally:
        source_conn.close()
        target_conn.close()


def main():
    # Initialize timer
    start_time = time.perf_counter()

    # Set up logging for main process
    logger = configure_logging()
    logging.info("Starting database migration process")

    try:
        # Initializations
        pyodbc.pooling = True

        source_server_name = ''
        source_database_name = ''
        source_schema_name = ''

        target_server_name = ''
        target_database_name = ''
        target_schema_name = ''

        contains_image = []

        source_username = ''
        source_password = ''

        # Connection parameters for the source and target databases
        # Change the driver based on what's appropriate, originally designed for SQL Server
        # For other SQL database servers, redesign the queries to match dialects
        source_conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={source_server_name};'
            f'DATABASE={source_database_name};'
            f'UID={source_username};'
            f'PWD={source_password};'
        )

        target_conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={target_server_name};'
            f'DATABASE={target_database_name};'
            'Trusted_Connection=yes;'
        )

        # Use whatever directory you want
        cwd = ''
        # Whatever txt file contains your table names
        # For the reader to appropriately read it, separate each table name by lines
        # i.e.
        # table1
        # table2
        # table3
        # - in your txt file
        table_names_path = os.path.join(cwd, 'txtfiles', 'StagingTables.txt')
        table_names = read_table_names(table_names_path)

        logging.info(f"Found {len(table_names)} tables to migrate")

        # Initializing the tracker here
        tracker_handler = initialize_tracker()

        # Initialize timestamp columns here
        # i.e. ['ADD_DATE_TIME_STAMP', 'CHANGE_DATE_TIME_STAMP', 'DATE_ADDED']
        timestamp_col_names = []


        # Removing all foreign keys first to avoid reference errors
        logging.info("Removing existing foreign keys from target schema")
        target_conn = pyodbc.connect(target_conn_str)
        remove_all_foreign_keys(target_conn, target_schema_name)
        target_conn.close()

        # Opening and closing the connections for create table statement
        logging.info("Creating table structures in target database")
        source_conn = pyodbc.connect(source_conn_str)
        target_conn = pyodbc.connect(target_conn_str)

        for table_name in table_names:
            create_table(source_conn, target_conn, table_name, source_schema_name, target_schema_name, contains_image,
                         timestamp_col_names=timestamp_col_names)

        source_conn.close()
        target_conn.close()

        logging.info(f"Tables containing image data: {contains_image}")

        # Prepare all table chunks, create task_args at this level
        logging.info("Analyzing tables and preparing chunks for large tables")

        # This preparation/analyzing for indexing table chunks is relatively short compared to the table copying process
        # but you can multiprocess it if it begins to take too long (i.e., we introduce a ton more tables)
        task_args = prepare_table_chunks(source_conn_str, target_conn_str, source_schema_name, target_schema_name,
                                         table_names, timestamp_col_names=timestamp_col_names)

        # Add tracker_handler to each task args
        task_args = [(arg[0], arg[1], arg[2], arg[3], arg[4], arg[5],
                      timestamp_col_names, tracker_handler) for arg in task_args]

        logging.info(f"Created {len(task_args)} total tasks (tables and chunks)")

        # Processpool for the table/chunk copy jobs (threadpool runs into GIL)
        logging.info("Starting parallel table and chunk copy operations")

        # Opening and closing the connections for table copying (handled in wrapper)
        with ProcessPoolExecutor(max_workers=20) as table_executor:
            # Submit all table copy jobs to the executor
            future_to_table = {table_executor.submit(copy_table_wrapper, args): args[0] for args in task_args}

            # Process results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Failed to copy table or chunk: {table_name}: {str(e)}")
                    logging.error(traceback.format_exc())

        # Applies constraints in parallel, with a constraint_args list
        logging.info("Applying constraints and foreign keys")

        # Prepare args (see function) for constraint application
        constraint_args = []
        for table_name in table_names:
            constraint_args.append((table_name, source_conn_str, target_conn_str,
                                    source_schema_name, target_schema_name))

        logging.info(f"Starting parallel constraint application for {len(constraint_args)} tables")

        # PPE implementation here
        with ProcessPoolExecutor(max_workers=20) as constraint_executor:
            # Submit all constraint application jobs to the executor
            future_to_table = {constraint_executor.submit(apply_constraint_wrapper, args): args[0]
                               for args in constraint_args}

            # Process results as they complete
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    logging.info(f"Constraint application for {table_name} completed successfully")
                except Exception as e:
                    logging.warning(f"Ran into an exception, intended behavior: \n"
                                    f"Constraint already should exist since it already existed in the Merge Table \n"
                                    f"Failed to apply constraints to table: {table_name}: {str(e)}")

        # Opening and closing the connections for getting and updating the appropriate foreign key constraints/references
        source_conn = pyodbc.connect(source_conn_str)
        target_conn = pyodbc.connect(target_conn_str)

        try:
            logging.info("Applying foreign key constraints")
            apply_foreign_key_constraints(target_conn, source_conn, table_names, source_schema_name, target_schema_name)
        finally:
            source_conn.close()
            target_conn.close()

        # Save the tracker, to the tracker folder
        save_tracker(tracker_handler)

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        logging.info(f"Elapsed time: {elapsed_time} seconds")

    except MemoryError as e:
        logging.error("MemoryError encountered: reducing batch size or optimizing memory usage may be required.")
        logging.error(traceback.format_exc())
    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error(traceback.format_exc())
        logging.error('Something went wrong with the whole block.')


if __name__ == "__main__":
    main()


