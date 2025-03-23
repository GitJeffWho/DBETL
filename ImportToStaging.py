import pyodbc
import os
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from collections import defaultdict
import logging
import logging.handlers
import sys

# Imports needed in the merge, not needed in import
# import random
# import re
# from datetime import datetime, timedelta


# Current Ver V8
# Moving the helper functions outside of main (due to ppe collisions, see below)
# Updated constraint configuration from unimplemented V5
# Replaced all print statements with logging.info for proper logging across multiple processes
# Configured (actual proper) logging to handle multiple processes correctly
#   Fixed in V8.1
# ProcessPoolExecutor for true interpreter splits on threads to workaround GIL, fixed computation/ram issues
# Limited workers to avoid memory issues
# With V8, added table chunking for consistent times and leveling out tables
# Most comments are in the MergeToStaging.py file, refer to that for more function docstrings

# Future:
# Add multithreading to the initial chunk creation (per table, not per chunk)
#   and to the constraint addition finalization (also per table, not per chunk)
# Dynamic memory management (workers, batch size) based on existing memory in system (HARD)



# Start of Log Functions

def configure_logging():
    """
    Configure the root logger for the main process.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear any existing handlers to avoid duplication
    if root_logger.handlers:
        for handler in root_logger.handlers:
            root_logger.removeHandler(handler)

    # Add file handler
    file_handler = logging.FileHandler("logfile.log", mode='w')
    file_formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(file_formatter)
    root_logger.addHandler(console_handler)

    return root_logger


def read_table_names(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file]


def generate_create_table_sql(connection, source_table_name, target_table_name,
                              source_schema_name, target_schema_name, contains_image):
    # This version splits up the old generate_create_table_sql to two functions (apply_constraints below)
    # Helps with time complexity in theory, should trim a few hours off the full import
    # and a few minutes off the merge
    columns = []
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?""", (source_table_name, source_schema_name))

        for row in cursor:
            column_name, data_type, is_nullable, char_max_length, numeric_precision, numeric_scale = row
            sql_type = data_type  # Default to use the original type

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
                sql_type = 'nvarchar(max)'

            column_def = f"[{column_name}] {sql_type} {'NULL' if is_nullable == 'YES' else 'NOT NULL'}"
            columns.append(column_def)

    if not columns:
        raise Exception(
            f"No columns found for table {source_table_name} in schema {source_schema_name}. Check table name and schema.")

    create_table_sql = f"CREATE TABLE [{target_schema_name}].[{target_table_name}] (\n    {',\n    '.join(columns)}\n);"

    return create_table_sql


def generate_constraint_sql(connection, source_table_name, target_table_name, source_schema_name, target_schema_name):
    # Alter statement for constraint application
    # Like adding foreign keys at the end but for other constraints with less separate table dependencies
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
                        logging.warning(f"Error adding foreign key {fk_name} to table {table_name}: {str(e)}")
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


def create_table(source_conn, target_conn, table_name, source_schema_name, target_schema_name, contains_image):
    with target_conn.cursor() as cursor:
        cursor.execute(f"SELECT OBJECT_ID('{target_schema_name}.{table_name}', 'U')")
        table_exists = cursor.fetchone()[0] is not None

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
    Wrapper function for applying constraints to a table in a separate process.
    Replaces original apply_constraint function

    Args:
        args: Tuple containing (table_name, source_conn_str, target_conn_str,
                               source_schema_name, target_schema_name)
    """
    table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name = args

    # Set up logging for this worker process
    process_logger = logging.getLogger()
    process_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    for handler in process_logger.handlers:
        process_logger.removeHandler(handler)

    # Add a file handler that writes to the same log file
    file_handler = logging.FileHandler("logfile.log", mode='a')
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
                    process_logger.error(f"Error applying constraint to {table_name}: {str(e)}")
                    process_logger.error(f"Failed constraint: {constraint}")
                    raise

        process_logger.info(f"Completed applying all constraints to {table_name}")
        return table_name

    except Exception as e:
        process_logger.error(f"Error in applying constraints to {table_name}: {str(e)}")
        process_logger.error(traceback.format_exc())
        raise
    finally:
        # Close connections
        source_conn.close()
        target_conn.close()


def handle_create_table_error(e, table_name, create_table_sql, target_conn):
    logging.error(f"Error creating table {table_name}: {str(e)}")
    logging.info(f"Create Table SQL: {create_table_sql}")

    if "There are no primary or candidate keys in the referenced table" in str(e):
        logging.warning(f"Warning: Foreign key constraint issue detected for table {table_name}. "
                        f"This will be handled later in the foreign key application step.")
    else:
        target_conn.rollback()
        raise


def batch_insert(target_conn_str, insert_query, rows, columns):
    try:
        target_conn = pyodbc.connect(target_conn_str)
        target_cursor = target_conn.cursor()
        target_cursor.fast_executemany = True

        # Ensure insert_query has the correct number of placeholders
        # (Doesn't matter for this query, matters for batch_merge)
        placeholders = ', '.join(['?' for _ in columns])
        formatted_query = insert_query.format(placeholders)

        # Filter rows to only include specified columns
        # Haven't run into an error with this part for table insert even with mismatched columns
        # Just for consistency's sake
        filtered_rows = [[row[columns.index(col)] for col in columns] for row in rows]

        target_cursor.executemany(formatted_query, filtered_rows)
        target_conn.commit()

        # This is too many reports, I just left this as None for now
        # return f"Inserted {len(rows)} rows"
        # logging.info(f"Inserted {len(filtered_rows)} rows")

    except pyodbc.Error as e:
        logging.error(f"Error in batch_insert: {e}")
        logging.error(traceback.format_exc())
        target_conn.rollback()
    finally:
        target_cursor.close()
        target_conn.close()


def get_row_count(connection, schema_name, table_name):
    """Get the approximate row count of a table."""
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
    """Get the primary key column(s) for a table."""
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
    """Get boundary values for GUID-based chunking."""
    # For GUID/uniqueidentifier columns, we need evenly spaced values
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
    Creates chunks for a table based on its primary key.
    Returns a list of WHERE clauses that can be used to filter the table.
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
    if data_type == 'uniqueidentifier':
        boundaries = get_guid_boundaries(connection, schema_name, table_name, primary_key, num_chunks)

        if not boundaries:
            return None, 0

        chunks = []
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
        logging.warning(f"Primary key {primary_key} has type {data_type}, which doesn't support chunking. Using full table.")
        return None, 0


def copy_table(source_conn, target_conn, table_name, source_schema, target_schema, target_conn_str,
               where_clause=None, batch_size=10000):
    """
    Enhanced version of copy_table that can handle a WHERE clause for chunking.
    This allows us to use the same connection handling logic but with chunked data.
    """
    # Track time
    table_start_time = time.time()

    # Get columns from source table
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

    # Full table copy or chunked copy
    if where_clause:
        select_query = f"SELECT * FROM {source_schema}.{table_name} WHERE {where_clause}"
        chunk_info = f" (with WHERE: {where_clause})"
    else:
        select_query = f"SELECT * FROM {source_schema}.{table_name}"
        chunk_info = ""

    logging.info(f"Executing select query for {table_name}{chunk_info}")

    source_cursor = source_conn.cursor()
    source_cursor.execute(select_query)

    select_end_time = time.time()
    logging.info(
        f"Time taken to execute select query for {table_name}{chunk_info}: {select_end_time - table_start_time:.2f} seconds")

    target_table_name = table_name

    # Regular insert query
    insert_query = f"INSERT INTO {target_schema}.{target_table_name} ({columns_str}) VALUES ({', '.join(['?' for _ in columns])})"

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
                        futures.append(
                            batch_executor.submit(batch_insert, target_conn_str, insert_query, rows, columns))
                        rows_copied += len(rows)
                        # Reset Rows (for loop and for next if)
                        rows = []

                if rows:
                    futures.append(batch_executor.submit(batch_insert, target_conn_str, insert_query, rows, columns))
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
    logging.info(f"Done with table {table_name}{chunk_info}, copied {rows_copied} rows")

    table_end_time = time.time()
    logging.info(f"Time taken for {table_name}{chunk_info}: {table_end_time - table_start_time} seconds")

    return rows_copied


def prepare_table_chunks(source_conn_str, target_conn_str, source_schema_name, target_schema_name, table_names,
                         chunk_size=100000, row_count_threshold=1500000):
    """
    Analyzes tables and prepares task arguments for chunked processing.
    Returns a list of task arguments with chunking information.
    """
    source_conn = pyodbc.connect(source_conn_str)

    task_args = []

    for table_name in table_names:
        try:
            # Check if table is large enough to need chunking
            row_count = get_row_count(source_conn, source_schema_name, table_name)

            if row_count < row_count_threshold:
                # Small table - process as a single unit
                task_args.append(
                    (table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name, None))
                continue

            # Calculate appropriate chunk count based on table size
            # More chunks for larger tables, but not too many
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


def copy_table_wrapper(args):
    # Unpack arguments
    """
        Enhanced wrapper function that supports both full table and chunked processing.
        Works with PPE, packing/unpacking args for PPE
    """
    table_name, source_conn_str, target_conn_str, source_schema_name, target_schema_name, where_clause = args

    # Set up logging for this worker process
    process_logger = logging.getLogger()
    process_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    for handler in process_logger.handlers:
        process_logger.removeHandler(handler)

    # Add a file handler that writes to the same log file
    # Hopefully, this will append the processes as the threads input/output with no collision and
    file_handler = logging.FileHandler("logfile.log", mode='a')
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
                                 target_conn_str, where_clause)
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
        # Leave default pooling, up to 100 (will reuse connections, should never run into issues)
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

        cwd = ''
        table_names_path = os.path.join(cwd, 'txtfiles', 'StagingTables.txt')
        table_names = read_table_names(table_names_path)

        logging.info(f"Found {len(table_names)} tables to migrate")

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
            create_table(source_conn, target_conn, table_name, source_schema_name, target_schema_name, contains_image)

        source_conn.close()
        target_conn.close()

        logging.info(f"Tables containing image data: {contains_image}")

        # Prepare all table chunks, create task_args at this level
        logging.info("Analyzing tables and preparing chunks for large tables")

        # This preparation/analyzing for indexing table chunks is relatively short compared to the table copying process
        # but you can multiprocess it if it begins to take too long (i.e., we introduce a ton more tables)
        task_args = prepare_table_chunks(source_conn_str, target_conn_str, source_schema_name, target_schema_name,
                                         table_names)

        logging.info(f"Created {len(task_args)} total tasks (tables and chunks)")

        # Processpool for the table/chunk copy jobs (threadpool runs into GIL)
        logging.info("Starting parallel table and chunk copy operations")

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
                    logging.error(f"Failed to apply constraints to table: {table_name}: {str(e)}")


        # Apply foreign key constraints (after all individual table constraints)
        # This is fast enough to not need multiprocessing
        source_conn = pyodbc.connect(source_conn_str)
        target_conn = pyodbc.connect(target_conn_str)
        try:
            logging.info("Applying foreign key constraints")
            apply_foreign_key_constraints(target_conn, source_conn, table_names, source_schema_name, target_schema_name)
        finally:
            source_conn.close()
            target_conn.close()

        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        logging.info(f"Database migration completed. Elapsed time: {elapsed_time} seconds")

    except MemoryError as e:
        logging.error("MemoryError encountered: reducing batch size or optimizing memory usage may be required.")
        logging.error(traceback.format_exc())
    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error(traceback.format_exc())
        logging.error('Something went wrong with the whole block.')


if __name__ == "__main__":
    main()