import mariadb
from typing import List, Dict, Tuple
from contextlib import contextmanager
from db_config import DatabaseConfig

class DatabaseSync:
    def __init__(self, source_config: DatabaseConfig, target_config: DatabaseConfig):
        self.source_config = source_config
        self.target_config = target_config
        self.sql_commands: List[str] = []

    @contextmanager
    def get_connection(self, db_config: DatabaseConfig):
        """Creates and manages database connection with context manager."""
        try:
            conn = mariadb.connect(
                host=db_config.host,
                port=db_config.port,
                user=db_config.user,
                password=db_config.password,
                database=db_config.database
            )
            yield conn
        except mariadb.Error as e:
            raise RuntimeError(f"Error connecting to database: {e}")
        finally:
            if 'conn' in locals():
                conn.close()

    def get_tables(self, conn: mariadb.Connection) -> List[str]:
        """Returns list of tables in database."""
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return [table[0] for table in cursor.fetchall()]

    def get_table_structure(self, conn: mariadb.Connection, table: str) -> Tuple[List[Dict], Dict, Dict]:
        """Returns table structure including columns, indexes, and foreign keys."""
        with conn.cursor(dictionary=True) as cursor:
            # Get columns
            cursor.execute("""
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
                FROM information_schema.COLUMNS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (conn.database, table))
            columns = cursor.fetchall()
            
            # Get indexes
            cursor.execute("""
                SELECT INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS columns,
                       NOT NON_UNIQUE AS is_unique
                FROM information_schema.STATISTICS 
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                GROUP BY INDEX_NAME
            """, (conn.database, table))
            indexes = {row['INDEX_NAME']: row for row in cursor.fetchall()}
            
            # Get foreign keys
            cursor.execute("""
                SELECT 
                    k.CONSTRAINT_NAME,
                    GROUP_CONCAT(k.COLUMN_NAME) AS columns,
                    k.REFERENCED_TABLE_NAME,
                    GROUP_CONCAT(k.REFERENCED_COLUMN_NAME) AS referenced_columns,
                    r.UPDATE_RULE,
                    r.DELETE_RULE
                FROM information_schema.KEY_COLUMN_USAGE k
                JOIN information_schema.REFERENTIAL_CONSTRAINTS r
                ON k.CONSTRAINT_NAME = r.CONSTRAINT_NAME
                WHERE k.TABLE_SCHEMA = %s 
                  AND k.TABLE_NAME = %s
                  AND k.REFERENCED_TABLE_NAME IS NOT NULL
                GROUP BY k.CONSTRAINT_NAME
            """, (conn.database, table))
            foreign_keys = {row['CONSTRAINT_NAME']: row for row in cursor.fetchall()}
            
            return columns, indexes, foreign_keys

    def format_column_definition(self, col: Dict) -> str:
        """Formats column definition for SQL statement."""
        col_def = f"`{col['COLUMN_NAME']}` {col['COLUMN_TYPE']}"
        col_def += " NULL" if col['IS_NULLABLE'] == 'YES' else " NOT NULL"

        if col['COLUMN_DEFAULT'] is not None:
            default = col['COLUMN_DEFAULT']
            
            if isinstance(default, str):
                if default.upper() == "NULL":
                    col_def += " DEFAULT NULL"  # Correção: NULL sem aspas
                elif default.upper() == "CURRENT_TIMESTAMP":
                    col_def += " DEFAULT CURRENT_TIMESTAMP"  # Sem aspas
                else:
                    safe_default = default.replace("'", "''")  # Evita aspas duplas
                    col_def += f" DEFAULT '{safe_default}'"
            else:
                col_def += f" DEFAULT {default}"

        if col['EXTRA']:
            col_def += f" {col['EXTRA']}"

        return col_def

    def generate_column_sql(self, table: str, source_cols: List[Dict], target_cols: List[Dict]) -> List[str]:
        """Generates SQL commands for column modifications."""
        sql = []
        target_col_map = {col['COLUMN_NAME']: col for col in target_cols}
        prev_column = None

        for i, scol in enumerate(source_cols):
            col_name = scol['COLUMN_NAME']
            tcol = target_col_map.get(col_name)
            
            if not tcol:
                # Add new column
                col_def = self.format_column_definition(scol)
                position = "FIRST" if not prev_column else f"AFTER `{prev_column}`"
                sql.append(f"ALTER TABLE `{table}` ADD COLUMN {col_def} {position}")
            else:
                # Check if modification needed
                if not self._columns_match(scol, tcol):
                    col_def = self.format_column_definition(scol)
                    position = "FIRST" if not prev_column else f"AFTER `{prev_column}`"
                    sql.append(f"ALTER TABLE `{table}` MODIFY COLUMN {col_def} {position}")
            
            prev_column = col_name
        
        # Process column drops
        source_col_names = {col['COLUMN_NAME'] for col in source_cols}
        for tcol in target_cols:
            if tcol['COLUMN_NAME'] not in source_col_names:
                sql.append(f"ALTER TABLE `{table}` DROP COLUMN `{tcol['COLUMN_NAME']}`")
        
        return sql

    def _columns_match(self, source_col: Dict, target_col: Dict) -> bool:
        """Compares two columns for equality."""
        return (
            source_col['COLUMN_TYPE'] == target_col['COLUMN_TYPE'] and
            source_col['IS_NULLABLE'] == target_col['IS_NULLABLE'] and
            source_col['COLUMN_DEFAULT'] == target_col['COLUMN_DEFAULT'] and
            source_col['EXTRA'] == target_col['EXTRA']
        )

    def generate_index_sql(self, table: str, source_idx: Dict, target_idx: Dict) -> List[str]:
        """Generates SQL commands for index modifications."""
        sql = []
        
        # Remove extra indexes
        for idx_name in set(target_idx) - set(source_idx):
            if idx_name != 'PRIMARY':  # Don't drop primary keys this way
                sql.append(f"ALTER TABLE `{table}` DROP INDEX `{idx_name}`")
        
        # Add/modify indexes
        for idx_name, sidx in source_idx.items():
            if idx_name == 'PRIMARY':
                continue  # Handle primary keys separately
                
            tidx = target_idx.get(idx_name)
            if not tidx or tidx['columns'] != sidx['columns'] or tidx['is_unique'] != sidx['is_unique']:
                if tidx:
                    sql.append(f"ALTER TABLE `{table}` DROP INDEX `{idx_name}`")
                
                unique = "UNIQUE" if sidx['is_unique'] else ""
                cols = ", ".join(f"`{col.strip()}`" for col in sidx['columns'].split(','))
                sql.append(f"ALTER TABLE `{table}` ADD {unique} INDEX `{idx_name}` ({cols})")
        
        return sql

    def generate_fk_sql(self, table: str, source_fk: Dict, target_fk: Dict) -> List[str]:
        """Generates SQL commands for foreign key modifications."""
        sql = []
        
        # Remove extra FKs
        for fk_name in set(target_fk) - set(source_fk):
            sql.append(f"ALTER TABLE `{table}` DROP FOREIGN KEY `{fk_name}`")
        
        # Add/modify FKs
        for fk_name, sfk in source_fk.items():
            tfk = target_fk.get(fk_name)
            if not tfk or tfk != sfk:
                if tfk:
                    sql.append(f"ALTER TABLE `{table}` DROP FOREIGN KEY `{fk_name}`")
                
                cols = ", ".join(f"`{col.strip()}`" for col in sfk['columns'].split(','))
                ref_cols = ", ".join(f"`{col.strip()}`" for col in sfk['referenced_columns'].split(','))
                
                sql.append(f"""ALTER TABLE `{table}` ADD CONSTRAINT `{fk_name}`
                    FOREIGN KEY ({cols}) 
                    REFERENCES `{sfk['REFERENCED_TABLE_NAME']}` ({ref_cols})
                    ON UPDATE {sfk['UPDATE_RULE']}
                    ON DELETE {sfk['DELETE_RULE']}""")
        
        return sql

    def generate_sync_sql(self) -> List[str]:
        """Generates complete SQL synchronization script."""
        try:
            with self.get_connection(self.source_config) as source_conn, \
                 self.get_connection(self.target_config) as target_conn:
                
                source_tables = self.get_tables(source_conn)
                target_tables = self.get_tables(target_conn)

                self.sql_commands = ["-- Database synchronization script",
                                   "SET FOREIGN_KEY_CHECKS = 0;"]
                
                # Process table structure
                for table in source_tables:
                    source_cols, source_idx, source_fk = self.get_table_structure(source_conn, table)
                    
                    if table not in target_tables:
                        # Generate CREATE TABLE statement
                        with source_conn.cursor() as cursor:
                            cursor.execute(f"SHOW CREATE TABLE `{table}`")
                            create_stmt = cursor.fetchone()[1]
                            self.sql_commands.extend([
                                f"\n-- Create new table {table}",
                                create_stmt
                            ])
                        continue
                    
                    # Get target structure
                    target_cols, target_idx, target_fk = self.get_table_structure(target_conn, table)
                    
                    # Generate modifications
                    self.sql_commands.extend(self.generate_column_sql(table, source_cols, target_cols))
                    self.sql_commands.extend(self.generate_index_sql(table, source_idx, target_idx))
                    self.sql_commands.extend(self.generate_fk_sql(table, source_fk, target_fk))
                
                # Generate DROP TABLE statements for extra tables
                for table in set(target_tables) - set(source_tables):
                    self.sql_commands.extend([
                        f"\n-- Drop table {table}",
                        f"DROP TABLE `{table}`"
                    ])

                self.sql_commands.append("\nSET FOREIGN_KEY_CHECKS = 1;")
                return self.sql_commands

        except Exception as e:
            raise RuntimeError(f"Error during sync: {e}")

    def save_sql_to_file(self, filename: str = 'sync_queries.sql') -> None:
        """Saves generated SQL commands to file."""
        if not self.sql_commands:
            raise RuntimeError("No SQL commands generated yet")
            
        with open(filename, 'w') as f:
            for command in self.sql_commands:
                # Handle multi-line SQL statements
                command = command.strip()
                if command:
                    if not command.endswith(';'):
                        command += ';'
                    f.write(f"{command}\n")
        print(f"SQL queries saved to {filename}")
