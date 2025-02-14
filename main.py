import argparse
import os
from dotenv import load_dotenv
from db_config import DatabaseConfig
from db_sync import DatabaseSync

def validate_config(config: DatabaseConfig, prefix: str) -> None:
    """Validate that all required configuration values are present"""
    if not config.user:
        raise ValueError(f"{prefix}_USER is required in config file")
    if not config.password:
        raise ValueError(f"{prefix}_PASSWORD is required in config file")
    if not config.database:
        raise ValueError(f"{prefix}_DATABASE is required in config file")

def load_config(config_file: str) -> tuple[DatabaseConfig, DatabaseConfig]:
    """
    Load database configurations from a .env file
    """
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file not found: {config_file}")
        
    load_dotenv(config_file)
    
    source_config = DatabaseConfig(
        host=os.getenv('SOURCE_HOST', 'localhost'),
        port=int(os.getenv('SOURCE_PORT', '3306')),
        user=os.getenv('SOURCE_USER', ''),
        password=os.getenv('SOURCE_PASSWORD', ''),
        database=os.getenv('SOURCE_DATABASE', '')
    )
    
    target_config = DatabaseConfig(
        host=os.getenv('TARGET_HOST', 'localhost'),
        port=int(os.getenv('TARGET_PORT', '3306')),
        user=os.getenv('TARGET_USER', ''),
        password=os.getenv('TARGET_PASSWORD', ''),
        database=os.getenv('TARGET_DATABASE', '')
    )
    
    # Validate configurations
    validate_config(source_config, "SOURCE")
    validate_config(target_config, "TARGET")
    
    return source_config, target_config

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Database structure synchronization tool')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--output', default=f'sync_queries.sql', help='Output SQL file path')
    args = parser.parse_args()
    
    try:
        # Load configurations
        source_config, target_config = load_config(args.config)
        
        # Initialize and run sync
        sync = DatabaseSync(source_config, target_config)
        sql_commands = sync.generate_sync_sql()
        sync.save_sql_to_file(args.output)
        
        print(f"SQL queries saved to {args.output}")
        print("Review the SQL file before executing it!")
        
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()