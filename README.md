# Hiropon DB-Sync

Hiropon DB-Sync is designed to help developers and database administrators keep database schemas in sync efficiently. Whether you are migrating databases, maintaining consistency between environments, or performing version control for your database schema, this tool provides an automated and reliable solution.

## Features

✅ Compare and synchronize database schemas\
✅ Generate SQL migration scripts\
✅ Support for **MariaDB/MySQL, ~~PostgreSQL, SQLite~~**\
✅ Handles tables, columns, indexes, and foreign keys\
✅ Provides safe validation before execution

## Installation

### Requirements

- Python 3.8+
- `pip` package manager
- MariaDB/MySQL, ~~PostgreSQL, or SQLite server~~

### Install dependencies

```bash
pip install -r requirements.txt
```

## Usage

Run the script with a configuration file:

```bash
python main.py --config your_config.env --output sync_script.sql
```

### Configuration

The tool uses an **.env** file for database credentials. Example:

```ini
# Source Database
SOURCE_HOST=localhost
SOURCE_PORT=3306
SOURCE_USER=root
SOURCE_PASSWORD=secret
SOURCE_DATABASE=source_db

# Target Database
TARGET_HOST=localhost
TARGET_PORT=3306
TARGET_USER=root
TARGET_PASSWORD=secret
TARGET_DATABASE=target_db
```

### Example Execution

```bash
python main.py --config db_config.env --output migration.sql
```

This will generate a **migration.sql** file containing SQL statements to sync the target database.

## Development

To modify the project, clone the repository and set up a virtual environment:

```bash
git clone https://github.com/anthony-sousa/hiropon-db-sync.git
cd hiropon-db-sync
pip install -r requirements.txt
```

## License

This project is licensed under the MIT License.

