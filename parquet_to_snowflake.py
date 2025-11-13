"""
Load Parquet Files to Snowflake

Uses Snowflake's PUT and COPY INTO commands for efficient bulk loading.

Loading Strategy:
    - Full Replace (always): team_rosters, current_standings, current_teams, season_schedules
    - Incremental Append: games, daily_standings, game_boxscore, game_summaries,
                          play_by_play

Usage:
    # Normal incremental load (full replace for specified tables, append for others)
    python parquet_to_snowflake.py --input-dir ./data

    # Force full replace for ALL tables
    python parquet_to_snowflake.py --input-dir ./data --drop-tables
"""

import argparse
import os
import logging
from pathlib import Path
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_snowflake_config():
    """Get Snowflake configuration from environment variables."""
    return {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        'database': os.getenv('SNOWFLAKE_DATABASE'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA'),
        'role': os.getenv('SNOWFLAKE_ROLE'),
    }


def load_parquet_to_snowflake(
    input_dir: str,
    drop_tables: bool = False,
):
    """
    Load Parquet files to Snowflake using PUT and COPY INTO.

    Args:
        input_dir: Directory containing Parquet files
        drop_tables: Whether to drop existing tables (forces full replace for all)
    """
    config = get_snowflake_config()

    # Define which tables should be full replace vs incremental
    FULL_REPLACE_TABLES = {'team_rosters', 'current_standings', 'current_teams', 'season_schedules'}

    logger.info("=" * 70)
    logger.info("Loading Parquet Files to Snowflake")
    logger.info("=" * 70)
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Target: {config['database']}.{config['schema']}")
    logger.info(f"Drop tables: {drop_tables}")
    logger.info(f"Full replace tables: {', '.join(FULL_REPLACE_TABLES)}")
    logger.info("=" * 70)

    # Connect to Snowflake
    conn = snowflake.connector.connect(**config)
    cursor = conn.cursor()

    try:
        # Create stage for file uploads
        stage_name = "nhl_parquet_stage"
        logger.info(f"\nCreating stage: {stage_name}")
        cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name}")

        # Get all parquet files
        parquet_files = list(Path(input_dir).glob("*.parquet"))
        logger.info(f"\nFound {len(parquet_files)} Parquet files")

        for parquet_file in parquet_files:
            table_name = parquet_file.stem  # filename without extension

            logger.info(f"\n{'='*70}")
            logger.info(f"Loading {table_name}")
            logger.info(f"{'='*70}")

            # Determine if this should be full replace or incremental
            is_full_replace = drop_tables or table_name in FULL_REPLACE_TABLES

            if is_full_replace:
                logger.info(f"Mode: FULL REPLACE")
            else:
                logger.info(f"Mode: INCREMENTAL APPEND")

            # Upload file to stage
            logger.info(f"Uploading {parquet_file.name} to stage...")
            cursor.execute(f"PUT file://{parquet_file.absolute()} @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

            # First create file format if not exists
            cursor.execute("""
            CREATE FILE FORMAT IF NOT EXISTS nhl_parquet_format
            TYPE = PARQUET
            """)

            if is_full_replace:
                # Full replace mode: drop and recreate table
                logger.info(f"Creating/replacing table {table_name}...")

                create_sql = f"""
                CREATE OR REPLACE TABLE {table_name}
                USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '@{stage_name}/{parquet_file.name}',
                            FILE_FORMAT => 'nhl_parquet_format'
                        )
                    )
                )
                """
                cursor.execute(create_sql)

                # Load data using COPY INTO
                logger.info(f"Loading data into {table_name}...")
                copy_sql = f"""
                COPY INTO {table_name}
                FROM @{stage_name}/{parquet_file.name}
                FILE_FORMAT = (FORMAT_NAME = 'nhl_parquet_format')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
                """
                cursor.execute(copy_sql)

            else:
                # Incremental mode: create table if not exists, then append
                # Check if table exists
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = '{config['schema'].upper()}'
                    AND TABLE_NAME = '{table_name.upper()}'
                """)
                table_exists = cursor.fetchone()[0] > 0

                if not table_exists:
                    # Create table for the first time
                    logger.info(f"Table doesn't exist. Creating {table_name}...")
                    create_sql = f"""
                    CREATE TABLE {table_name}
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '@{stage_name}/{parquet_file.name}',
                                FILE_FORMAT => 'nhl_parquet_format'
                            )
                        )
                    )
                    """
                    cursor.execute(create_sql)

                    # Load initial data
                    logger.info(f"Loading initial data into {table_name}...")
                    copy_sql = f"""
                    COPY INTO {table_name}
                    FROM @{stage_name}/{parquet_file.name}
                    FILE_FORMAT = (FORMAT_NAME = 'nhl_parquet_format')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = CONTINUE
                    """
                    cursor.execute(copy_sql)

                else:
                    # Table exists - use temp table for incremental load
                    temp_table = f"{table_name}_temp"
                    logger.info(f"Table exists. Using temp table for incremental load...")

                    # Create temp table
                    create_temp_sql = f"""
                    CREATE OR REPLACE TABLE {temp_table}
                    USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '@{stage_name}/{parquet_file.name}',
                                FILE_FORMAT => 'nhl_parquet_format'
                            )
                        )
                    )
                    """
                    cursor.execute(create_temp_sql)

                    # Load data into temp table
                    logger.info(f"Loading data into temp table...")
                    copy_temp_sql = f"""
                    COPY INTO {temp_table}
                    FROM @{stage_name}/{parquet_file.name}
                    FILE_FORMAT = (FORMAT_NAME = 'nhl_parquet_format')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    ON_ERROR = CONTINUE
                    """
                    cursor.execute(copy_temp_sql)

                    # Get count of new records
                    cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
                    new_records = cursor.fetchone()[0]

                    # Append from temp table to main table
                    logger.info(f"Appending {new_records} new records to {table_name}...")
                    cursor.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")

                    # Drop temp table
                    cursor.execute(f"DROP TABLE {temp_table}")

            # Get final row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]

            logger.info(f"✓ Table {table_name} now has {count} total records")

        logger.info("\n" + "=" * 70)
        logger.info("All files loaded successfully!")
        logger.info("=" * 70)

    finally:
        cursor.close()
        conn.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Load Parquet files to Snowflake with incremental loading support'
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default='./data',
        help='Directory containing Parquet files (default: ./data)'
    )
    parser.add_argument(
        '--drop-tables',
        action='store_true',
        help='Force full replace for ALL tables (default: only replace team_rosters, current_standings, current_teams)'
    )

    args = parser.parse_args()

    try:
        load_parquet_to_snowflake(
            input_dir=args.input_dir,
            drop_tables=args.drop_tables,
        )
    except Exception as e:
        logger.error(f"Error during loading: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
