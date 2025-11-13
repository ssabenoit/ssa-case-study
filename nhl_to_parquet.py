"""
NHL Data Extractor to Parquet Files

Extracts data from NHL API and saves to Parquet files for Snowflake loading.

Usage:
    python nhl_to_parquet.py --start-date 2025-11-07 --end-date 2025-11-07 --output-dir ./data
"""

import argparse
import os
import logging
from datetime import datetime, timedelta
import pandas as pd

from nhl_extractor import NHLExtractor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_to_parquet(
    start_date: str,
    end_date: str,
    output_dir: str = "./data",
    include_dependent: bool = True,
    request_delay: float = 1.0,
):
    """
    Extract NHL data and save to Parquet files.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Directory to save Parquet files
        include_dependent: Whether to include dependent streams
        request_delay: Delay between API requests in seconds
    """
    logger.info("=" * 70)
    logger.info("NHL Data Extraction to Parquet Files")
    logger.info("=" * 70)
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Request delay: {request_delay}s (to avoid rate limiting)")
    logger.info("=" * 70)

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Initialize extractor
    extractor = NHLExtractor(
        start_date=start_date,
        end_date=end_date,
        max_retries=5,
        retry_delay=2,
        request_delay=request_delay
    )

    # Extract all streams
    all_data = extractor.extract_all(include_dependent=include_dependent)

    # Save each stream to Parquet
    logger.info("\n" + "=" * 70)
    logger.info("Saving to Parquet files...")
    logger.info("=" * 70)

    for stream_name, records in all_data.items():
        if not records:
            logger.warning(f"No records for {stream_name}, skipping...")
            continue

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Convert all dict/list columns to JSON strings for Parquet compatibility
        # Snowflake can parse JSON when loading
        import json
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if any value in column is dict or list
                sample = df[col].dropna().head(1)
                if len(sample) > 0 and isinstance(sample.iloc[0], (dict, list)):
                    df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)

        # Add ETL timestamp
        df['_etl_loaded_at'] = datetime.now()

        # Save to Parquet
        output_file = os.path.join(output_dir, f"{stream_name}.parquet")
        df.to_parquet(output_file, index=False, engine='pyarrow')

        logger.info(f"✓ Saved {len(records)} records to {output_file}")

    logger.info("\n" + "=" * 70)
    logger.info("Extraction complete!")
    logger.info("=" * 70)
    logger.info(f"\nParquet files saved to: {output_dir}")
    logger.info("\nNext steps:")
    logger.info("  1. Run: python parquet_to_snowflake.py --input-dir ./data")
    logger.info("=" * 70)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Extract NHL data to Parquet files'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
        help='Start date in YYYY-MM-DD format (default: 7 days ago)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='End date in YYYY-MM-DD format (default: today)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./data',
        help='Directory to save Parquet files (default: ./data)'
    )
    parser.add_argument(
        '--no-dependent',
        action='store_true',
        help='Skip dependent streams (faster for testing)'
    )
    parser.add_argument(
        '--request-delay',
        type=float,
        default=1.0,
        help='Delay between API requests in seconds (default: 1.0)'
    )

    args = parser.parse_args()

    try:
        extract_to_parquet(
            start_date=args.start_date,
            end_date=args.end_date,
            output_dir=args.output_dir,
            include_dependent=not args.no_dependent,
            request_delay=args.request_delay,
        )
    except Exception as e:
        logger.error(f"Error during extraction: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    main()
