"""
NHL Data Extractor - Reproduces Airbyte extraction logic in Python

This module extracts data from the NHL API following the patterns defined
in the Airbyte configuration. It supports:
- Simple streams (single API calls)
- Incremental streams (date-ranged extraction)
- Dependent streams (using parent stream data)
"""

import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Iterator, Callable
from abc import ABC, abstractmethod
import time
import logging
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NHLAPIClient:
    """Base client for NHL API with retry logic and rate limiting."""

    BASE_URL = "https://api-web.nhle.com/v1"

    def __init__(self, max_retries: int = 5, retry_delay: int = 2, request_delay: float = 0.5):
        """
        Initialize NHL API client.

        Args:
            max_retries: Maximum number of retry attempts
            retry_delay: Base delay between retries in seconds
            request_delay: Delay between all requests to avoid rate limiting (seconds)
        """
        self.session = requests.Session()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_delay = request_delay
        self.last_request_time = 0

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Make GET request with retry logic and rate limiting."""
        url = f"{self.BASE_URL}/{endpoint}"

        # Add delay between requests to avoid rate limiting
        if self.request_delay > 0:
            time_since_last = time.time() - self.last_request_time
            if time_since_last < self.request_delay:
                time.sleep(self.request_delay - time_since_last)

        for attempt in range(self.max_retries):
            try:
                self.last_request_time = time.time()
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.warning(f"404 Not Found: {url}")
                    return None
                elif e.response.status_code == 429:
                    # Rate limited - use exponential backoff with longer delays
                    wait_time = self.retry_delay * (2 ** attempt) * 2  # Double the wait for 429
                    logger.warning(f"HTTP 429 Rate Limited on attempt {attempt + 1}. Waiting {wait_time}s before retry...")
                    if attempt < self.max_retries - 1:
                        time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"HTTP error on attempt {attempt + 1}: {e}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1}: {e}")

            if attempt < self.max_retries - 1:
                # Exponential backoff
                wait_time = self.retry_delay * (attempt + 1)
                time.sleep(wait_time)

        logger.error(f"Failed to fetch {url} after {self.max_retries} attempts")
        return None

    def extract_field(self, data: Dict[str, Any], field_path: List[str]) -> Any:
        """Extract data using field path (similar to DpathExtractor)."""
        if not field_path:
            return data

        result = data
        for field in field_path:
            if isinstance(result, dict):
                result = result.get(field)
            else:
                return None
            if result is None:
                return None
        return result


@dataclass
class StreamConfig:
    """Configuration for a data stream."""
    name: str
    endpoint_template: str
    field_path: List[str] = None

    def __post_init__(self):
        if self.field_path is None:
            self.field_path = []


class BaseStream(ABC):
    """Base class for all streams."""

    def __init__(self, client: NHLAPIClient, config: StreamConfig):
        self.client = client
        self.config = config

    @abstractmethod
    def read_records(self, **kwargs) -> Iterator[Dict[str, Any]]:
        """Read records from the stream."""
        pass

    def _extract_records(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract records from API response using field path."""
        extracted = self.client.extract_field(data, self.config.field_path)

        if extracted is None:
            return []

        # If field_path is empty, return the whole response as single record
        if not self.config.field_path:
            return [extracted] if isinstance(extracted, dict) else []

        # If extracted is a list, return it; otherwise wrap in list
        return extracted if isinstance(extracted, list) else [extracted]


class SimpleStream(BaseStream):
    """Stream that makes a single API call."""

    def read_records(self, **kwargs) -> Iterator[Dict[str, Any]]:
        """Fetch data from a simple endpoint."""
        logger.info(f"Fetching {self.config.name}...")

        data = self.client.get(self.config.endpoint_template)
        if data:
            records = self._extract_records(data)
            logger.info(f"Retrieved {len(records)} records from {self.config.name}")
            yield from records


class IncrementalStream(BaseStream):
    """Stream that iterates over date ranges."""

    def __init__(self, client: NHLAPIClient, config: StreamConfig,
                 start_date: str, end_date: Optional[str] = None,
                 step_days: int = 1):
        super().__init__(client, config)
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d") if end_date else datetime.now()
        self.step_days = step_days

    def read_records(self, **kwargs) -> Iterator[Dict[str, Any]]:
        """Fetch data for each date in the range."""
        logger.info(f"Fetching {self.config.name} from {self.start_date.date()} to {self.end_date.date()}...")

        current_date = self.start_date
        total_records = 0

        while current_date <= self.end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            endpoint = self.config.endpoint_template.format(date=date_str)

            data = self.client.get(endpoint)
            if data:
                records = self._extract_records(data)
                # Add date field to each record
                for record in records:
                    if isinstance(record, dict):
                        record['date'] = date_str
                        yield record
                        total_records += 1

            current_date += timedelta(days=self.step_days)

        logger.info(f"Retrieved {total_records} total records from {self.config.name}")


class DependentStream(BaseStream):
    """Stream that depends on data from a parent stream."""

    def __init__(self, client: NHLAPIClient, config: StreamConfig,
                 parent_stream: BaseStream, parent_key: str,
                 partition_field: str):
        super().__init__(client, config)
        self.parent_stream = parent_stream
        self.parent_key = parent_key
        self.partition_field = partition_field

    def read_records(self, parent_records: Optional[List[Dict]] = None, **kwargs) -> Iterator[Dict[str, Any]]:
        """Fetch data for each partition from parent stream."""
        logger.info(f"Fetching {self.config.name} (depends on {self.parent_stream.config.name})...")

        # If parent records not provided, fetch them
        if parent_records is None:
            parent_records = list(self.parent_stream.read_records(**kwargs))

        total_records = 0

        for parent_record in parent_records:
            partition_value = parent_record.get(self.parent_key)
            if partition_value is None:
                continue

            endpoint = self.config.endpoint_template.format(**{self.partition_field: partition_value})

            data = self.client.get(endpoint)
            if data:
                records = self._extract_records(data)
                # Add the partition value to each record (e.g., team_abv for rosters)
                for record in records:
                    if isinstance(record, dict):
                        record[self.partition_field] = partition_value
                    yield record
                total_records += len(records)

        logger.info(f"Retrieved {total_records} total records from {self.config.name}")


class NHLExtractor:
    """Main extractor class that orchestrates all streams."""

    def __init__(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_retries: int = 5,
        retry_delay: int = 2,
        request_delay: float = 0.5
    ):
        """
        Initialize NHL extractor.

        Args:
            start_date: Start date for incremental streams (YYYY-MM-DD)
            end_date: End date for incremental streams (YYYY-MM-DD)
            max_retries: Maximum retry attempts for failed requests
            retry_delay: Base delay between retries (seconds)
            request_delay: Delay between all requests to avoid rate limiting (seconds)
        """
        self.client = NHLAPIClient(
            max_retries=max_retries,
            retry_delay=retry_delay,
            request_delay=request_delay
        )
        self.start_date = start_date or (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        self.end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        self._setup_streams()

    def _setup_streams(self):
        """Configure all NHL data streams."""

        # Simple streams - no dependencies
        self.current_standings_stream = SimpleStream(
            self.client,
            StreamConfig(
                name="current_standings",
                endpoint_template="standings/now",
                field_path=["standings"]
            )
        )

        self.current_teams_stream = SimpleStream(
            self.client,
            StreamConfig(
                name="current_teams",
                endpoint_template="schedule-calendar/now",
                field_path=["teams"]  # Extract the teams array
            )
        )

        # Incremental streams - date-based
        self.games_stream = IncrementalStream(
            self.client,
            StreamConfig(
                name="games",
                endpoint_template="score/{date}",
                field_path=["games"]
            ),
            start_date=self.start_date,
            end_date=self.end_date
        )

        self.daily_standings_stream = IncrementalStream(
            self.client,
            StreamConfig(
                name="daily_standings",
                endpoint_template="standings/{date}",
                field_path=["standings"]
            ),
            start_date=self.start_date,
            end_date=self.end_date
        )

        # Dependent streams - require parent data
        self.team_rosters_stream = DependentStream(
            self.client,
            StreamConfig(
                name="team_rosters",
                endpoint_template="roster/{team_abv}/current",
                field_path=[]
            ),
            parent_stream=self.current_teams_stream,
            parent_key="abbrev",
            partition_field="team_abv"
        )

        self.season_schedules_stream = DependentStream(
            self.client,
            StreamConfig(
                name="season_schedules",
                endpoint_template="club-schedule-season/{team_abv}/now",
                field_path=["games"]
            ),
            parent_stream=self.current_teams_stream,
            parent_key="abbrev",
            partition_field="team_abv"
        )

        self.game_boxscore_stream = DependentStream(
            self.client,
            StreamConfig(
                name="game_boxscore",
                endpoint_template="gamecenter/{game_id}/boxscore",
                field_path=[]
            ),
            parent_stream=self.games_stream,
            parent_key="id",
            partition_field="game_id"
        )

        self.game_summaries_stream = DependentStream(
            self.client,
            StreamConfig(
                name="game_summaries",
                endpoint_template="wsc/game-story/{game_id}",
                field_path=[]
            ),
            parent_stream=self.games_stream,
            parent_key="id",
            partition_field="game_id"
        )

        self.play_by_play_stream = DependentStream(
            self.client,
            StreamConfig(
                name="play_by_play",
                endpoint_template="gamecenter/{game_id}/play-by-play",
                field_path=[]
            ),
            parent_stream=self.games_stream,
            parent_key="id",
            partition_field="game_id"
        )

    def extract_stream(self, stream_name: str, **kwargs) -> List[Dict[str, Any]]:
        """Extract data from a specific stream."""
        stream = getattr(self, f"{stream_name}_stream", None)
        if stream is None:
            raise ValueError(f"Unknown stream: {stream_name}")

        return list(stream.read_records(**kwargs))

    def extract_all(self, include_dependent: bool = True) -> Dict[str, List[Dict[str, Any]]]:
        """
        Extract data from all streams.

        Args:
            include_dependent: Whether to include dependent streams (can be slow)

        Returns:
            Dictionary mapping stream names to their records
        """
        results = {}

        # Extract simple streams
        logger.info("=" * 60)
        logger.info("Extracting simple streams...")
        logger.info("=" * 60)
        results['current_standings'] = self.extract_stream('current_standings')
        results['current_teams'] = self.extract_stream('current_teams')

        # Extract incremental streams
        logger.info("=" * 60)
        logger.info("Extracting incremental streams...")
        logger.info("=" * 60)
        results['games'] = self.extract_stream('games')
        results['daily_standings'] = self.extract_stream('daily_standings')

        if include_dependent:
            # Extract dependent streams that use current_teams
            logger.info("=" * 60)
            logger.info("Extracting team-dependent streams...")
            logger.info("=" * 60)
            results['team_rosters'] = self.extract_stream(
                'team_rosters',
                parent_records=results['current_teams']
            )
            results['season_schedules'] = self.extract_stream(
                'season_schedules',
                parent_records=results['current_teams']
            )

            # Extract dependent streams that use games
            logger.info("=" * 60)
            logger.info("Extracting game-dependent streams...")
            logger.info("=" * 60)
            results['game_boxscore'] = self.extract_stream(
                'game_boxscore',
                parent_records=results['games']
            )
            results['game_summaries'] = self.extract_stream(
                'game_summaries',
                parent_records=results['games']
            )
            results['play_by_play'] = self.extract_stream(
                'play_by_play',
                parent_records=results['games']
            )

        return results


# Example usage
if __name__ == "__main__":
    import json

    # Initialize extractor for the last 3 days
    extractor = NHLExtractor(
        start_date="2024-11-04",
        end_date="2024-11-07"
    )

    # Option 1: Extract specific streams
    print("\n" + "=" * 60)
    print("OPTION 1: Extract specific streams")
    print("=" * 60)

    standings = extractor.extract_stream('current_standings')
    print(f"\nCurrent standings: {len(standings)} records")
    print(json.dumps(standings[0], indent=2) if standings else "No data")

    games = extractor.extract_stream('games')
    print(f"\nGames: {len(games)} records")
    if games:
        print(json.dumps(games[0], indent=2))

    # Option 2: Extract all streams (warning: can be slow with dependent streams)
    print("\n" + "=" * 60)
    print("OPTION 2: Extract all streams")
    print("=" * 60)

    # Set to False to skip dependent streams (faster)
    all_data = extractor.extract_all(include_dependent=False)

    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    for stream_name, records in all_data.items():
        print(f"{stream_name}: {len(records)} records")

    # Optionally save to JSON files
    # for stream_name, records in all_data.items():
    #     with open(f"{stream_name}.json", "w") as f:
    #         json.dump(records, f, indent=2)
