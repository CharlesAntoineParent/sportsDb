"""This module contains parameters for the sportsdb package."""
from datetime import datetime

from pytz import timezone

STARTING_MONTH = 10
TIMEZONE = "Canada/Eastern"


class SeasonYear:
    """This class acts as a parameter for season year."""

    current_datetime = datetime.now(timezone(TIMEZONE))
    current_season_year = current_datetime.year
    if current_datetime.month <= STARTING_MONTH:
        current_season_year -= 1

    default = current_season_year
