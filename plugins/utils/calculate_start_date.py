from datetime import datetime

from airflow.models import Variable
from airflow.utils.dates import days_ago

# Zettle = 2 years - but maybe  "2016-08-01T00:00:00.000Z" ZETTLE_START_DAYS_AGO
# Default = 3 years MONGO_START_DATE
# stripe = 2 years
# shopify


def get_days_ago_start_date(variable_name="DEFAULT_DAYS_OFFSET", default_days=30 * 3):
    """
    Retrieve the number of days ago from an Airflow Variable to calculate the DAG's start_date.

    :param variable_name: The name of the Airflow Variable storing the days offset.
    :param default_days: The default number of days ago to use if the Variable is not set.
    :return: A datetime object representing the start_date.
    """
    try:
        # Get the variable value; use the default if not set
        days_offset = int(Variable.get(variable_name, default_var=default_days))
        return days_ago(days_offset)
    except ValueError:
        raise ValueError(f"The variable {variable_name} must contain an integer value.")


def fixed_date_start_date(variable_name, default_date):
    """
    Retrieve a fixed start date from an Airflow Variable containing an ISO date string
    or use a default datetime object if the Variable is not set.

    :param variable_name: The name of the Airflow Variable storing the ISO date string.
    :param default_date: A default datetime object to use if the Variable is not set.
    :return: A datetime object representing the fixed start date.
    :raises ValueError: If the Variable is set but does not contain a valid ISO date string.
    """
    try:
        # Try to get the variable value
        iso_date_string = Variable.get(variable_name)
        # If the variable exists, parse it as an ISO date string
        return datetime.fromisoformat(iso_date_string)
    except KeyError:
        # If the variable is not set, return the default date
        return default_date
    except ValueError:
        # If the variable exists but is not a valid ISO date string, raise an error
        raise ValueError(f"The variable '{variable_name}' does not contain a valid ISO date string.")
