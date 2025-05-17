"""The Datadis integration."""
import logging
from datetime import timedelta

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.const import CONF_USERNAME, CONF_PASSWORD
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from datadis_client import DatadisClient

from .const import DOMAIN, CONF_CUPS, CONF_DISTRIBUTOR_CODE, CONF_POINT_TYPE

_LOGGER = logging.getLogger(__name__)

# Define the platforms that this integration will support.
# TODO: Add sensor platform once sensor.py is created
PLATFORMS = ["sensor"]  # Assuming you will have sensors


async def fetch_consumption_data(hass, client, cups, distributor_code, date_str):
    """Fetch consumption data for a specific month."""
    try:
        consumption_data = await hass.async_add_executor_job(
            client.get_consumption_data,
            cups, distributor_code, date_str, date_str, "0", CONF_POINT_TYPE
        )
        if not consumption_data:
            _LOGGER.warning(f"Empty response for {cups} for month {date_str}")
            return None

        if not consumption_data.get("timeCurve"):
            _LOGGER.debug(f"No 'timeCurve' data for {cups} in month {date_str}, response: {consumption_data}")

        return consumption_data
    except Exception as e:
        _LOGGER.error(f"Unexpected error fetching data for {cups} ({date_str}): {e}")
        return None


async def merge_consumption_data(existing_data, new_data, data_key):
    """Merge existing and new consumption data."""
    if not new_data or "timeCurve" not in new_data:
        return existing_data.get(data_key, new_data)

    if not existing_data.get(data_key) or not isinstance(existing_data.get(data_key), dict) or \
       "timeCurve" not in existing_data.get(data_key, {}) or \
       not isinstance(existing_data.get(data_key, {}).get("timeCurve"), list):
        return new_data

    # Merge time curve data
    existing = existing_data[data_key]
    existing_readings = {
        (r["date"], r["time"]) for r in existing["timeCurve"] 
        if isinstance(r, dict) and "date" in r and "time" in r
    }
    merged_time_curve = existing["timeCurve"][:]

    for reading in new_data["timeCurve"]:
        if isinstance(reading, dict) and "date" in reading and "time" in reading:
            if (reading["date"], reading["time"]) not in existing_readings:
                merged_time_curve.append(reading)
                existing_readings.add((reading["date"], reading["time"]))

    return {**existing, **new_data, "timeCurve": merged_time_curve}


async def check_historical_data_exists(cups, all_consumption_data, current_year, current_month):
    """Check if historical data exists for the given CUPS."""
    if not all_consumption_data:
        return False

    for key in all_consumption_data.keys():
        if key.startswith(cups):  # Check against the configured CUPS
            try:
                year_month_str = key.split('_', 1)[1]
                year = int(year_month_str[:4])
                month_val = int(year_month_str[4:])
                if year < current_year or (year == current_year and month_val < current_month):
                    return True
            except (IndexError, ValueError, TypeError):
                _LOGGER.warning(f"Could not parse key {key} for historical check.")
                continue
    return False


async def perform_historical_backfill(hass, client, cups, distributor_code, all_consumption_data, current_year, current_month):
    """Perform historical data backfill if needed."""
    _LOGGER.info(f"Performing initial historical data fetch for {cups} from 2000/01 to {current_year}/{current_month:02d}")

    for year_loop in range(2000, current_year + 1):
        end_loop_month = current_month if year_loop == current_year else 12
        for month_loop in range(1, end_loop_month + 1):
            date_str_api = f"{year_loop:04d}/{month_loop:02d}"
            month_key = f"{cups}_{year_loop:04d}{month_loop:02d}"

            # Skip if we already have data for this month
            if month_key in all_consumption_data and \
               isinstance(all_consumption_data.get(month_key), dict) and \
               all_consumption_data[month_key].get("timeCurve"):
                _LOGGER.debug(f"Skipping already fetched month {date_str_api} for {cups}")
                continue

            _LOGGER.debug(f"Fetching historical data for {cups}, month: {date_str_api}")
            month_consumption = await fetch_consumption_data(hass, client, cups, distributor_code, date_str_api)
            if month_consumption:
                all_consumption_data[month_key] = month_consumption

    return all_consumption_data


async def update_monthly_data(hass, client, cups, distributor_code, all_consumption_data, year, month, is_current=False):
    """Update data for a specific month."""
    month_str_api = f"{year:04d}/{month:02d}"
    month_key = f"{cups}_{year:04d}{month:02d}"

    period_label = "current" if is_current else "previous"
    _LOGGER.info(f"Fetching/Updating {period_label} month data for {cups}: {month_str_api}")

    new_month_data = await fetch_consumption_data(hass, client, cups, distributor_code, month_str_api)
    if new_month_data:
        merged_data = await merge_consumption_data(all_consumption_data, new_month_data, month_key)
        all_consumption_data[month_key] = merged_data

    return all_consumption_data


def create_update_data_method(hass, client, config_cups, config_distributor_code):
    """Create the update data method for the coordinator."""

    async def async_update_data():
        """Fetch data from API endpoint."""
        try:
            # Authenticate with the Datadis API
            await hass.async_add_executor_job(client.authenticate)

            # Fetch all supplies for context/diagnostics
            all_user_supplies = await hass.async_add_executor_job(client.get_supplies)
            _LOGGER.debug(f"Found {len(all_user_supplies)} total supplies for the account.")

            # Use the configured CUPS and distributor code
            cups = config_cups
            distributor_code = config_distributor_code
            _LOGGER.debug(f"Using configured CUPS: {cups}, Distributor Code: {distributor_code}")

            # Get current data from coordinator if available
            coordinator = hass.data[DOMAIN].get(cups)
            existing_data = coordinator.data if coordinator and hasattr(coordinator, 'data') and coordinator.data else {}
            all_consumption_data = existing_data.get("consumption_data", {}).copy()

            # Get current date information
            now = dt_util.now()
            current_year = now.year
            current_month = now.month

            # Check if we need to do an initial backfill of historical data
            has_historical_data = await check_historical_data_exists(
                cups, all_consumption_data, current_year, current_month
            )
            _LOGGER.debug(f"Initial backfill for {cups}: {not has_historical_data}")

            if not has_historical_data:
                all_consumption_data = await perform_historical_backfill(
                    hass, client, cups, distributor_code, all_consumption_data, current_year, current_month
                )

            # Update current month's data
            all_consumption_data = await update_monthly_data(
                hass, client, cups, distributor_code, all_consumption_data, 
                current_year, current_month, is_current=True
            )

            # Update previous month's data
            prev_month_dt = now.replace(day=1) - timedelta(days=1)
            prev_year = prev_month_dt.year
            prev_month = prev_month_dt.month

            all_consumption_data = await update_monthly_data(
                hass, client, cups, distributor_code, all_consumption_data,
                prev_year, prev_month, is_current=False
            )

            # Return the data
            return {
                "supplies": all_user_supplies, 
                "consumption_data": all_consumption_data, 
                "main_cups": cups
            }

        except Exception as err:
            _LOGGER.error(f"Unexpected error fetching Datadis data: {err}")
            raise UpdateFailed(f"Unexpected error: {err}")

    return async_update_data


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Datadis from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    # Get configuration values
    username = entry.data[CONF_USERNAME]
    password = entry.data[CONF_PASSWORD]
    config_cups = entry.data.get(CONF_CUPS)
    config_distributor_code = entry.data.get(CONF_DISTRIBUTOR_CODE)

    if not config_cups or not config_distributor_code:
        _LOGGER.error("CUPS or distributor code not found in config entry. Please reconfigure the integration.")
        return False  # Indicate setup failure

    # Create datadis client
    client = DatadisClient(username=username, password=password)

    # Create update method for coordinator
    update_method = create_update_data_method(hass, client, config_cups, config_distributor_code)

    # Create coordinator with dynamic name including CUPS
    coordinator = DataUpdateCoordinator(
        hass,
        _LOGGER,
        name=f"datadis_sensor_update_{config_cups}",
        update_method=update_method,
        update_interval=timedelta(hours=24),
    )

    # Store the coordinator for later access in the update method
    hass.data[DOMAIN][config_cups] = coordinator
    hass.data[DOMAIN][entry.entry_id] = coordinator

    # Perform first refresh to load initial data
    await coordinator.async_config_entry_first_refresh()

    # Set up the platform entities
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
        # Also remove by CUPS key if it exists
        cups = entry.data.get(CONF_CUPS)
        if cups and cups in hass.data[DOMAIN]:
            hass.data[DOMAIN].pop(cups)

    return unload_ok
