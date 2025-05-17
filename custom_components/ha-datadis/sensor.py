"""Sensor platform for Datadis integration."""
import logging
from datetime import datetime, timedelta

from homeassistant.components.sensor import SensorEntity, SensorStateClass, SensorDeviceClass
from homeassistant.core import HomeAssistant, callback
from homeassistant.config_entries import ConfigEntry
from homeassistant.helpers.device_registry import DeviceInfo, DeviceEntryType
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

HOURS_TO_DISPLAY = 24  # Display last 24 hours of consumption


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Datadis sensor entities."""
    coordinator = hass.data[DOMAIN][entry.entry_id]

    # For now, we create one sensor for the main CUPS hourly consumption.
    # Future improvements: Create sensors for each supply point (CUPS).
    # The coordinator.data should contain {"supplies": [...], "consumption_data": {...}, "main_cups": "ESXXX"}

    if coordinator.data and coordinator.data.get("main_cups"):
        main_cups = coordinator.data["main_cups"]
        # Find the supply details for the main_cups to get a better name if possible
        supply_info = next((s for s in coordinator.data.get("supplies", []) if s.get("cups") == main_cups), None)
        sensor_name_extra = f" {supply_info.get('alias', main_cups).replace('-', ' ').title()}" if supply_info else f" {main_cups}"

        async_add_entities([
            DatadisHourlyConsumptionSensor(coordinator, entry, main_cups, sensor_name_extra)
        ])
    else:
        _LOGGER.warning("No main CUPS found in coordinator data to set up sensor.")


class DatadisHourlyConsumptionSensor(CoordinatorEntity, SensorEntity):
    """Datadis hourly consumption sensor."""

    _attr_state_class = SensorStateClass.MEASUREMENT
    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_native_unit_of_measurement = "kWh"
    _attr_icon = "mdi:flash"

    def __init__(self, coordinator, config_entry: ConfigEntry, cups: str, name_extra: str):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._config_entry = config_entry
        self._cups = cups
        self._attr_unique_id = f"{config_entry.entry_id}_{cups}_hourly_consumption"
        self._attr_name = f"Datadis Hourly Consumption{name_extra}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, config_entry.entry_id)},
            name=f"Datadis {config_entry.title}",
            manufacturer="Datadis",
            entry_type=DeviceEntryType.SERVICE,
        )
        self._update_attrs()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        self._update_attrs()
        self.async_write_ha_state()

    def _update_attrs(self) -> None:
        """Update sensor attributes based on coordinator data."""
        # The coordinator.data["consumption_data"] contains data for current and previous months
        # e.g., {"ESXXX_202311": {data...}, "ESXXX_202312": {data...} }
        # We need to parse 'timeCurve' from these entries and combine them.

        raw_hourly_data = []
        if self.coordinator.data and "consumption_data" in self.coordinator.data:
            consumption_data_for_cups = self.coordinator.data["consumption_data"]
            for key, month_data in consumption_data_for_cups.items():
                if key.startswith(self._cups) and month_data and "timeCurve" in month_data:
                    raw_hourly_data.extend(month_data["timeCurve"])

        if not raw_hourly_data:
            self._attr_native_value = None
            self._attr_extra_state_attributes = {"hourly_data": [], "last_updated_raw": None}
            _LOGGER.debug(f"No raw hourly data found for {self._cups}")
            return

        # Sort data by date and time to ensure correct order
        # The time format seems to be HH:MM:SS, date YYYY-MM-DD
        try:
            raw_hourly_data.sort(key=lambda x: datetime.fromisoformat(f"{x['date']}T{x['time']}"))
        except (TypeError, KeyError, ValueError) as e:
            _LOGGER.error(f"Error sorting raw consumption data for {self._cups}: {e} - Data: {raw_hourly_data[:5]}")
            self._attr_native_value = None  # Or keep previous value
            self._attr_extra_state_attributes = {"hourly_data": [], "last_updated_raw": "Error sorting data"}
            return

        # Keep only the last N hours (e.g., 24 or 48 for calculations)
        # We need to handle timezones correctly. Assume API times are UTC or local.
        # For simplicity, this example assumes times are naive but sequential.
        # Home Assistant expects UTC for datetime objects.
        # Datadis 'date' is YYYY-MM-DD, 'time' is HH:MM:SS. Let's assume it's local time without tzinfo.
        # We should convert these to UTC if they represent local time based on Spanish timezone.

        now_utc = dt_util.utcnow()
        cutoff_utc = now_utc - timedelta(hours=HOURS_TO_DISPLAY)

        parsed_and_filtered_data = []
        for reading in raw_hourly_data:
            try:
                # Assuming reading['date'] and reading['time'] are in local Spanish time
                # Create a naive datetime object first
                naive_dt = datetime.fromisoformat(f"{reading['date']}T{reading['time']}")
                # Localize to Spain (this requires knowing the exact timezone, e.g., 'Europe/Madrid')
                # For now, if dt_util.get_time_zone cannot be imported, we'll treat as system local then UTC
                # A more robust way is to let HA handle timezone conversions if possible or make it configurable
                # For now, we treat as local and convert to UTC for comparison
                # This part can be tricky without knowing the exact timezone from Datadis.
                # If the timestamps are already UTC, this localization step is not needed.
                # Based on the API usage (Spanish platform), 'Europe/Madrid' is a safe bet.
                local_tz = dt_util.get_time_zone("Europe/Madrid")
                # Ensure naive_dt is indeed naive before localizing
                if naive_dt.tzinfo is None or naive_dt.tzinfo.utcoffset(naive_dt) is None:
                    local_dt = naive_dt.replace(tzinfo=local_tz)
                else:  # If it's already timezone-aware (e.g. fromisoformat parsed a Z or offset)
                    local_dt = naive_dt  # Assume it's already correct or handle conversion if it's not Madrid time

                reading_utc = local_dt.astimezone(dt_util.UTC)

                if reading_utc >= cutoff_utc:
                    parsed_and_filtered_data.append({
                        "timestamp_utc": reading_utc.isoformat(),
                        "consumption_kWh": reading.get("consumptionKWh"),
                        "original_date": reading['date'],
                        "original_time": reading['time']
                    })
            except (TypeError, KeyError, ValueError) as e:
                _LOGGER.warning(f"Could not parse reading: {reading}, error: {e}")
                continue

        # Sort again just in case, and take the most recent HOURS_TO_DISPLAY
        parsed_and_filtered_data.sort(key=lambda x: x["timestamp_utc"], reverse=True)
        final_hourly_data = parsed_and_filtered_data[:HOURS_TO_DISPLAY]
        final_hourly_data.reverse()  # Show oldest first for charts

        if final_hourly_data:
            # The sensor state could be the latest hourly consumption, or sum of last 24h, or disabled.
            # For now, let's set it to the consumption of the most recent hour available in our dataset.
            # Or, sum of the last 24 hours of data we have.
            # The user asked for "hourly energy consumptions". Displaying *all* of them in the state is not feasible.
            # The state will be the sum of the kWh in `final_hourly_data`.
            # Individual hourly values will be in attributes.
            current_sum_kWh = sum(d['consumption_kWh'] for d in final_hourly_data if d['consumption_kWh'] is not None)
            self._attr_native_value = round(current_sum_kWh, 3)
            self._attr_extra_state_attributes = {
                "hourly_data": final_hourly_data,
                "last_updated_raw": dt_util.utcnow().isoformat(),
                "source_cups": self._cups
            }
        else:
            self._attr_native_value = None
            self._attr_extra_state_attributes = {"hourly_data": [], "last_updated_raw": dt_util.utcnow().isoformat(), "source_cups": self._cups}
            _LOGGER.info(f"No recent hourly data found for {self._cups} after filtering for last {HOURS_TO_DISPLAY}h.")
