"""Config flow for Datadis integration."""
import voluptuous as vol
from homeassistant import config_entries
from homeassistant.const import CONF_USERNAME, CONF_PASSWORD
from datadis_client import DatadisClient

from .const import DOMAIN, CONF_CUPS, CONF_DISTRIBUTOR_CODE, CONF_POINT_TYPE

DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_USERNAME): str,
        vol.Required(CONF_PASSWORD): str,
    }
)


class DatadisConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Datadis."""

    VERSION = 1
    # To store user input and fetched supplies between steps
    _user_input = None
    _supplies = None

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            try:
                client = DatadisClient(
                    username=user_input[CONF_USERNAME],
                    password=user_input[CONF_PASSWORD],
                )
                await self.hass.async_add_executor_job(client.authenticate)
                supplies = await self.hass.async_add_executor_job(client.get_supplies)

                if not supplies:
                    errors["base"] = "no_supplies"  # Error if no supplies found
                else:
                    self._user_input = user_input
                    self._supplies = supplies
                    # Proceed to the supply selection step
                    return await self.async_step_select_supply()

            except Exception:  # pylint: disable=broad-except
                # _LOGGER.exception("Unexpected exception") # Add proper logging
                errors["base"] = "cannot_connect"  # More specific error for connection/auth issues

        return self.async_show_form(
            step_id="user",
            data_schema=DATA_SCHEMA,
            errors=errors
        )

    async def async_step_select_supply(self, user_input=None):
        """Handle the supply selection step."""
        errors = {}

        # Ensure supplies were fetched and stored
        if not self._supplies:
            # This case should ideally not be reached if logic in async_step_user is correct
            # Redirect back to user step or show a general error
            errors["base"] = "missing_supplies"
            return self.async_show_form(step_id="user", data_schema=DATA_SCHEMA, errors=errors)

        if user_input is not None:
            selected_cups_value = user_input[CONF_CUPS]
            # Find the selected supply dictionary from the stored list
            selected_supply_details = next(
                (s for s in self._supplies if s.get("cups") == selected_cups_value), None  # type: ignore[union-attr]
            )

            if selected_supply_details:
                # Combine original user input (credentials) with selected supply details
                data_to_save = {
                    **self._user_input,  # type: ignore[misc]
                    CONF_CUPS: selected_supply_details.get("cups"),  # type: ignore[union-attr]
                    CONF_DISTRIBUTOR_CODE: selected_supply_details.get("distributorCode"),  # type: ignore[union-attr]
                    CONF_POINT_TYPE: selected_supply_details.get("pointType"),  # type: ignore[union-attr]
                }
                # Use the selected CUPS as the title for the config entry
                return self.async_create_entry(title=selected_cups_value, data=data_to_save)
            
            errors["base"] = "invalid_selection"  # Error if selection is somehow invalid

        # Prepare the schema for the selection form dynamically
        # Display CUPS and its address for easier identification
        supply_options = {
            supply.get("cups"): f"{supply.get('cups')} - {supply.get('address', 'N/A')}"  # type: ignore[union-attr]
            for supply in self._supplies  # type: ignore[union-attr]
        }
        
        select_supply_schema = vol.Schema(
            {vol.Required(CONF_CUPS): vol.In(supply_options)}
        )

        return self.async_show_form(
            step_id="select_supply",
            data_schema=select_supply_schema,
            errors=errors,
            description_placeholders={
                "num_supplies": str(len(self._supplies)),  # type: ignore[arg-type]
                # Potentially add other placeholders if your translations use them
            },
        )

    # If you want to support re-authentication or options flow:
    # @staticmethod
    # @callback
    # def async_get_options_flow(config_entry):
    #     return OptionsFlowHandler(config_entry)

# class OptionsFlowHandler(config_entries.OptionsFlow):
#     def __init__(self, config_entry: config_entries.ConfigEntry):
#         self.config_entry = config_entry
#
#     async def async_step_init(self, user_input=None):
#         # Example: allow users to change update interval
#         return self.async_show_form(step_id="init", data_schema=vol.Schema({}))
