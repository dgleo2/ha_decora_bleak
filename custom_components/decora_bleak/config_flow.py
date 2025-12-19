"""Config flow for Leviton Decora Bleak integration."""
import asyncio
import logging
from typing import Any, Dict, Optional

import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.selector import SelectSelector, SelectSelectorConfig, SelectSelectorMode
from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_discovered_service_info,
    async_ble_device_from_address,
)
from homeassistant.helpers.device_registry import format_mac

from .decora_bleak_device import DecoraBLEDevice


from .const import DOMAIN, CONF_ADDRESS, CONF_API_KEY

_LOGGER = logging.getLogger(__name__)

# Leviton manufacturer ID
LEVITON_MANUFACTURER_ID = 0x05BC
MANUAL_MAC = "manual"

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ADDRESS): str,
        vol.Required(CONF_API_KEY): str,
    }
)


class DecoraBleakConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Leviton Decora Bleak."""

    VERSION = 1
    MINOR_VERSION = 0

    def __init__(self):
        """Initialize the config flow."""
        self._discovered_devices: Dict[str, Dict[str, Any]] = {}
        self._selected_address: Optional[str] = None
        self._discovery_info: Optional[BluetoothServiceInfoBleak] = None

    # =========================================================
    # Bluetooth Auto-Discovery
    # =========================================================
    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> FlowResult:
        """Handle discovered BLE device via Bluetooth integration."""
        _LOGGER.debug(
            "Discovered Bluetooth device: %s (%s)",
            discovery_info.name,
            discovery_info.address,
        )

        if not discovery_info.address or not discovery_info.name:
            return self.async_abort(reason="invalid_discovery_info")

        # Check for Leviton manufacturer ID
        if discovery_info.manufacturer_data:
            if LEVITON_MANUFACTURER_ID not in discovery_info.manufacturer_data:
                _LOGGER.debug(
                    "Device %s not a Leviton device (not in manufacturer data)",
                    discovery_info.address
                )
                return self.async_abort(reason="not_supported")
        else:
            _LOGGER.debug(
                "Device %s has no manufacturer data",
                discovery_info.address
            )
            return self.async_abort(reason="not_supported")

        await self.async_set_unique_id(discovery_info.address)
        _LOGGER.info(
            "Found Leviton Decora device: %s (%s)",
            discovery_info.name,
            discovery_info.address,
        )

        # Check if configuration already exists for this device (rediscovery)
        existing_entries = self.hass.config_entries.async_entries(DOMAIN)
        for entry in existing_entries:
            if entry.data.get(CONF_ADDRESS) == discovery_info.address:
                _LOGGER.info(
                    "Rediscovered Leviton Decora device: %s (%s)",
                    discovery_info.name,
                    discovery_info.address,
                )
                
                # Trigger reconnection if instance exists
                if DOMAIN in self.hass.data and entry.entry_id in self.hass.data[DOMAIN]:
                    device_instance = self.hass.data[DOMAIN][entry.entry_id]
                    _LOGGER.debug("Triggering reconnection for %s via Bluetooth discovery", discovery_info.address)
                    # Schedule a reconnection attempt in background
                    asyncio.create_task(device_instance._async_reconnect_on_discovery())
                
                return self.async_abort(reason="already_configured")
        
        # Not yet configured, proceed with setup flow
        self._abort_if_unique_id_configured()

        # Store discovery info and proceed to user confirmation
        self._discovery_info = discovery_info
        self._discovered_devices[discovery_info.address] = {
            "name": discovery_info.name,
            "address": discovery_info.address,
            "device": None,  # Will be set in next step
        }

        return await self.async_step_bluetooth_confirm()

    async def async_step_bluetooth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Confirm adding BLE device."""
        self._set_confirm_only()
        return await self.async_step_user()

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle the user step - scan for devices or select from list."""
        errors: Dict[str, str] = {}

        if user_input is not None:
            if user_input.get("address") == MANUAL_MAC:
                return await self.async_step_manual()
            else:
                # Device selected from list
                self._selected_address = user_input["address"]
                return await self.async_step_api_key()

        # Scan for Leviton devices using Home Assistant's Bluetooth integration
        if not self._discovered_devices:
            _LOGGER.info("Scanning for Leviton Decora devices using HA Bluetooth integration...")
            
            # Get discovered devices from HA Bluetooth integration
            current_addresses = self._async_current_ids()
            discovered_devices = async_discovered_service_info(self.hass)
            
            leviton_count = 0
            for device_info in discovered_devices:
                if device_info.address in current_addresses:
                    _LOGGER.debug("Device %s already configured", device_info.address)
                    continue
                
                # Check for Leviton manufacturer ID
                if device_info.manufacturer_data and LEVITON_MANUFACTURER_ID in device_info.manufacturer_data:
                    device_name = device_info.name or f"Leviton Device {device_info.address}"
                    _LOGGER.debug(
                        "Found Leviton device: %s (%s)",
                        device_name,
                        device_info.address,
                    )
                    self._discovered_devices[device_info.address] = {
                        "name": device_name,
                        "address": device_info.address,
                        "device": None,
                    }
                    leviton_count += 1
            
            if leviton_count > 0:
                _LOGGER.info("Found %d Leviton device(s)", leviton_count)
            else:
                _LOGGER.warning("No Leviton devices found with company ID 0x%04X", LEVITON_MANUFACTURER_ID)
        
        if not self._discovered_devices:
            _LOGGER.info("No devices discovered, allowing manual entry")
            return await self.async_step_manual()
        
        # Build selection options with detailed device information
        device_options = [
            {
                "label": f"{info['name']} ({info['address']})",
                "value": address,
            }
            for address, info in self._discovered_devices.items()
        ]
        
        # Add manual entry option
        device_options.append({"label": "Manual Entry", "value": MANUAL_MAC})
        
        data_schema = vol.Schema(
            {
                vol.Required("address"): SelectSelector(
                    SelectSelectorConfig(
                        options=device_options,
                        mode=SelectSelectorMode.DROPDOWN,
                    )
                )
            }
        )

        # Build device list for banner display
        device_list = "\n".join([
            f"• {info['name']}\n  {info['address']}"
            for address, info in self._discovered_devices.items()
        ])

        return self.async_show_form(
            step_id="user",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={
                "device_count": str(len(self._discovered_devices)),
                "device_list": device_list,
            },
        )

    async def async_step_api_key(
        self, user_input: Optional[Dict[str, Any]] | None = None
    ) -> FlowResult:
        """Handle API key entry for selected device."""
        errors: Dict[str, str] = {}

        if user_input is not None:
            # Check if user clicked auto-retrieve button
            if user_input.get("auto_retrieve"):
                return await self.async_step_retrieve_key()
            
            device_info = self._discovered_devices.get(self._selected_address)
            if not device_info:
                _LOGGER.error("Selected device %s not found in discovered devices", self._selected_address)
                errors["base"] = "unknown"
            else:
                # Get the actual BLE device
                device = await self.async_get_device_from_address(self._selected_address)
                if not device:
                    _LOGGER.error("Cannot get BLE device for %s", self._selected_address)
                    errors["base"] = "cannot_connect"
                else:
                    try:
                        _LOGGER.info("Validating API key for %s", device_info['name'])
                        await self.async_validate_connection(
                            device, 
                            user_input[CONF_API_KEY]
                        )
                    except CannotConnect as ex:
                        _LOGGER.error("Cannot connect: %s", ex)
                        errors["base"] = "cannot_connect"
                    except InvalidAuth as ex:
                        _LOGGER.error("Invalid authentication: %s", ex)
                        errors["base"] = "invalid_auth"
                    except Exception as ex:
                        _LOGGER.exception("Unexpected exception during validation\"")
                        errors["base"] = "unknown"
                    else:
                        _LOGGER.info("✓ Validation successful, creating config entry")
                        await self.async_set_unique_id(self._selected_address)
                        self._abort_if_unique_id_configured()

                        return self.async_create_entry(
                            title=f"{device_info['name']}",
                            data={
                                CONF_ADDRESS: self._selected_address,
                                CONF_API_KEY: user_input[CONF_API_KEY],
                            },
                        )
        
        device_info = self._discovered_devices.get(self._selected_address, {})
        device_name = device_info.get("name", self._selected_address)

        data_schema = vol.Schema(
            {
                vol.Optional(CONF_API_KEY): str,
                vol.Optional("auto_retrieve", default=False): bool,
            }
        )

        return self.async_show_form(
            step_id="api_key",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={"device_name": device_name, "address": self._selected_address},
        )

    async def async_step_retrieve_key(
        self, user_input: Optional[Dict[str, Any]] = None
    ) -> FlowResult:
        """Automatically retrieve API key from device in pairing mode."""
        errors: Dict[str, str] = {}
        
        device_info = self._discovered_devices.get(self._selected_address)
        if not device_info:
            _LOGGER.error("Selected device %s not found", self._selected_address)
            return await self.async_step_api_key({"error": "Device not found"})
        
        # Get the actual BLE device
        device = await self.async_get_device_from_address(self._selected_address)
        if not device:
            _LOGGER.error("Cannot get BLE device for %s", self._selected_address)
            errors["base"] = "cannot_connect"
        else:
            _LOGGER.info("Attempting to retrieve API key from %s", self._selected_address)
            
            try:
                # Get API key from device (must be in pairing mode)
                from .exceptions import DeviceNotInPairingModeError
                
                api_key = await DecoraBLEDevice.get_api_key(device)
                _LOGGER.info("Successfully retrieved API key from %s: %s", self._selected_address, api_key)
                
                # Validate the connection with retrieved key
                try:
                    _LOGGER.info("Validating retrieved API key for %s", device_info['name'])
                    await self.async_validate_connection(device, api_key)
                except CannotConnect as ex:
                    _LOGGER.error("Cannot connect with retrieved key: %s", ex)
                    errors["base"] = "cannot_connect"
                except InvalidAuth as ex:
                    _LOGGER.error("Retrieved API key validation failed: %s", ex)
                    errors["base"] = "invalid_auth"
                except Exception as ex:
                    _LOGGER.error("Unexpected error during validation: %s", ex, exc_info=True)
                    errors["base"] = "unknown"
                else:
                    # Success! Create entry automatically
                    _LOGGER.info("✓ Retrieved API key validated, creating config entry")
                    await self.async_set_unique_id(self._selected_address)
                    self._abort_if_unique_id_configured()
                    
                    return self.async_create_entry(
                        title=f"{device_info['name']}",
                        data={
                            CONF_ADDRESS: self._selected_address,
                            CONF_API_KEY: api_key,
                        },
                    )
                    
            except DeviceNotInPairingModeError:
                _LOGGER.warning("Device %s is not in pairing mode", self._selected_address)
                errors["base"] = "not_in_pairing_mode"
            except Exception as ex:
                _LOGGER.error("Failed to retrieve API key from %s: %s", self._selected_address, ex, exc_info=True)
                errors["base"] = "cannot_retrieve_key"
        
        # If retrieval failed, show api_key form with error
        device_name = device_info.get("name", self._selected_address)
        
        data_schema = vol.Schema(
            {
                vol.Optional(CONF_API_KEY): str,
                vol.Optional("auto_retrieve", default=False): bool,
            }
        )
        
        return self.async_show_form(
            step_id="api_key",
            data_schema=data_schema,
            errors=errors,
            description_placeholders={"device_name": device_name, "address": self._selected_address},
        )

    async def async_step_manual(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle manual device entry."""
        errors: Dict[str, str] = {}

        if user_input is not None:
            try:
                _LOGGER.info("Validating manual entry for %s", user_input[CONF_ADDRESS])
                await self.async_validate_input(user_input)
            except CannotConnect as ex:
                _LOGGER.error("Cannot connect: %s", ex)
                errors["base"] = "cannot_connect"
            except InvalidAuth as ex:
                _LOGGER.error("Invalid authentication: %s", ex)
                errors["base"] = "invalid_auth"
            except Exception as ex:
                _LOGGER.exception("Unexpected exception: %s", ex)
                errors["base"] = "unknown"
            else:
                _LOGGER.info("✓ Manual entry validated, creating config entry")
                await self.async_set_unique_id(user_input[CONF_ADDRESS])
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=f"Decora Light {user_input[CONF_ADDRESS]}",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="manual",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
            description_placeholders={},
        )

    async def async_validate_connection(self, device, api_key: str) -> None:
        """Validate connection to a specific device."""
        _LOGGER.info("Testing connection to %s with provided API key", device.address)
        
        from .exceptions import (
            DeviceConnectionError,
            IncorrectAPIKeyError,
        )
        
        decora_device = DecoraBLEDevice(device, api_key)
        try:
            _LOGGER.debug("Attempting to connect...")
            await decora_device.connect()
            _LOGGER.info("✓ Successfully connected to %s", device.address)
            
            _LOGGER.debug("Verifying device responds correctly...")
            # Make sure we can read the state (validates unlock worked)
            if decora_device.is_connected:
                _LOGGER.debug("✓ Device unlocked and responding")
            
            await decora_device.disconnect()
            _LOGGER.info("✓ Connection test completed successfully")
            
        except IncorrectAPIKeyError as ex:
            _LOGGER.error("Invalid API key for %s", device.address)
            raise InvalidAuth(f"The API key is incorrect. Device rejected authentication.")
            
        except DeviceConnectionError as ex:
            _LOGGER.error("Connection failed for %s: %s", device.address, ex)
            raise CannotConnect(f"Unable to connect to device: {str(ex)}")
            
        except Exception as ex:
            _LOGGER.error("Unexpected error validating %s: %s", device.address, ex, exc_info=True)
            raise InvalidAuth(f"Connection test failed: {str(ex)}")

    async def async_get_device_from_address(self, address: str):
        """Get BLE device from address using HA Bluetooth integration."""
        _LOGGER.debug("Getting BLE device for %s from HA Bluetooth integration", address)
        
        # First try to get from discovered devices cache
        if address in self._discovered_devices and self._discovered_devices[address].get("device"):
            return self._discovered_devices[address]["device"]
        
        # Try to get from HA Bluetooth integration
        device = async_ble_device_from_address(self.hass, address)
        if device:
            _LOGGER.debug("Found device %s in HA Bluetooth integration", address)
            return device
        
        _LOGGER.warning("Device %s not found", address)
        return None

    async def async_validate_input(self, user_input: Dict[str, Any]) -> None:
        """Validate the user input allows us to connect."""
        address = format_mac(user_input[CONF_ADDRESS])
        api_key = user_input[CONF_API_KEY]
        _LOGGER.info("Validating connection to %s", address)

        # Get the BLE device
        device = await self.async_get_device_from_address(address)
        if device is None:
            _LOGGER.warning("Device %s not found", address)
            raise CannotConnect("Device not found")
        
        _LOGGER.debug("Found device %s (%s)", device.name, device.address)

        # Validate connection
        await self.async_validate_connection(device, api_key)


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate invalid authentication."""
