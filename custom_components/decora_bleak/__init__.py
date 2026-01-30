"""The Leviton Decora Bleak integration."""
from __future__ import annotations

import logging
from typing import Final

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Event
from homeassistant.const import EVENT_HOMEASSISTANT_STOP

_LOGGER: logging.Logger = logging.getLogger(__name__)

DOMAIN: Final = "decora_bleak"

PLATFORMS = ["light"]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Leviton Decora Bleak from a config entry."""
    _LOGGER.info("Setting up Decora Bleak integration for %s", entry.data.get("address"))
    hass.data.setdefault(DOMAIN, {})
    
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    _LOGGER.debug("Platforms setup complete for %s", entry.entry_id)
    
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    
    # Register shutdown handler to ensure clean disconnect
    async def _async_stop(event: Event) -> None:
        _LOGGER.debug("Stopping Decora Bleak (%s) due to HA shutdown", entry.data.get("address"))
        if DOMAIN in hass.data and entry.entry_id in hass.data[DOMAIN]:
            device_instance = hass.data[DOMAIN].get(entry.entry_id)
            if device_instance and hasattr(device_instance, 'device'):
                try:
                    await device_instance.device.stop()
                except Exception as ex:
                    _LOGGER.debug("Error stopping device during shutdown: %s", ex)
    
    entry.async_on_unload(
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_stop)
    )
    
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    _LOGGER.info("Unloading Decora Bleak integration for %s", entry.data.get("address"))
    
    # Clean up device instance before unloading platforms
    if DOMAIN in hass.data and entry.entry_id in hass.data[DOMAIN]:
        device_instance = hass.data[DOMAIN].pop(entry.entry_id, None)
        if device_instance and device_instance.device:
            try:
                # Stop the device to ensure clean disconnect
                if device_instance.device._running:
                    device_instance.device._running = False
                if device_instance.device.is_connected:
                    _LOGGER.debug("Disconnecting device %s before unload", entry.data.get("address"))
                    await device_instance.device.disconnect()
            except Exception as ex:
                _LOGGER.debug("Error disconnecting device during unload: %s", ex)
    
        if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
            _LOGGER.debug("Successfully unloaded %s", entry.entry_id)
        else:
            _LOGGER.warning("Failed to unload platforms for %s", entry.entry_id)
    
    return unload_ok


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload config entry."""
    _LOGGER.info("Reloading Decora Bleak integration for %s", entry.data.get("address"))
    await async_unload_entry(hass, entry)
    await async_setup_entry(hass, entry)
