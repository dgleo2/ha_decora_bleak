"""Light platform for Leviton Decora Bleak integration."""
import asyncio
import logging
import random
from typing import Any

from homeassistant.components.light import (
    ATTR_BRIGHTNESS,
    ColorMode,
    LightEntity,
    LightEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, CoreState
from homeassistant.const import EVENT_HOMEASSISTANT_STARTED
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.components.bluetooth import async_ble_device_from_address

from .decora_bleak_device import DecoraBLEDevice
from .models import DecoraBLEDeviceState, DecoraBLEDeviceSummary

from .const import DOMAIN, CONF_ADDRESS, CONF_API_KEY, DEFAULT_NAME

_LOGGER = logging.getLogger(__name__)


class DeviceInstance:
    """Wrapper for device and light entity for state management."""
    
    def __init__(self, device: DecoraBLEDevice, light_entity: 'DecoraBleakLight'):
        """Initialize the device instance."""
        self.device = device
        self.light_entity = light_entity
        self._reconnect_task_scheduled = False
        self._last_connection_attempt = 0.0
        self.address = device.address
        self.summary: DecoraBLEDeviceSummary | None = None

    def _on_connection(self, summary: DecoraBLEDeviceSummary) -> None:
        """Store device summary on connection for later retrieval."""
        self.summary = summary
    
    async def _async_reconnect_on_discovery(self) -> None:
        """Handle reconnection when device is rediscovered via Bluetooth."""
        import time
        
        # Check minimum interval between connection attempts (5 seconds)
        now = time.monotonic()
        if now - self._last_connection_attempt < 5.0:
            _LOGGER.debug("%s: Skipping reconnection, last attempt too recent (%.1fs ago)", 
                         self.address, now - self._last_connection_attempt)
            return
        
        if self._reconnect_task_scheduled:
            _LOGGER.debug("%s: Reconnection already scheduled", self.address)
            return
        
        self._reconnect_task_scheduled = True
        self._last_connection_attempt = now
        try:
            # Small stagger to avoid storm
            try:
                offset = (sum(ord(c) for c in self.light_entity._entry.entry_id) % 50) / 100.0  # 0..0.49s
            except Exception:
                offset = 0.2
            delay = 0.1 + offset
            _LOGGER.debug("%s: Reconnect after rediscovery in %.2fs", self.address, delay)
            await asyncio.sleep(delay)
            _LOGGER.info("Attempting to reconnect to %s after rediscovery", self.address)
            # Disconnect current instance
            if self.device.is_connected:
                await self.device.disconnect()
                await asyncio.sleep(1.0)
            
            # Attempt reconnection
            await self.light_entity._async_start_connection()
            _LOGGER.info("Successfully reconnected to %s", self.address)
        except Exception as ex:
            _LOGGER.debug("Reconnection failed for %s: %s", self.address, ex)
        finally:
            self._reconnect_task_scheduled = False

async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
):
    """Set up the light platform."""
    address = entry.data[CONF_ADDRESS]
    api_key = entry.data[CONF_API_KEY]
    _LOGGER.info("Setting up light platform for %s", address)

    # Get BLE device from HA Bluetooth integration
    ble_device = async_ble_device_from_address(hass, address, connectable=True)
    if ble_device is None:
        _LOGGER.info("Device %s not found during light setup; deferring until available", address)
        # Try registering a bluetooth rediscovery callback; fallback to polling
        try:
            from homeassistant.components.bluetooth import (
                async_register_callback,
                BluetoothCallbackMatcher,
                BluetoothChange,
                BluetoothServiceInfoBleak,
            )

            def _bt_cb(service_info: BluetoothServiceInfoBleak, change: BluetoothChange) -> None:
                if service_info.address != address:
                    return
                if not service_info.connectable:
                    return
                # Only act on connectable advertisements
                _LOGGER.debug("%s rediscovered via Bluetooth; creating entity", address)
                try:
                    # Unsubscribe first to avoid duplicate calls
                    if unsub:
                        unsub()
                except Exception:
                    pass
                # Create entity now
                asyncio.create_task(_async_wait_for_device_and_add(hass, entry, async_add_entities))

            matcher = BluetoothCallbackMatcher(address=address)
            unsub = async_register_callback(hass, _bt_cb, matcher, connectable=True)
            entry.async_on_unload(unsub)
            _LOGGER.debug("Registered bluetooth rediscovery callback for %s", address)
        except Exception:
            # Fallback to polling defer
            asyncio.create_task(_async_wait_for_device_and_add(hass, entry, async_add_entities))
        return
    
    _LOGGER.debug("Found device %s (%s)", ble_device.name, ble_device.address)

    # Create the Decora device
    _LOGGER.debug("Creating DecoraBLEDevice instance")
    decora_device = DecoraBLEDevice(ble_device, api_key)

    # Create the light entity
    _LOGGER.debug("Creating light entity")
    light = DecoraBleakLight(decora_device, entry)

    # Create device instance wrapper
    device_instance = DeviceInstance(decora_device, light)

    # Store the device instance in hass.data for later access
    hass.data[DOMAIN][entry.entry_id] = device_instance

    # Register a connection callback to capture summary in the instance
    try:
        decora_device.register_connection_callback(device_instance._on_connection)
    except Exception:
        pass

    async_add_entities([light], True)
    _LOGGER.info("Light entity added for %s", address)


async def _async_wait_for_device_and_add(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Wait until the BLE device becomes available, then create and add the entity."""
    address = entry.data[CONF_ADDRESS]
    api_key = entry.data[CONF_API_KEY]
    attempt = 0
    while True:
        ble_device = async_ble_device_from_address(hass, address, connectable=True)
        if ble_device is not None:
            _LOGGER.info("Deferred setup: found device %s, creating entity", address)
            decora_device = DecoraBLEDevice(ble_device, api_key)
            light = DecoraBleakLight(decora_device, entry)
            device_instance = DeviceInstance(decora_device, light)
            hass.data[DOMAIN][entry.entry_id] = device_instance
            try:
                decora_device.register_connection_callback(device_instance._on_connection)
            except Exception:
                pass
            async_add_entities([light], True)
            _LOGGER.info("Deferred setup: light entity added for %s", address)
            return
        # Staggered retry delay
        base = 0.5
        deterministic = (sum(ord(c) for c in entry.entry_id) % 50) / 100.0
        jitter = random.random() * 0.2  # 0..0.2
        backoff = min(attempt * 0.5, 2.0)
        delay = base + deterministic + jitter + backoff
        _LOGGER.debug("Device %s not yet available; retry in %.2fs (attempt %d)", address, delay, attempt + 1)
        await asyncio.sleep(delay)
        attempt += 1



class DecoraBleakLight(LightEntity):
    """Representation of a Leviton Decora Bluetooth light."""

    _attr_color_mode = ColorMode.BRIGHTNESS
    _attr_supported_color_modes = {ColorMode.BRIGHTNESS}
    _attr_supported_features = LightEntityFeature.TRANSITION
    _attr_has_entity_name = True
    _attr_name = None

    def __init__(self, device: DecoraBLEDevice, entry: ConfigEntry) -> None:
        """Initialize the light."""
        self._device = device
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_light"
        self._summary: DecoraBLEDeviceSummary | None = None
        self._state: DecoraBLEDeviceState | None = None
        self._connected = False
        self._last_available = False
        self._unsub_state = None
        self._unsub_connection = None
        
        _LOGGER.debug("Initializing light entity for %s", device.address)

    def _connection_updated(self, summary: DecoraBLEDeviceSummary) -> None:
        """Handle connection update."""
        self._summary = summary
        self._connected = True
        _LOGGER.debug("Device connected: %s", summary)
        # Update device registry with model and firmware once we have them
        try:
            device_registry = dr.async_get(self.hass)
            device_entry = device_registry.async_get_device(identifiers={(DOMAIN, self._device.address)})
            if device_entry:
                device_registry.async_update_device(
                    device_entry.id,
                    manufacturer=summary.manufacturer,
                    model=summary.model,
                    sw_version=summary.software_revision,
                    name=f"{summary.manufacturer} {summary.model}",
                )
                _LOGGER.debug("%s: Device registry updated with model=%s sw=%s", self.entity_id, summary.model, summary.software_revision)
        except Exception as ex:
            _LOGGER.debug("%s: Failed to update device registry: %s", self.entity_id, ex)
        self.async_write_ha_state()

    def _state_updated(self, state: DecoraBLEDeviceState) -> None:
        """Handle state update."""
        _LOGGER.debug("State updated: %s", state)
        self._state = state
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        if self._summary:
            return DeviceInfo(
                identifiers={(DOMAIN, self._device.address)},
                name=f"{self._summary.manufacturer} {self._summary.model}",
                manufacturer=self._summary.manufacturer,
                model=self._summary.model,
                sw_version=self._summary.software_revision,
            )
        # If instance has a cached summary, use it
        try:
            instance = self.hass.data.get(DOMAIN, {}).get(self._entry.entry_id)
            if instance and getattr(instance, "summary", None):
                s = instance.summary
                return DeviceInfo(
                    identifiers={(DOMAIN, self._device.address)},
                    name=f"{s.manufacturer} {s.model}",
                    manufacturer=s.manufacturer,
                    model=s.model,
                    sw_version=s.software_revision,
                )
        except Exception:
            pass
        # Use BLE device name as fallback
        device_name = getattr(self._device._device, 'name', None) or DEFAULT_NAME
        return DeviceInfo(
            identifiers={(DOMAIN, self._device.address)},
            name=device_name,
            manufacturer="Leviton",
        )

    @property
    def is_on(self) -> bool | None:
        """Return true if light is on."""
        if self._state is None:
            return None
        return self._state.is_on

    @property
    def brightness(self) -> int | None:
        """Return the brightness of this light between 0..255."""
        if self._state is None:
            return None
        # Map from 0-100 to 0-255
        return int((self._state.brightness_level / 100) * 255)

    @property
    def available(self) -> bool:
        """Return true if device is available."""
        is_available = self._connected and self._device.is_connected
        if hasattr(self, '_last_available') and self._last_available != is_available:
            _LOGGER.debug("%s: Availability changed to %s", self.entity_id, is_available)
        self._last_available = is_available
        return is_available

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on the light."""
        brightness = kwargs.get(ATTR_BRIGHTNESS)
        _LOGGER.debug("%s: Turn on called (brightness=%s)", self.entity_id, brightness)
        
        if not self._device.is_connected:
            _LOGGER.debug("%s: Device not connected, connecting...", self.entity_id)
            try:
                await self._device.connect()
            except Exception as ex:
                _LOGGER.error("%s: Failed to connect for turn_on: %s", self.entity_id, ex)
                raise

        if brightness is not None:
            # Map from 0-255 to 0-100
            brightness_level = int((brightness / 255) * 100)
            _LOGGER.debug("%s: Setting brightness to %d%%", self.entity_id, brightness_level)
            await self._device.turn_on(brightness_level=brightness_level)
        else:
            _LOGGER.debug("%s: Turning on (no brightness specified)", self.entity_id)
            await self._device.turn_on()
        # State will be updated via the state callback

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the light."""
        _LOGGER.debug("%s: Turn off called", self.entity_id)
        
        if not self._device.is_connected:
            _LOGGER.debug("%s: Device not connected, connecting...", self.entity_id)
            try:
                await self._device.connect()
            except Exception as ex:
                _LOGGER.error("%s: Failed to connect for turn_off: %s", self.entity_id, ex)
                raise

        await self._device.turn_off()
        # State will be updated via the state callback

    async def async_will_remove_from_hass(self) -> None:
        """Disconnect when removing from Home Assistant."""
        _LOGGER.info("%s: Removing entity from Home Assistant", self.entity_id)
        
        # Unregister callbacks if they were registered
        if self._unsub_state:
            self._unsub_state()
        if self._unsub_connection:
            self._unsub_connection()

        # Stop the device to ensure clean shutdown
        if self._device._running:
            self._device._running = False
            
        if self._device.is_connected:
            _LOGGER.debug("%s: Disconnecting device", self.entity_id)
            try:
                await self._device.disconnect()
            except Exception as ex:
                _LOGGER.debug("%s: Error disconnecting device: %s", self.entity_id, ex)
        _LOGGER.debug("%s: Entity removed", self.entity_id)

    async def async_added_to_hass(self) -> None:
        """When entity is added to hass."""
        _LOGGER.info("%s: Entity added to Home Assistant", self.entity_id)
        
        # Register callbacks NOW that entity is added to hass
        _LOGGER.debug("%s: Registering callbacks", self.entity_id)
        self._unsub_state = self._device.register_state_callback(self._state_updated)
        self._unsub_connection = self._device.register_connection_callback(
            self._connection_updated
        )
        _LOGGER.debug("Callbacks registered for %s", self.entity_id)
        
        try:
            # Defer connection until HA core has fully started, then stagger
            if self.hass.state != CoreState.running:
                _LOGGER.debug("%s: HA not fully started; will connect after startup", self.entity_id)
                self.hass.bus.async_listen_once(
                    EVENT_HOMEASSISTANT_STARTED, self._async_on_ha_started
                )
            else:
                _LOGGER.debug("%s: HA running; scheduling delayed initial connect", self.entity_id)
                asyncio.create_task(self._async_delayed_initial_connect())
        except Exception as ex:
            _LOGGER.error("%s: Error scheduling device connection: %s", self.entity_id, ex, exc_info=True)

    async def _async_connect_device(self) -> None:
        """Connect to the device."""
        try:
            await self._device.connect()
            _LOGGER.debug("Device connected successfully")
        except Exception as ex:
            _LOGGER.error("Failed to connect to device: %s", ex)

    async def _async_delayed_initial_connect(self) -> None:
        """Start device and connect after a small delay to avoid reload storms."""
        try:
            # Wait for Bluetooth integration to be fully ready
            if not await self._wait_for_bluetooth_ready():
                _LOGGER.warning("%s: Bluetooth integration not ready, delaying connection", self.entity_id)
                # Try again after additional delay
                await asyncio.sleep(5.0)
            
            delay = self._compute_stagger_delay()
            _LOGGER.debug("%s: Staggered initial connect in %.2fs", self.entity_id, delay)
            await asyncio.sleep(delay)
            await self._start_and_connect()
        except Exception as ex:
            _LOGGER.debug("%s: Delayed initial connect failed: %s", self.entity_id, ex)

    async def _async_on_ha_started(self, _event=None) -> None:
        """Callback when Home Assistant has fully started; begin connection."""
        _LOGGER.debug("%s: HA started; scheduling staggered initial connect", self.entity_id)
        # Add small delay after HA started to let integrations settle
        await asyncio.sleep(2.0)
        asyncio.create_task(self._async_delayed_initial_connect())

    async def _wait_for_bluetooth_ready(self) -> bool:
        """Wait for Bluetooth integration to be ready.
        
        Returns True if ready, False if timeout.
        """
        try:
            from homeassistant.components import bluetooth
            
            # Check if bluetooth integration is loaded
            max_wait = 10  # seconds
            waited = 0
            check_interval = 0.5
            
            while waited < max_wait:
                try:
                    # Try to get a BLE device - if this works, bluetooth is ready
                    device = async_ble_device_from_address(self.hass, self._device.address, connectable=True)
                    if device is not None:
                        _LOGGER.debug("%s: Bluetooth integration is ready", self.entity_id)
                        return True
                    
                    # Device not found yet, but integration might still be initializing
                    await asyncio.sleep(check_interval)
                    waited += check_interval
                except Exception as ex:
                    _LOGGER.debug("%s: Bluetooth not ready yet: %s", self.entity_id, ex)
                    await asyncio.sleep(check_interval)
                    waited += check_interval
            
            # Timeout - proceed anyway but log warning
            _LOGGER.warning("%s: Bluetooth readiness check timed out after %ds", self.entity_id, max_wait)
            return False
        except Exception as ex:
            _LOGGER.debug("%s: Error checking bluetooth readiness: %s", self.entity_id, ex)
            # If we can't check, assume it's ready and proceed
            return True

    async def _start_and_connect(self) -> None:
        """Ensure device is started, then connect."""
        # Avoid double-start
        if not self._device.is_running:
            _LOGGER.debug("%s: Starting device prior to connect", self.entity_id)
            await self._device.start()
        else:
            _LOGGER.debug("%s: Device already running; skipping start", self.entity_id)
        asyncio.create_task(self._async_connect_device())

    def _compute_stagger_delay(self) -> float:
        """Compute a deterministic staggered delay per device with small jitter.

        Base 1.0s + deterministic 0..0.5s from entry_id + random 0..0.2s jitter.
        """
        try:
            base = 1.0  # Increased from 0.5s to give BLE proxy more time
            deterministic = (sum(ord(c) for c in self._entry.entry_id) % 50) / 100.0
            jitter = random.random() * 0.2  # 0..0.2
            return base + deterministic + jitter
        except Exception:
            return 1.2

    async def _async_start_connection(self) -> None:
        """Start or restart device connection (used for reconnection on rediscovery)."""
        try:
            _LOGGER.debug("%s: Starting connection after rediscovery", self.entity_id)
            
            # Ensure Bluetooth is ready before reconnecting
            if not await self._wait_for_bluetooth_ready():
                _LOGGER.warning("%s: Bluetooth not ready for reconnection, will retry later", self.entity_id)
                return
            
            # Stop current connection if running
            if self._device.is_connected:
                try:
                    await self._device.disconnect()
                    await asyncio.sleep(1.0)
                except Exception as ex:
                    _LOGGER.debug("Error disconnecting before reconnection: %s", ex)

            # Ensure started only once
            if not self._device.is_running:
                await self._device.start()

            await self._device.connect()
            _LOGGER.info("%s: Successfully reconnected after rediscovery", self.entity_id)
        except Exception as ex:
            _LOGGER.error("%s: Failed to reconnect after rediscovery: %s", self.entity_id, ex, exc_info=True)
