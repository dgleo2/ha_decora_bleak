from __future__ import annotations
import contextlib

import async_timeout

import asyncio
import logging
from collections.abc import Callable
from typing import Optional
from dataclasses import replace

from bleak import BleakClient, BleakGATTCharacteristic
from bleak.backends.device import BLEDevice
from bleak_retry_connector import BLEAK_RETRY_EXCEPTIONS as BLEAK_EXCEPTIONS, establish_connection

from .device_const import EVENT_CHARACTERISTIC_UUID, STATE_CHARACTERISTIC_UUID, UNPAIRED_API_KEY, SYSTEM_ID_DESCRIPTOR_UUID, MODEL_NUMBER_DESCRIPTOR_UUID, SOFTWARE_REVISION_DESCRIPTOR_UUID, MANUFACTURER_DESCRIPTOR_UUID, WAIT_FOR_DEVICE_CONNECTION_TIMEOUT
from .exceptions import DeviceConnectionError, DeviceConnectionTimeoutError, DeviceNotInPairingModeError, IncorrectAPIKeyError
from .models import DecoraBLEDeviceState, DecoraBLEDeviceSummary

_LOGGER = logging.getLogger(__name__)


class DecoraBLEDevice():
    def __init__(self, device: BLEDevice, api_key: str):
        self._device = device
        self._key = bytearray.fromhex(api_key)

        self._running = False
        self._device_connection_future: asyncio.Future[None] | None = None

        self._client = None
        self._summary = None
        self._state = DecoraBLEDeviceState()
        self._connection_callbacks: list[Callable[[
            DecoraBLEDeviceSummary], None]] = []
        self._state_callbacks: list[Callable[[
            DecoraBLEDeviceState], None]] = []
        # Connection coordination
        self._connect_lock = asyncio.Lock()
        self._connection_task: asyncio.Task | None = None

    @staticmethod
    async def get_api_key(device: BLEDevice) -> str:
        async with BleakClient(device) as client:
            await client.write_gatt_char(EVENT_CHARACTERISTIC_UUID, bytearray([0x22, 0x53, 0x00, 0x00, 0x00, 0x00, 0x00]), response=True)
            rawkey = await client.read_gatt_char(EVENT_CHARACTERISTIC_UUID)
            _LOGGER.debug("Raw API key from device: %s", repr(rawkey))

            if rawkey[2:6] != UNPAIRED_API_KEY:
                return bytearray(rawkey)[2:].hex()
            else:
                raise DeviceNotInPairingModeError

    async def start(self) -> Callable[[], None]:
        """Start watching for updates."""
        if self._running:
            raise RuntimeError("Already running")
        self._running = True
        self._device_connection_future = asyncio.get_running_loop().create_future()

        def _cancel() -> None:
            self._running = False

        return _cancel

    async def wait_for_device_connection(self) -> None:
        if not self._running:
            raise RuntimeError("Not running")
        if not self._device_connection_future:
            raise RuntimeError("Already waited for first update")
        try:
            async with async_timeout.timeout(WAIT_FOR_DEVICE_CONNECTION_TIMEOUT):
                await self._device_connection_future
        except (asyncio.TimeoutError, asyncio.CancelledError) as ex:
            self._device_connection_future.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._device_connection_future
            raise DeviceConnectionTimeoutError(
                "No advertisement received before timeout"
            ) from ex
        finally:
            self._device_connection_future = None

    def register_connection_callback(
        self, callback: Callable[[DecoraBLEDeviceSummary], None]
    ) -> Callable[[], None]:
        def unregister_callback() -> None:
            self._connection_callbacks.remove(callback)

        self._connection_callbacks.append(callback)

        if self._summary is not None:
            callback(self._summary)

        return unregister_callback

    def register_state_callback(
        self, callback: Callable[[DecoraBLEDeviceState], None]
    ) -> Callable[[], None]:
        def unregister_callback() -> None:
            self._state_callbacks.remove(callback)

        self._state_callbacks.append(callback)

        if self._state is not None:
            callback(self._state)

        return unregister_callback

    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_connected

    @property
    def is_running(self) -> bool:
        """Return whether the device is running."""
        return self._running

    @property
    def address(self) -> str:
        """Return the BLE device address."""
        return self._device.address

    @property
    def summary(self) -> Optional[DecoraBLEDeviceSummary]:
        return self._summary

    def update_device(self, device: BLEDevice) -> None:
        self._device = device

    async def connect(self) -> None:
        """Establish BLE connection with idempotency and coordination."""
        # Fast path: already connected
        if self.is_connected:
            return

        async with self._connect_lock:
            # Re-check after acquiring lock
            if self.is_connected:
                return

            # If a connection task is already running, wait for it
            if self._connection_task and not self._connection_task.done():
                _LOGGER.debug("%s: connect() already in progress, waiting", self._device.address)
                try:
                    await self._connection_task
                    return
                except Exception:
                    # Task failed, continue to try new connection
                    pass

            # Create new connection task
            self._connection_task = asyncio.create_task(self._do_connect())
            try:
                await self._connection_task
            finally:
                self._connection_task = None

    async def _do_connect(self) -> None:
        """Actual connection implementation."""
        device = self._device
        _LOGGER.debug("attempting to connect to %s using key: %s",
                      device.address, self._key.hex())

        def disconnected(client):
            _LOGGER.info("Device disconnected: %s", device.address)
            self._disconnect_cleanup()

        try:
            self._client = await establish_connection(
                BleakClient,
                self._device,
                self._device.name,
                disconnected,
                use_services_cache=True,
            )

            await self._unlock()
        except IncorrectAPIKeyError:
            # Re-raise API key errors as-is
            self._handle_device_connection(IncorrectAPIKeyError())
            if self._client:
                with contextlib.suppress(Exception):
                    await self._client.disconnect()
            self._disconnect_cleanup()
            raise
        except Exception as ex:
            _LOGGER.error("Failed to establish connection or unlock %s: %s", device.address, ex, exc_info=True)
            self._handle_device_connection(ex)
            # Clean up client if connection or unlock failed
            if self._client:
                with contextlib.suppress(Exception):
                    await self._client.disconnect()
            self._disconnect_cleanup()
            raise DeviceConnectionError(str(ex)) from ex

        try:
            # Ensure client is still valid after unlock
            if not self._client or not self._client.is_connected:
                raise DeviceConnectionError("Client disconnected after unlock")
            
            # Wait for device to process unlock before attempting operations
            # This is critical - device needs time to update authorization state
            await asyncio.sleep(0.5)
            
            # Verify unlock worked by attempting a test read
            try:
                await self._client.read_gatt_char(STATE_CHARACTERISTIC_UUID)
                _LOGGER.debug("Unlock verification successful for %s", device.address)
            except Exception as ex:
                _LOGGER.error("Unlock verification failed for %s: %s", device.address, ex)
                raise IncorrectAPIKeyError from ex
            
            # Issues in unlocking will be seen when first interacting with the device
            await self._register_for_state_notifications()

            self._summary = await self._summarize()
            self._handle_device_connection(None)
            self._fire_connection_callbacks(self._summary)

            await self._read_state()

            _LOGGER.info("Successfully connected to %s", device.address)
            _LOGGER.debug("Finished connecting %s", self._client.is_connected)
        except BLEAK_EXCEPTIONS as ex:
            _LOGGER.error("Incorrect API key or BLE error for %s: %s", device.address, ex, exc_info=True)
            self._handle_device_connection(ex)
            # Clean up on authentication/BLE error
            if self._client:
                with contextlib.suppress(Exception):
                    await self._client.disconnect()
            self._disconnect_cleanup()
            raise IncorrectAPIKeyError

    async def disconnect(self) -> None:
        """Disconnect the BLE client if connected."""
        try:
            if self._client and getattr(self._client, "is_connected", False):
                await self._client.disconnect()
        finally:
            # Ensure cleanup even if client was None or already disconnected
            self._disconnect_cleanup()

    async def _summarize(self) -> DecoraBLEDeviceSummary:
        system_identifier = await self.read_summary_descriptor(
            "system_identifier", SYSTEM_ID_DESCRIPTOR_UUID)
        manufacturer = await self.read_summary_descriptor(
            "manufacturer", MANUFACTURER_DESCRIPTOR_UUID)
        model = await self.read_summary_descriptor(
            "model", MODEL_NUMBER_DESCRIPTOR_UUID)
        software_revision = await self.read_summary_descriptor(
            "software_revision", SOFTWARE_REVISION_DESCRIPTOR_UUID)

        return DecoraBLEDeviceSummary(
            system_identifier=system_identifier.hex(),
            manufacturer=manufacturer.decode('utf-8'),
            model=model.decode('utf-8'),
            software_revision=self._revision_string(
                software_revision, "Software Revision"),
        )

    async def read_summary_descriptor(self, descriptor: str, descriptor_uuid: str) -> bytearray:
        """Read descriptor with retry logic for authorization timing issues."""
        max_retries = 3
        retry_delay = 0.3
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                raw_response = await self._client.read_gatt_char(descriptor_uuid)
                _LOGGER.debug("Raw %s from device: %s", descriptor, repr(raw_response))
                return raw_response
            except BLEAK_EXCEPTIONS as ex:
                last_exception = ex
                error_msg = str(ex).lower()
                
                # Check if it's an authorization error that might resolve with retry
                if "authorization" in error_msg or "authentication" in error_msg:
                    _LOGGER.warning(
                        "Authorization error reading %s (attempt %d/%d): %s",
                        descriptor, attempt + 1, max_retries, ex
                    )
                    if attempt < max_retries - 1:
                        # Give device more time to process authorization
                        await asyncio.sleep(retry_delay)
                        # Check if still connected
                        if not self._client or not self._client.is_connected:
                            raise DeviceConnectionError(f"Client disconnected while reading {descriptor}") from ex
                    continue
                else:
                    # Non-authorization error, don't retry
                    raise
        
        # All retries exhausted
        _LOGGER.error("Failed to read %s after %d attempts", descriptor, max_retries)
        raise last_exception if last_exception else DeviceConnectionError(f"Failed to read {descriptor}")

    def _revision_string(self, value: bytearray, prefix: str) -> Optional[str]:
        stripped_value = value.decode('utf-8').removeprefix(prefix)
        if len(stripped_value) > 0:
            return stripped_value.strip()
        else:
            return None

    async def turn_on(self, brightness_level: Optional[int] = None) -> None:
        _LOGGER.debug("Turning on...")
        brightness_level = brightness_level if brightness_level is not None else self._state.brightness_level
        await self._write_state(replace(self._state, is_on=True, brightness_level=brightness_level))

    async def turn_off(self) -> None:
        _LOGGER.debug("Turning off...")
        await self._write_state(replace(self._state, is_on=False))

    async def set_brightness_level(self, brightness_level: int):
        _LOGGER.debug("Setting brightness level to %d...", brightness_level)
        await self._write_state(replace(self._state, brightness_level=brightness_level))

    def _disconnect_cleanup(self):
        # Don't destroy _device and _key as they're needed for reconnection
        self._client = None
        self._summary = None
        self._state = DecoraBLEDeviceState()

    async def _unlock(self):
        """Send unlock command to device with API key."""
        packet = bytearray([0x11, 0x53, *self._key])
        _LOGGER.debug("Sending unlock command to %s", self._device.address)
        try:
            await self._client.write_gatt_char(EVENT_CHARACTERISTIC_UUID, packet, response=True)
            _LOGGER.debug("Unlock command sent successfully to %s", self._device.address)
        except Exception as ex:
            _LOGGER.error("Failed to send unlock command to %s: %s", self._device.address, ex)
            raise

    def _apply_device_state_data(self, data: bytearray) -> None:
        self._state = replace(
            self._state, is_on=data[0] == 1, brightness_level=data[1])
        _LOGGER.debug("State updated: %s", self._state)

    async def _write_state(self, state: DecoraBLEDeviceState) -> None:
        self._state = state
        packet = bytearray([1 if state.is_on else 0, state.brightness_level])
        _LOGGER.debug("Writing state: %s", state)
        await self._client.write_gatt_char(STATE_CHARACTERISTIC_UUID, packet, response=True)

    async def _read_state(self) -> None:
        data = await self._client.read_gatt_char(STATE_CHARACTERISTIC_UUID)
        self._apply_device_state_data(data)
        self._fire_state_callbacks(self._state)

    async def _register_for_state_notifications(self) -> None:
        def callback(sender: BleakGATTCharacteristic, data: bytearray) -> None:
            self._apply_device_state_data(data)
            self._fire_state_callbacks(self._state)

        # Retry logic for notification registration
        # Sometimes the device needs a moment after unlock before accepting notifications
        max_retries = 3
        retry_delay = 0.4  # seconds
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                await self._client.start_notify(STATE_CHARACTERISTIC_UUID, callback)
                _LOGGER.debug("Successfully registered for state notifications on attempt %d for %s", 
                             attempt + 1, self._device.address)
                return
            except BLEAK_EXCEPTIONS as ex:
                last_exception = ex
                error_msg = str(ex).lower()
                
                # Check if it's an authorization error
                if "authorization" in error_msg or "authentication" in error_msg:
                    _LOGGER.warning(
                        "%s: Authorization error registering notifications (attempt %d/%d): %s",
                        self._device.address, attempt + 1, max_retries, ex
                    )
                else:
                    _LOGGER.warning(
                        "%s: Failed to register for state notifications (attempt %d/%d): %s",
                        self._device.address, attempt + 1, max_retries, ex
                    )
                    
                if attempt < max_retries - 1:
                    # Wait before retrying
                    await asyncio.sleep(retry_delay)
                    # Check if client is still connected
                    if not self._client or not self._client.is_connected:
                        raise DeviceConnectionError("Client disconnected during notification registration") from ex
        
        # All retries exhausted
        raise last_exception if last_exception else DeviceConnectionError("Failed to register for notifications")

    def _handle_device_connection(self, exception: Exception | None) -> None:
        """Set the device connection future to resolve it."""
        if not self._device_connection_future:
            return
        if self._device_connection_future.done():
            return
        if exception:
            self._device_connection_future.set_exception(exception)
        else:
            self._device_connection_future.set_result(None)

    def _fire_connection_callbacks(self, summary: DecoraBLEDeviceSummary) -> None:
        for callback in self._connection_callbacks:
            callback(summary)

    def _fire_state_callbacks(self, state: DecoraBLEDeviceState) -> None:
        for callback in self._state_callbacks:
            callback(state)
