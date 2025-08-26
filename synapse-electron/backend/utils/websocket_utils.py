"""
WebSocket utilities for the Synapse Desktop Client backend.

This module handles WebSocket connections and message broadcasting
to the Electron frontend.
"""

import asyncio
import json
import logging
import threading
from typing import Any, Dict, Optional, Set

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

# Global set to track connected WebSocket clients
connected_clients: Set[Any] = set()


async def handle_websocket_client(websocket: Any, path: Optional[str] = None) -> None:
    """
    Handle individual WebSocket connections from the Electron frontend.

    Manages the lifecycle of a WebSocket connection including registration,
    message handling, and cleanup when the connection ends.

    Arguments:
        websocket: The WebSocket connection object
        path: The WebSocket connection path (optional)

    Returns:
        None

    Raises:
        ConnectionClosed: When the WebSocket connection is closed (handled gracefully)
        Exception: Other WebSocket errors are caught and logged
    """
    connected_clients.add(websocket)
    logger.info(
        f"WebSocket client connected from path: {path}. "
        f"Total clients: {len(connected_clients)}"
    )

    try:
        # Send connection confirmation
        await websocket.send(
            json.dumps({"type": "connection_status", "connected": True})
        )

        # Listen for incoming messages
        async for message in websocket:
            logger.info(f"Received WebSocket message: {message}")

    except ConnectionClosed:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        connected_clients.discard(websocket)
        logger.info(
            f"WebSocket client removed. Total clients: {len(connected_clients)}"
        )


async def broadcast_message(message: Dict[str, Any]) -> None:
    """
    Broadcast a message to all connected WebSocket clients.

    Sends a message to all currently connected WebSocket clients,
    handling disconnections gracefully and cleaning up dead connections.

    Arguments:
        message: The message dictionary to broadcast

    Returns:
        None

    Raises:
        Exception: Broadcast errors are handled gracefully per client.
    """
    if not connected_clients:
        return _log_no_clients_message(message)

    _add_message_metadata(message)

    # Create a copy to avoid "set changed size during iteration" error
    clients_copy = connected_clients.copy()
    disconnected = set()
    message_json = json.dumps(message)

    for client in clients_copy:
        try:
            await client.send(message_json)
        except ConnectionClosed:
            disconnected.add(client)
        except Exception as e:
            logger.warning(f"Failed to send message to client: {e}")
            disconnected.add(client)

    # Remove disconnected clients
    for client in disconnected:
        connected_clients.discard(client)


def _log_no_clients_message(message: Dict[str, Any]) -> None:
    """
    Log when no clients are connected, avoiding spam for frequent messages.

    Provides selective logging to avoid spamming logs when no WebSocket
    clients are connected, filtering out frequent message types.

    Arguments:
        message: The message that couldn't be sent

    Returns:
        None

    Raises:
        None: This function does not raise exceptions.
    """
    message_type = message.get("type", "unknown")
    if message_type not in ["log", "progress"]:
        logger.debug(f"No WebSocket clients connected to send message: {message_type}")


def _add_message_metadata(message: Dict[str, Any]) -> None:
    """
    Add metadata to outgoing messages.

    Enhances outgoing messages with additional metadata such as
    timestamps and UI hints for better frontend handling.

    Arguments:
        message: The message to add metadata to

    Returns:
        None

    Raises:
        None: This function does not raise exceptions.
    """
    # Add auto-scroll flag for log messages
    if message.get("type") == "log":
        message["auto_scroll"] = True

    # Add timestamp if not present
    if "timestamp" not in message:
        import time

        message["timestamp"] = time.time()


def start_websocket_server(port: int) -> None:
    """
    Start the WebSocket server in a separate thread.

    Initializes and starts the WebSocket server in its own thread with
    a dedicated event loop to avoid blocking the main application.

    Arguments:
        port: The port number to bind the WebSocket server to

    Returns:
        None

    Raises:
        Exception: Server startup errors are caught and logged.
    """

    def run_websocket_server() -> None:
        """
        Run the WebSocket server in its own event loop.

        Creates a new event loop and runs the WebSocket server within it,
        handling all WebSocket connections independently of the main application.

        Arguments:
            None

        Returns:
            None

        Raises:
            Exception: Server runtime errors are caught and logged.
        """

        async def websocket_server() -> None:
            """
            Create and run the WebSocket server.

            Sets up the WebSocket server with proper handlers and runs
            it indefinitely until the application shuts down.

            Arguments:
                None

            Returns:
                None

            Raises:
                Exception: Server creation and runtime errors are caught and logged.
            """

            async def websocket_handler(
                websocket: Any, path: Optional[str] = None
            ) -> None:
                await handle_websocket_client(websocket, path)

            try:
                async with websockets.serve(websocket_handler, "localhost", port):
                    logger.info("WebSocket server started on ws://localhost:%s", port)
                    await asyncio.Future()  # Run forever
            except Exception:
                logger.exception("WebSocket server error")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(websocket_server())

    ws_thread = threading.Thread(target=run_websocket_server)
    ws_thread.daemon = True
    ws_thread.start()
