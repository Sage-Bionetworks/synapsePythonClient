"""
WebSocket utilities for the Synapse Desktop Client backend.

This module handles WebSocket connections and message broadcasting
to the Electron frontend.
"""

import asyncio
import json
import logging
import threading
from typing import Any, Dict, Set

import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

# Global set to track connected WebSocket clients
connected_clients: Set[Any] = set()


async def handle_websocket_client(websocket: Any, path: str = None) -> None:
    """
    Handle individual WebSocket connections from the Electron frontend.

    Args:
        websocket: The WebSocket connection object
        path: The WebSocket connection path (optional)
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

    Args:
        message: The message dictionary to broadcast
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

    Args:
        message: The message that couldn't be sent
    """
    message_type = message.get("type", "unknown")
    if message_type not in ["log", "progress"]:
        logger.debug(f"No WebSocket clients connected to send message: {message_type}")


def _add_message_metadata(message: Dict[str, Any]) -> None:
    """
    Add metadata to outgoing messages.

    Args:
        message: The message to add metadata to
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

    Args:
        port: The port number to bind the WebSocket server to
    """

    def run_websocket_server() -> None:
        """Run the WebSocket server in its own event loop."""

        async def websocket_server() -> None:
            """Create and run the WebSocket server."""

            async def websocket_handler(websocket: Any, path: str = None) -> None:
                await handle_websocket_client(websocket, path)

            try:
                async with websockets.serve(websocket_handler, "localhost", port):
                    logger.info("WebSocket server started on ws://localhost:%s", port)
                    await asyncio.Future()  # Run forever
            except Exception as e:
                logger.error(f"WebSocket server error: {e}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(websocket_server())

    ws_thread = threading.Thread(target=run_websocket_server)
    ws_thread.daemon = True
    ws_thread.start()
