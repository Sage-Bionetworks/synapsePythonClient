#!/usr/bin/env python3
"""
Minimal Synapse CLI with GET and STORE commands - Interactive Version.
Includes interactive session support for missing parameters.
Fixed version that ensures all dependencies are loaded.
"""

import argparse
import getpass
import os
import sys

# Now import synapseclient
try:
    import synapseclient
    from synapseclient.core import utils
    from synapseclient.core.exceptions import (
        SynapseAuthenticationError,
        SynapseNoCredentialsError,
    )
    from synapseclient.models import File
except ImportError as e:
    print(f"Error: synapseclient is required but not installed: {e}")
    print("Install with: pip install synapseclient")
    input("Press the enter key to exit...")
    sys.exit(1)


def prompt_for_missing(
    prompt_text, current_value=None, is_password=False, required=True
):
    """Prompt user for missing values interactively"""
    if current_value is not None:
        return current_value

    if not required:
        value = input(f"{prompt_text} (Optional): ").strip()
        return value if value else None

    while True:
        if is_password:
            value = getpass.getpass(f"{prompt_text}: ").strip()
        else:
            value = input(f"{prompt_text}: ").strip()

        if value:
            return value
        print("This field is required. Please enter a value.")


def get_command(args, syn, interactive=False):
    """Download a file from Synapse"""
    # Strip quotes from download location if provided
    if args.downloadLocation:
        args.downloadLocation = args.downloadLocation.strip("\"'")

    # Interactive prompting for missing values
    if interactive:
        args.id = prompt_for_missing(
            "Enter the Synapse ID of the file you want to download (e.g., syn123456)",
            args.id,
        )

        if args.version is None:
            version_input = prompt_for_missing(
                "Enter version number to download (press Enter for latest version)",
                None,
                required=False,
            )
            if version_input:
                try:
                    args.version = int(version_input)
                except ValueError:
                    print("Warning: Invalid version number, using latest version")
                    args.version = None

        if not args.downloadLocation or args.downloadLocation == "./":
            download_loc = prompt_for_missing(
                "Enter download location (press Enter for current directory)",
                args.downloadLocation,
                required=False,
            )
            if download_loc:
                # Strip quotes from download location
                download_loc = download_loc.strip("\"'")
                args.downloadLocation = download_loc

    if args.id is None:
        raise ValueError("Missing required id argument for get command")

    try:
        file_obj = File(
            id=args.id,
            version_number=args.version,
            path=args.downloadLocation,
            download_file=True,
        )

        file_obj = file_obj.get(synapse_client=syn)

        if file_obj.path and os.path.exists(file_obj.path):
            print(f"Downloaded: {file_obj.path}")
            input("Press the enter key to exit...")
            sys.exit(1)
        else:
            print(f"WARNING: No files associated with entity {file_obj.id}")
            input("Press the enter key to exit...")
            sys.exit(1)
    except (SynapseAuthenticationError, ValueError, KeyError) as e:
        print(f"Error downloading {args.id}: {e}")
        input("Press the enter key to exit...")
        sys.exit(1)


def store_command(args, syn, interactive=False):
    """Upload and store a file to Synapse"""
    # Strip quotes from file path if provided
    if args.file:
        args.file = args.file.strip("\"'")

    # Interactive prompting for missing values
    if interactive:
        args.file = prompt_for_missing(
            "Enter the full path to the file you want to upload", args.file
        )
        # Strip quotes from the file path
        if args.file:
            args.file = args.file.strip("\"'")

        # Check if file exists
        while not os.path.exists(args.file):
            print(f"ERROR: File not found at path: {args.file}")
            args.file = prompt_for_missing("Please enter a valid file path", None)
            # Strip quotes from the file path
            if args.file:
                args.file = args.file.strip("\"'")

        # Parent ID or existing entity ID
        if args.parentid is None and args.id is None:
            print("\n" + "=" * 60)
            print("UPLOAD MODE SELECTION")
            print("=" * 60)
            print("Choose how you want to upload this file:")
            print()
            print("  [NEW]    Create a new file entity in Synapse")
            print("           - Requires a parent location (project or folder ID)")
            print("           - Will create a brand new entity")
            print()
            print("  [UPDATE] Update an existing file entity in Synapse")
            print("           - Requires the existing entity's Synapse ID")
            print("           - Will replace the current file with your new file")
            print()

            while True:
                choice = input("Enter your choice [NEW/UPDATE]: ").strip().upper()
                if choice in ["NEW", "N"]:
                    args.parentid = prompt_for_missing(
                        "Enter the Synapse ID of the parent (project or folder) "
                        "where you want to create the new file (e.g., syn123456)",
                        args.parentid,
                    )
                    break
                elif choice in ["UPDATE", "U"]:
                    args.id = prompt_for_missing(
                        "Enter the Synapse ID of the existing file entity "
                        "you want to update (e.g., syn789012)",
                        args.id,
                    )
                    break
                else:
                    print("Please enter either 'NEW' or 'UPDATE' (or 'N' or 'U')")

        # Entity name
        if args.name is None:
            default_name = utils.guess_file_name(args.file)
            name_input = prompt_for_missing(
                f"Enter a name for this file in Synapse (press Enter to use: {default_name})",
                None,
                required=False,
            )
            if name_input:
                args.name = name_input
            else:
                args.name = default_name

    # Validate arguments
    if args.parentid is None and args.id is None:
        raise ValueError("synapse store requires either parentId or id to be specified")

    if args.file is None:
        raise ValueError("store command requires a file to upload")

    if not os.path.exists(args.file):
        raise ValueError(f"File does not exist: {args.file}")

    try:
        if args.id is not None:
            file_obj = File(
                id=args.id, path=args.file, name=args.name, download_file=False
            )
            file_obj = file_obj.get(synapse_client=syn)
            file_obj.path = args.file
            if args.name:
                file_obj.name = args.name
        else:
            file_obj = File(
                path=args.file,
                name=args.name or utils.guess_file_name(args.file),
                parent_id=args.parentid,
            )

        file_obj = file_obj.store(synapse_client=syn)
        print(f"Created/Updated entity: {file_obj.id} - {file_obj.name}")
        input("Press the enter key to exit...")
        sys.exit(1)

    except (SynapseAuthenticationError, ValueError, KeyError, OSError) as e:
        print(f"Error storing file {args.file}: {e}")
        input("Press the enter key to exit...")
        sys.exit(1)


def login_with_prompt(syn, user=None, auth_token=None, silent=False, interactive=False):
    """Login to Synapse with credentials"""
    try:
        # Interactive prompting for missing credentials
        if interactive:
            if auth_token is None:
                auth_token = prompt_for_missing(
                    "Enter your Personal Access Token (or leave blank and press Enter to use config file)",
                    auth_token,
                    is_password=True,
                    required=False,
                )

            if user is None and auth_token is None:
                user = prompt_for_missing(
                    "Enter your Synapse username or email (or leave blank and press Enter to use config file)",
                    user,
                    required=False,
                )  # Try to login with provided credentials
        if auth_token:
            syn.login(authToken=auth_token, silent=silent)
        elif user:
            # Prompt for auth token
            if not silent and not interactive:
                auth_token = getpass.getpass(f"Auth token for user {user}: ")
            elif interactive and auth_token is None:
                auth_token = prompt_for_missing(
                    f"Auth token for user {user}", None, is_password=True
                )
            syn.login(email=user, authToken=auth_token, silent=silent)
        else:
            # Try to login with cached credentials
            syn.login(silent=silent)

    except SynapseNoCredentialsError:
        if silent:
            raise
        print("No saved credentials found in your Synapse configuration.")
        if not interactive:
            user = input("Enter your Synapse username or email (optional): ") or None
            auth_token = getpass.getpass("Enter your Personal Access Token: ")
        else:
            user = prompt_for_missing("Synapse username or email", None, required=False)
            auth_token = prompt_for_missing(
                "Personal Access Token", None, is_password=True
            )
        syn.login(email=user, authToken=auth_token, silent=silent)
    except SynapseAuthenticationError as e:
        print(f"Authentication failed: {e}")
        input("Press the enter key to exit...")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Minimal Synapse CLI with GET and STORE commands"
    )

    # Global arguments
    parser.add_argument(
        "-u",
        "--username",
        dest="synapseUser",
        help="Username used to connect to Synapse",
    )
    parser.add_argument(
        "-p",
        "--password",
        dest="synapse_auth_token",
        help="Personal Access Token used to connect to Synapse",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--silent", action="store_true", help="Suppress console output")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # GET command
    parser_get = subparsers.add_parser("get", help="Download a file from Synapse")
    parser_get.add_argument("id", nargs="?", help="Synapse ID (e.g., syn123)")
    parser_get.add_argument(
        "-v",
        "--version",
        type=int,
        default=None,
        help="Synapse version number to retrieve",
    )
    parser_get.add_argument(
        "--downloadLocation",
        default="./",
        help="Directory to download file to [default: ./]",
    )
    parser_get.set_defaults(func=get_command)

    # STORE command
    parser_store = subparsers.add_parser("store", help="Upload a file to Synapse")
    parser_store.add_argument("file", nargs="?", help="File to upload")
    parser_store.add_argument(
        "--parentid",
        "--parentId",
        dest="parentid",
        help="Synapse ID of parent project or folder",
    )
    parser_store.add_argument("--id", help="Synapse ID of existing entity to update")
    parser_store.add_argument("--name", help="Name for the entity in Synapse")
    parser_store.set_defaults(func=store_command)

    args = parser.parse_args()

    # Enable interactive mode if no command is provided or if insufficient args are provided
    interactive_mode = False

    if args.command is None:
        # No command provided - enter interactive mode
        interactive_mode = True
        print("\n" + "=" * 50)
        print("Interactive Synapse CLI")
        print("=" * 50)
        print("Available commands:")
        print("  GET   - Download a file from Synapse")
        print("  STORE - Upload a file to Synapse")
        print()
        command = (
            input("Enter command [GET/STORE] (Case insensitive): ").strip().upper()
        )
        if command not in ["GET", "STORE"]:
            print("Invalid command. Please use 'GET' or 'STORE' (Case insensitive)")
            input("Press the enter key to exit...")
            sys.exit(1)
        args.command = command.lower()

        # Set up default args for interactive mode
        if command.lower() == "get":
            args.id = None
            args.version = None
            args.downloadLocation = "./"
            args.func = get_command
        elif command.lower() == "store":
            args.file = None
            args.parentid = None
            args.id = None
            args.name = None
            args.func = store_command
    else:
        # Command provided - check if we need interactive mode for missing required args
        if args.command == "get" and (not hasattr(args, "id") or args.id is None):
            interactive_mode = True
        elif args.command == "store" and (
            not hasattr(args, "file") or args.file is None
        ):
            interactive_mode = True

    # Initialize Synapse client
    syn = synapseclient.Synapse(debug=args.debug, silent=args.silent, skip_checks=True)

    # Login
    try:
        login_with_prompt(
            syn,
            args.synapseUser,
            args.synapse_auth_token,
            args.silent,
            interactive_mode,
        )
    except (SynapseAuthenticationError, SynapseNoCredentialsError) as e:
        print(f"Login failed: {e}")
        input("Press the enter key to exit...")
        sys.exit(1)

    # Execute command
    try:
        if interactive_mode:
            args.func(args, syn, interactive=True)
        else:
            args.func(args, syn)
    except (ValueError, KeyError, OSError) as e:
        print(f"Command failed: {e}")
        input("Press the enter key to exit...")
        sys.exit(1)


if __name__ == "__main__":
    main()
