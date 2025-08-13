#!/usr/bin/env python3
"""
Tkinter GUI for Synapse CLI - Cross-platform desktop interface.
Provides a user-friendly GUI for GET and STORE operations.
"""

import os
import queue
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk

# Import the existing CLI functionality
try:
    import synapseclient
    from synapseclient.api.configuration_services import get_config_file
    from synapseclient.core import utils
    from synapseclient.models import File
except ImportError as e:
    print(f"Error: synapseclient is required but not installed: {e}")
    print("Install with: pip install synapseclient")
    sys.exit(1)


def get_available_profiles(config_path=None):
    """Get list of available authentication profiles from config file"""
    if config_path is None:
        config_path = os.path.expanduser("~/.synapseConfig")

    profiles = []

    try:
        config = get_config_file(config_path)
        sections = config.sections()

        # Look for profiles
        for section in sections:
            if section == "default":
                profiles.append("default")
            elif section.startswith("profile "):
                profile_name = section[8:]  # Remove "profile " prefix
                profiles.append(profile_name)
            elif section == "authentication":
                # Legacy authentication section
                profiles.append("authentication (legacy)")

        # If no profiles found but config exists, add default
        if not profiles and os.path.exists(config_path):
            profiles.append("default")

    except Exception:
        # If config file doesn't exist or can't be read, return empty list
        pass

    return profiles


def get_profile_info(profile_name, config_path=None):
    """Get username for a specific profile"""
    if config_path is None:
        config_path = os.path.expanduser("~/.synapseConfig")

    try:
        config = get_config_file(config_path)

        # Handle different profile name formats
        if profile_name == "default":
            section_name = "default"
        elif profile_name == "authentication (legacy)":
            section_name = "authentication"
        else:
            section_name = f"profile {profile_name}"

        if config.has_section(section_name):
            username = config.get(section_name, "username", fallback="")
            return username

    except Exception:
        pass

    return ""


class TQDMProgressCapture:
    """Capture TQDM progress updates for GUI display"""

    def __init__(self, operation_queue):
        self.operation_queue = operation_queue
        self.last_progress = 0

    def write(self, s):
        """Capture TQDM output and extract progress information"""
        if s and "\r" in s:
            # TQDM typically uses \r for progress updates
            progress_line = s.strip().replace("\r", "")
            if "%" in progress_line and (
                "B/s" in progress_line or "it/s" in progress_line
            ):
                # Parse progress percentage
                try:
                    # Look for percentage in the format "XX%"
                    import re

                    match = re.search(r"(\d+)%", progress_line)
                    if match:
                        progress = int(match.group(1))
                        if progress != self.last_progress:
                            self.last_progress = progress
                            self.operation_queue.put(
                                ("progress", f"Progress: {progress}%", progress)
                            )
                            # Also send the full progress line for detailed info
                            self.operation_queue.put(("progress_detail", progress_line))
                except Exception:
                    pass

    def flush(self):
        """Required for file-like object interface"""
        pass


class ToolTip:
    """Create a tooltip for a given widget"""

    def __init__(self, widget, text="widget info"):
        self.widget = widget
        self.text = text
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)
        self.tipwindow = None

    def enter(self, event=None):
        self.show_tooltip()

    def leave(self, event=None):
        self.hide_tooltip()

    def show_tooltip(self):
        if self.tipwindow or not self.text:
            return
        x, y, cx, cy = self.widget.bbox("insert")
        x = x + self.widget.winfo_rootx() + 25
        y = y + cy + self.widget.winfo_rooty() + 25
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry("+%d+%d" % (x, y))
        label = tk.Label(
            tw,
            text=self.text,
            justify=tk.LEFT,
            background="#ffffe0",
            relief=tk.SOLID,
            borderwidth=1,
            font=("tahoma", "8", "normal"),
        )
        label.pack(ipadx=1)

    def hide_tooltip(self):
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()


class SynapseGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Synapse Desktop Client")
        self.root.geometry("800x700")
        self.root.resizable(True, True)

        # Configure style
        style = ttk.Style()
        style.theme_use("clam")  # Cross-platform theme

        # Initialize variables
        self.syn = None
        self.is_logged_in = False
        self.logged_in_username = ""
        self.operation_queue = queue.Queue()
        self.config_file_available = False

        # Create the GUI
        self.create_widgets()

        # Start checking for operation results
        self.check_queue()

    def create_widgets(self):
        """Create all GUI widgets"""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.rowconfigure(3, weight=1)

        # Title
        title_label = ttk.Label(
            main_frame, text="Synapse Desktop Client", font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, pady=(0, 20))

        # Login Section
        self.create_login_section(main_frame)

        # Operation Tabs
        self.create_operation_tabs(main_frame)

        # Output Section
        self.create_output_section(main_frame)

        # Status Bar
        self.create_status_bar(main_frame)

    def create_login_section(self, parent):
        """Create login section with multi-profile support"""
        login_frame = ttk.LabelFrame(parent, text="Login", padding="10")
        login_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        login_frame.columnconfigure(1, weight=1)

        # Check if config file is available to determine default mode
        available_profiles = get_available_profiles()
        self.config_file_available = len(available_profiles) > 0
        default_mode = "config" if self.config_file_available else "manual"

        # Login mode selection
        mode_frame = ttk.Frame(login_frame)
        mode_frame.grid(
            row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )

        self.login_mode_var = tk.StringVar(value=default_mode)
        ttk.Radiobutton(
            mode_frame,
            text="Manual Login (Username + Token)",
            variable=self.login_mode_var,
            value="manual",
            command=self.on_login_mode_change,
        ).grid(row=0, column=0, sticky=tk.W, padx=(0, 20))

        config_radio = ttk.Radiobutton(
            mode_frame,
            text="Config File Login",
            variable=self.login_mode_var,
            value="config",
            command=self.on_login_mode_change,
        )
        config_radio.grid(row=0, column=1, sticky=tk.W)

        # Add tooltip if no config file available
        if not self.config_file_available:
            ToolTip(config_radio, "No Synapse config file found at ~/.synapseConfig")

        # Profile selection (for config mode)
        self.profile_frame = ttk.Frame(login_frame)
        self.profile_frame.grid(
            row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        self.profile_frame.columnconfigure(1, weight=1)

        ttk.Label(self.profile_frame, text="Profile:").grid(
            row=0, column=0, sticky=tk.W, padx=(0, 5)
        )
        self.profile_var = tk.StringVar()
        self.profile_combo = ttk.Combobox(
            self.profile_frame,
            textvariable=self.profile_var,
            state="readonly",
            width=25,
        )
        self.profile_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        self.profile_combo.bind("<<ComboboxSelected>>", self.on_profile_selected)

        # Profile info label
        self.profile_info_var = tk.StringVar()
        self.profile_info_label = ttk.Label(
            self.profile_frame,
            textvariable=self.profile_info_var,
            foreground="blue",
            font=("Arial", 8),
        )
        self.profile_info_label.grid(
            row=1, column=0, columnspan=3, sticky=tk.W, pady=(5, 0)
        )

        # Manual login fields
        self.manual_frame = ttk.Frame(login_frame)
        self.manual_frame.grid(
            row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        self.manual_frame.columnconfigure(1, weight=1)

        # Username
        ttk.Label(self.manual_frame, text="Username/Email:").grid(
            row=0, column=0, sticky=tk.W, padx=(0, 5)
        )
        self.username_var = tk.StringVar()
        self.username_entry = ttk.Entry(
            self.manual_frame, textvariable=self.username_var, width=30
        )
        self.username_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))

        # Auth Token
        ttk.Label(self.manual_frame, text="Personal Access Token:").grid(
            row=1, column=0, sticky=tk.W, padx=(0, 5), pady=(5, 0)
        )
        self.token_var = tk.StringVar()
        self.token_entry = ttk.Entry(
            self.manual_frame, textvariable=self.token_var, show="*", width=30
        )
        self.token_entry.grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 10), pady=(5, 0)
        )

        # Login button
        self.login_button = ttk.Button(
            login_frame, text="Login", command=self.login_logout
        )
        self.login_button.grid(row=3, column=0, columnspan=3, pady=(10, 0))

        # Status indicator
        self.login_status_var = tk.StringVar(value="Not logged in")
        self.login_status_label = ttk.Label(
            login_frame, textvariable=self.login_status_var, foreground="red"
        )
        self.login_status_label.grid(row=4, column=0, columnspan=3, pady=(5, 0))

        # Logged in user info
        self.user_info_var = tk.StringVar()
        self.user_info_label = ttk.Label(
            login_frame,
            textvariable=self.user_info_var,
            foreground="green",
            font=("Arial", 9, "bold"),
        )
        self.user_info_label.grid(row=5, column=0, columnspan=3, pady=(5, 0))

        # Initialize the interface
        self.refresh_profiles()
        self.on_login_mode_change()

    def create_operation_tabs(self, parent):
        """Create tabbed interface for operations"""
        self.notebook = ttk.Notebook(parent)
        self.notebook.grid(
            row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10)
        )

        # Download tab
        self.download_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.download_frame, text="Download File")
        self.create_download_tab()

        # Upload tab
        self.upload_frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(self.upload_frame, text="Upload File")
        self.create_upload_tab()

    def create_download_tab(self):
        """Create download tab widgets"""
        # Synapse ID
        ttk.Label(self.download_frame, text="Synapse ID:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5)
        )
        self.download_id_var = tk.StringVar()
        download_id_entry = ttk.Entry(
            self.download_frame, textvariable=self.download_id_var, width=40
        )
        download_id_entry.grid(
            row=0, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )

        # Version (optional)
        ttk.Label(self.download_frame, text="Version (optional):").grid(
            row=1, column=0, sticky=tk.W, pady=(0, 5)
        )
        self.download_version_var = tk.StringVar()
        download_version_entry = ttk.Entry(
            self.download_frame, textvariable=self.download_version_var, width=40
        )
        download_version_entry.grid(
            row=1, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )

        # Download location
        ttk.Label(self.download_frame, text="Download Location:").grid(
            row=2, column=0, sticky=tk.W, pady=(0, 5)
        )

        location_frame = ttk.Frame(self.download_frame)
        location_frame.grid(
            row=2, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0)
        )
        location_frame.columnconfigure(0, weight=1)

        self.download_location_var = tk.StringVar(value=str(Path.home() / "Downloads"))
        download_location_entry = ttk.Entry(
            location_frame, textvariable=self.download_location_var
        )
        download_location_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

        browse_button = ttk.Button(
            location_frame, text="Browse", command=self.browse_download_location
        )
        browse_button.grid(row=0, column=1)

        # Download button
        self.download_button = ttk.Button(
            self.download_frame,
            text="Download File",
            command=self.download_file,
            state="disabled",
        )
        self.download_button.grid(row=3, column=0, columnspan=2, pady=(20, 0))

        # Progress bar for downloads
        self.download_progress_var = tk.StringVar(value="")
        self.download_progress_label = ttk.Label(
            self.download_frame,
            textvariable=self.download_progress_var,
            foreground="blue",
            font=("Arial", 8),
        )
        self.download_progress_label.grid(row=4, column=0, columnspan=2, pady=(5, 0))

        self.download_progress_bar = ttk.Progressbar(
            self.download_frame, mode="determinate"
        )
        self.download_progress_bar.grid(
            row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0)
        )

        # Configure grid weights
        self.download_frame.columnconfigure(1, weight=1)

    def create_upload_tab(self):
        """Create upload tab widgets"""
        # File selection
        ttk.Label(self.upload_frame, text="File to Upload:").grid(
            row=0, column=0, sticky=tk.W, pady=(0, 5)
        )

        file_frame = ttk.Frame(self.upload_frame)
        file_frame.grid(row=0, column=1, sticky=(tk.W, tk.E), pady=(0, 5), padx=(10, 0))
        file_frame.columnconfigure(0, weight=1)

        self.upload_file_var = tk.StringVar()
        upload_file_entry = ttk.Entry(file_frame, textvariable=self.upload_file_var)
        upload_file_entry.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=(0, 5))

        browse_file_button = ttk.Button(
            file_frame, text="Browse", command=self.browse_upload_file
        )
        browse_file_button.grid(row=0, column=1)

        # Upload mode selection
        mode_frame = ttk.LabelFrame(self.upload_frame, text="Upload Mode", padding="10")
        mode_frame.grid(
            row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0)
        )
        mode_frame.columnconfigure(1, weight=1)

        self.upload_mode_var = tk.StringVar(value="new")

        new_radio = ttk.Radiobutton(
            mode_frame,
            text="Create New File",
            variable=self.upload_mode_var,
            value="new",
            command=self.on_upload_mode_change,
        )
        new_radio.grid(row=0, column=0, sticky=tk.W, pady=(0, 5))

        update_radio = ttk.Radiobutton(
            mode_frame,
            text="Update Existing File",
            variable=self.upload_mode_var,
            value="update",
            command=self.on_upload_mode_change,
        )
        update_radio.grid(row=1, column=0, sticky=tk.W)

        # Parent ID / Entity ID fields
        self.parent_label = ttk.Label(mode_frame, text="Parent ID (Project/Folder):")
        self.parent_label.grid(row=0, column=1, sticky=tk.W, padx=(20, 5), pady=(0, 5))

        self.parent_id_var = tk.StringVar()
        self.parent_id_entry = ttk.Entry(
            mode_frame, textvariable=self.parent_id_var, width=30
        )
        self.parent_id_entry.grid(row=0, column=2, sticky=(tk.W, tk.E), pady=(0, 5))

        self.entity_label = ttk.Label(mode_frame, text="Entity ID to Update:")
        self.entity_label.grid(row=1, column=1, sticky=tk.W, padx=(20, 5))

        self.entity_id_var = tk.StringVar()
        self.entity_id_entry = ttk.Entry(
            mode_frame, textvariable=self.entity_id_var, width=30, state="disabled"
        )
        self.entity_id_entry.grid(row=1, column=2, sticky=(tk.W, tk.E))

        # File name
        ttk.Label(self.upload_frame, text="Entity Name (optional):").grid(
            row=2, column=0, sticky=tk.W, pady=(10, 5)
        )
        self.upload_name_var = tk.StringVar()
        upload_name_entry = ttk.Entry(
            self.upload_frame, textvariable=self.upload_name_var, width=40
        )
        upload_name_entry.grid(
            row=2, column=1, sticky=(tk.W, tk.E), pady=(10, 5), padx=(10, 0)
        )

        # Upload button
        self.upload_button = ttk.Button(
            self.upload_frame,
            text="Upload File",
            command=self.upload_file,
            state="disabled",
        )
        self.upload_button.grid(row=3, column=0, columnspan=2, pady=(20, 0))

        # Progress bar for uploads
        self.upload_progress_var = tk.StringVar(value="")
        self.upload_progress_label = ttk.Label(
            self.upload_frame,
            textvariable=self.upload_progress_var,
            foreground="blue",
            font=("Arial", 8),
        )
        self.upload_progress_label.grid(row=4, column=0, columnspan=2, pady=(5, 0))

        self.upload_progress_bar = ttk.Progressbar(
            self.upload_frame, mode="determinate"
        )
        self.upload_progress_bar.grid(
            row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(5, 0)
        )

        # Configure grid weights
        self.upload_frame.columnconfigure(1, weight=1)
        mode_frame.columnconfigure(2, weight=1)

    def create_output_section(self, parent):
        """Create output/log section"""
        output_frame = ttk.LabelFrame(parent, text="Output", padding="5")
        output_frame.grid(
            row=3, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10)
        )
        output_frame.columnconfigure(0, weight=1)
        output_frame.rowconfigure(0, weight=1)

        self.output_text = scrolledtext.ScrolledText(
            output_frame, height=20, wrap=tk.WORD, font=("Consolas", 9)
        )
        self.output_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Clear button
        clear_button = ttk.Button(
            output_frame, text="Clear Output", command=self.clear_output
        )
        clear_button.grid(row=1, column=0, pady=(5, 0))

    def create_status_bar(self, parent):
        """Create status bar"""
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(parent, textvariable=self.status_var, relief=tk.SUNKEN)
        status_bar.grid(row=4, column=0, sticky=(tk.W, tk.E))

    def log_output(self, message, error=False):
        """Add message to output text widget"""
        self.output_text.insert(tk.END, f"{message}\n")
        self.output_text.see(tk.END)
        if error:
            # Color the last line red for errors
            line_start = self.output_text.index("end-1c linestart")
            line_end = self.output_text.index("end-1c lineend")
            self.output_text.tag_add("error", line_start, line_end)
            self.output_text.tag_config("error", foreground="red")
        self.root.update_idletasks()

    def clear_output(self):
        """Clear the output text widget"""
        self.output_text.delete(1.0, tk.END)

    def browse_download_location(self):
        """Browse for download directory"""
        directory = filedialog.askdirectory(initialdir=self.download_location_var.get())
        if directory:
            self.download_location_var.set(directory)

    def browse_upload_file(self):
        """Browse for file to upload"""
        file_path = filedialog.askopenfilename(
            title="Select file to upload", initialdir=str(Path.home())
        )
        if file_path:
            self.upload_file_var.set(file_path)
            # Auto-fill name if empty
            if not self.upload_name_var.get():
                self.upload_name_var.set(Path(file_path).name)

    def on_upload_mode_change(self):
        """Handle upload mode radio button changes"""
        mode = self.upload_mode_var.get()
        if mode == "new":
            self.parent_id_entry.config(state="normal")
            self.entity_id_entry.config(state="disabled")
            self.entity_id_var.set("")
        else:  # update
            self.parent_id_entry.config(state="disabled")
            self.entity_id_entry.config(state="normal")
            self.parent_id_var.set("")

    def on_login_mode_change(self):
        """Handle login mode radio button changes"""
        mode = self.login_mode_var.get()
        if mode == "manual":
            # Show manual login fields, hide profile selection
            self.manual_frame.grid()
            self.profile_frame.grid_remove()
        else:  # config
            # Show profile selection, hide manual login fields
            self.manual_frame.grid_remove()
            self.profile_frame.grid()
            self.refresh_profiles()

    def refresh_profiles(self):
        """Refresh the list of available profiles"""
        try:
            profiles = get_available_profiles()
            if profiles:
                self.profile_combo["values"] = profiles
                if not self.profile_var.get() or self.profile_var.get() not in profiles:
                    self.profile_var.set(profiles[0])
                self.on_profile_selected()
            else:
                self.profile_combo["values"] = []
                self.profile_var.set("")
                self.profile_info_var.set("No profiles found in config file")
        except Exception as e:
            self.profile_combo["values"] = []
            self.profile_var.set("")
            self.profile_info_var.set(f"Error reading config: {str(e)}")

    def on_profile_selected(self, event=None):
        """Handle profile selection"""
        profile_name = self.profile_var.get()
        if profile_name:
            username = get_profile_info(profile_name)
            if username:
                self.profile_info_var.set(f"Username: {username}")
            else:
                self.profile_info_var.set("No username found for this profile")
        else:
            self.profile_info_var.set("")

    def login_logout(self):
        """Handle login/logout button click"""
        if self.is_logged_in:
            self.logout()
        else:
            self.login()

    def logout(self):
        """Logout from Synapse"""
        try:
            if self.syn:
                self.syn.logout()
            self.syn = None
            self.is_logged_in = False
            self.logged_in_username = ""

            # Update UI
            self.login_status_var.set("Logged out")
            self.login_status_label.config(foreground="red")
            self.login_button.config(text="Login")
            self.user_info_var.set("")
            self.download_button.config(state="disabled")
            self.upload_button.config(state="disabled")
            self.status_var.set("Ready")
            self.log_output("Logged out successfully")

        except Exception as e:
            self.log_output(f"Logout error: {e}", error=True)

    def login(self):
        """Login to Synapse with support for both manual and config file authentication"""

        def login_worker():
            try:
                self.log_output("Attempting to login...")
                self.syn = synapseclient.Synapse(skip_checks=True)

                mode = self.login_mode_var.get()

                if mode == "manual":
                    # Manual login with username and token
                    username = self.username_var.get().strip()
                    token = self.token_var.get().strip()

                    if not token:
                        raise ValueError(
                            "Personal Access Token is required for manual login"
                        )

                    # Use email parameter for username when provided to ensure compliance
                    if username:
                        self.syn.login(email=username, authToken=token, silent=True)
                    else:
                        self.syn.login(authToken=token, silent=True)

                else:  # config mode
                    # Config file login with profile
                    profile_name = self.profile_var.get()
                    if not profile_name:
                        raise ValueError("Please select a profile")

                    # Clean profile name for login
                    if profile_name == "authentication (legacy)":
                        # Use None to let Synapse handle legacy authentication section
                        self.syn.login(silent=True)
                    else:
                        # Use the specific profile
                        self.syn.login(profile=profile_name, silent=True)

                # Get the logged-in username from the Synapse client
                username = getattr(self.syn, "username", None) or getattr(
                    self.syn, "email", "Unknown User"
                )
                self.operation_queue.put(
                    (
                        "login_success",
                        f"Login successful! Logged in as: {username}",
                        username,
                    )
                )

            except Exception as e:
                self.operation_queue.put(("login_error", str(e)))

        # Disable login button during login attempt
        self.login_button.config(state="disabled")
        self.status_var.set("Logging in...")

        # Run login in separate thread
        threading.Thread(target=login_worker, daemon=True).start()

    def download_file(self):
        """Download file from Synapse"""

        def download_worker():
            try:
                synapse_id = self.download_id_var.get().strip()
                version = self.download_version_var.get().strip()
                download_path = self.download_location_var.get().strip()

                if not synapse_id:
                    raise ValueError("Synapse ID is required")

                version_num = None
                if version:
                    try:
                        version_num = int(version)
                    except ValueError:
                        raise ValueError("Version must be a number")

                self.operation_queue.put(("status", f"Downloading {synapse_id}..."))
                self.operation_queue.put(("progress_start", "download"))

                # Capture TQDM progress output
                progress_capture = TQDMProgressCapture(self.operation_queue)

                # Redirect stderr to capture TQDM output
                import sys

                original_stderr = sys.stderr
                sys.stderr = progress_capture

                try:
                    file_obj = File(
                        id=synapse_id,
                        version_number=version_num,
                        path=download_path,
                        download_file=True,
                    )

                    file_obj = file_obj.get(synapse_client=self.syn)

                    if file_obj.path and os.path.exists(file_obj.path):
                        self.operation_queue.put(
                            ("download_success", f"Downloaded: {file_obj.path}")
                        )
                    else:
                        self.operation_queue.put(
                            (
                                "download_error",
                                f"No files associated with entity {synapse_id}",
                            )
                        )
                finally:
                    # Restore original stderr
                    sys.stderr = original_stderr
                    self.operation_queue.put(("progress_end", "download"))

            except Exception as e:
                self.operation_queue.put(("download_error", str(e)))
                self.operation_queue.put(("progress_end", "download"))

        if not self.is_logged_in:
            messagebox.showerror("Error", "Please log in first")
            return

        # Reset progress indicators and set operation context
        self.download_progress_var.set("")
        self.download_progress_bar["value"] = 0
        self._current_operation = "download"

        # Run download in separate thread
        threading.Thread(target=download_worker, daemon=True).start()

    def upload_file(self):
        """Upload file to Synapse"""

        def upload_worker():
            try:
                file_path = self.upload_file_var.get().strip()
                name = self.upload_name_var.get().strip()
                mode = self.upload_mode_var.get()

                if not file_path:
                    raise ValueError("File path is required")

                if not os.path.exists(file_path):
                    raise ValueError(f"File does not exist: {file_path}")

                self.operation_queue.put(("status", f"Uploading {file_path}..."))
                self.operation_queue.put(("progress_start", "upload"))

                # Capture TQDM progress output
                progress_capture = TQDMProgressCapture(self.operation_queue)

                # Redirect stderr to capture TQDM output
                import sys

                original_stderr = sys.stderr
                sys.stderr = progress_capture

                try:
                    if mode == "new":
                        parent_id = self.parent_id_var.get().strip()
                        if not parent_id:
                            raise ValueError("Parent ID is required for new files")

                        file_obj = File(
                            path=file_path,
                            name=name or utils.guess_file_name(file_path),
                            parent_id=parent_id,
                        )
                    else:  # update
                        entity_id = self.entity_id_var.get().strip()
                        if not entity_id:
                            raise ValueError("Entity ID is required for updates")

                        file_obj = File(
                            id=entity_id, path=file_path, name=name, download_file=False
                        )
                        file_obj = file_obj.get(synapse_client=self.syn)
                        file_obj.path = file_path
                        if name:
                            file_obj.name = name

                    file_obj = file_obj.store(synapse_client=self.syn)
                    msg = f"Created/Updated entity: {file_obj.id} - {file_obj.name}"
                    self.operation_queue.put(("upload_success", msg))
                finally:
                    # Restore original stderr
                    sys.stderr = original_stderr
                    self.operation_queue.put(("progress_end", "upload"))

            except Exception as e:
                self.operation_queue.put(("upload_error", str(e)))
                self.operation_queue.put(("progress_end", "upload"))

        if not self.is_logged_in:
            messagebox.showerror("Error", "Please log in first")
            return

        # Reset progress indicators and set operation context
        self.upload_progress_var.set("")
        self.upload_progress_bar["value"] = 0
        self._current_operation = "upload"

        # Run upload in separate thread
        threading.Thread(target=upload_worker, daemon=True).start()

    def check_queue(self):
        """Check for operation results from background threads"""
        try:
            while True:
                result = self.operation_queue.get_nowait()

                # Handle different result formats
                if len(result) == 2:
                    operation_type, message = result
                    username = None
                    progress = None
                elif len(result) == 3:
                    operation_type, message, username_or_progress = result
                    if operation_type == "progress":
                        username = None
                        progress = username_or_progress
                    else:
                        username = username_or_progress
                        progress = None
                else:
                    continue

                if operation_type == "login_success":
                    self.is_logged_in = True
                    if username:
                        self.logged_in_username = username
                        self.user_info_var.set(f"Logged in as: {username}")
                    else:
                        self.user_info_var.set("Logged in successfully")
                    self.login_status_var.set("Logged in successfully")
                    self.login_status_label.config(foreground="green")
                    self.login_button.config(text="Logout", state="normal")
                    self.download_button.config(state="normal")
                    self.upload_button.config(state="normal")
                    self.status_var.set("Ready")
                    self.log_output(message)

                elif operation_type == "login_error":
                    self.is_logged_in = False
                    self.logged_in_username = ""
                    self.user_info_var.set("")
                    self.login_status_var.set(f"Login failed: {message}")
                    self.login_status_label.config(foreground="red")
                    self.login_button.config(text="Login", state="normal")
                    self.status_var.set("Ready")
                    self.log_output(f"Login failed: {message}", error=True)

                elif operation_type == "progress_start":
                    if message == "download":
                        self.download_progress_var.set("Preparing download...")
                        self.download_progress_bar["value"] = 0
                    elif message == "upload":
                        self.upload_progress_var.set("Preparing upload...")
                        self.upload_progress_bar["value"] = 0

                elif operation_type == "progress":
                    # Update progress bars based on current operation
                    if progress is not None:
                        # Determine which progress bar to update based on which operation is active
                        if hasattr(self, "_current_operation"):
                            if self._current_operation == "download":
                                self.download_progress_bar["value"] = progress
                                self.download_progress_var.set(message)
                            elif self._current_operation == "upload":
                                self.upload_progress_bar["value"] = progress
                                self.upload_progress_var.set(message)
                        else:
                            # Fallback: update both (shouldn't happen in normal operation)
                            self.download_progress_bar["value"] = progress
                            self.upload_progress_bar["value"] = progress

                elif operation_type == "progress_detail":
                    # Log detailed progress information
                    self.log_output(message)

                elif operation_type == "progress_end":
                    if message == "download":
                        self.download_progress_var.set("")
                        self.download_progress_bar["value"] = 0
                        if (
                            hasattr(self, "_current_operation")
                            and self._current_operation == "download"
                        ):
                            delattr(self, "_current_operation")
                    elif message == "upload":
                        self.upload_progress_var.set("")
                        self.upload_progress_bar["value"] = 0
                        if (
                            hasattr(self, "_current_operation")
                            and self._current_operation == "upload"
                        ):
                            delattr(self, "_current_operation")

                elif operation_type == "download_success":
                    self.status_var.set("Download completed")
                    self.log_output(message)
                    self.download_progress_var.set("Download completed")
                    self.download_progress_bar["value"] = 100
                    messagebox.showinfo("Success", message)

                elif operation_type == "download_error":
                    self.status_var.set("Download failed")
                    self.log_output(f"Download failed: {message}", error=True)
                    self.download_progress_var.set("Download failed")
                    self.download_progress_bar["value"] = 0
                    messagebox.showerror("Download Error", message)

                elif operation_type == "upload_success":
                    self.status_var.set("Upload completed")
                    self.log_output(message)
                    self.upload_progress_var.set("Upload completed")
                    self.upload_progress_bar["value"] = 100
                    messagebox.showinfo("Success", message)

                elif operation_type == "upload_error":
                    self.status_var.set("Upload failed")
                    self.log_output(f"Upload failed: {message}", error=True)
                    self.upload_progress_var.set("Upload failed")
                    self.upload_progress_bar["value"] = 0
                    messagebox.showerror("Upload Error", message)

                elif operation_type == "status":
                    self.status_var.set(message)
                    self.log_output(message)

        except queue.Empty:
            pass

        # Schedule next check
        self.root.after(100, self.check_queue)


def main():
    """Main function to run the GUI"""
    root = tk.Tk()
    SynapseGUI(root)

    # Center the window
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f"{width}x{height}+{x}+{y}")

    try:
        root.mainloop()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
