"""
Login UI component - separated from main window
"""
import tkinter as tk
from tkinter import ttk
from typing import Callable

from ..models.config import ConfigManager
from ..utils.tooltips import ToolTip


class LoginComponent:
    """
    Login section UI component.
    
    Provides functionality for user authentication through manual login
    (username/token) or configuration file profiles.
    """

    def __init__(
        self,
        parent: tk.Widget,
        config_manager: ConfigManager,
        on_login_callback: Callable,
        on_logout_callback: Callable,
    ) -> None:
        """
        Initialize the login component.

        Args:
            parent: Parent widget to contain this component
            config_manager: Configuration manager for profile handling
            on_login_callback: Callback function for login events
            on_logout_callback: Callback function for logout events
        """
        self.parent = parent
        self.config_manager = config_manager
        self.on_login = on_login_callback
        self.on_logout = on_logout_callback

        self.login_mode_var = tk.StringVar()
        self.profile_var = tk.StringVar()
        self.profile_info_var = tk.StringVar()
        self.username_var = tk.StringVar()
        self.token_var = tk.StringVar()
        self.login_status_var = tk.StringVar(value="Not logged in")
        self.user_info_var = tk.StringVar()

        self.login_button = None
        self.login_status_label = None
        self.profile_combo = None
        self.manual_frame = None
        self.profile_frame = None

        self.create_ui()

    def create_ui(self) -> None:
        """Create and configure the login UI components."""
        login_frame = ttk.LabelFrame(self.parent, text="Login", padding="10")
        login_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        login_frame.columnconfigure(1, weight=1)

        available_profiles = self.config_manager.get_available_profiles()
        config_available = len(available_profiles) > 0
        default_mode = "config" if config_available else "manual"
        self.login_mode_var.set(default_mode)

        self._create_mode_selection(login_frame, config_available)
        self._create_profile_section(login_frame)
        self._create_manual_section(login_frame)
        self._create_login_button(login_frame)
        self._create_status_labels(login_frame)

        self.refresh_profiles()
        self.on_login_mode_change()

    def _create_mode_selection(self, parent: tk.Widget, config_available: bool) -> None:
        """
        Create login mode radio buttons.

        Args:
            parent: Parent widget to contain the mode selection
            config_available: Whether configuration file profiles are available
        """
        mode_frame = ttk.Frame(parent)
        mode_frame.grid(
            row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )

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

        if not config_available:
            ToolTip(config_radio, "No Synapse config file found at ~/.synapseConfig")

    def _create_profile_section(self, parent: tk.Widget) -> None:
        """
        Create profile selection UI elements.

        Args:
            parent: Parent widget to contain the profile section
        """
        self.profile_frame = ttk.Frame(parent)
        self.profile_frame.grid(
            row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        self.profile_frame.columnconfigure(1, weight=1)

        ttk.Label(self.profile_frame, text="Profile:").grid(
            row=0, column=0, sticky=tk.W, padx=(0, 5)
        )

        self.profile_combo = ttk.Combobox(
            self.profile_frame,
            textvariable=self.profile_var,
            state="readonly",
            width=25,
        )
        self.profile_combo.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        self.profile_combo.bind("<<ComboboxSelected>>", self.on_profile_selected)

        profile_info_label = ttk.Label(
            self.profile_frame,
            textvariable=self.profile_info_var,
            foreground="blue",
            font=("Arial", 8),
        )
        profile_info_label.grid(row=1, column=0, columnspan=3, sticky=tk.W, pady=(5, 0))

    def _create_manual_section(self, parent: tk.Widget) -> None:
        """
        Create manual login input fields.

        Args:
            parent: Parent widget to contain the manual section
        """
        self.manual_frame = ttk.Frame(parent)
        self.manual_frame.grid(
            row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0, 10)
        )
        self.manual_frame.columnconfigure(1, weight=1)

        ttk.Label(self.manual_frame, text="Username/Email:").grid(
            row=0, column=0, sticky=tk.W, padx=(0, 5)
        )
        username_entry = ttk.Entry(
            self.manual_frame, textvariable=self.username_var, width=30
        )
        username_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))

        ttk.Label(self.manual_frame, text="Personal Access Token:").grid(
            row=1, column=0, sticky=tk.W, padx=(0, 5), pady=(5, 0)
        )
        token_entry = ttk.Entry(
            self.manual_frame, textvariable=self.token_var, show="*", width=30
        )
        token_entry.grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(0, 10), pady=(5, 0)
        )

    def _create_login_button(self, parent: tk.Widget) -> None:
        """
        Create the login/logout button.

        Args:
            parent: Parent widget to contain the button
        """
        self.login_button = ttk.Button(
            parent, text="Login", command=self._handle_login_logout
        )
        self.login_button.grid(row=3, column=0, columnspan=3, pady=(10, 0))

    def _create_status_labels(self, parent: tk.Widget) -> None:
        """
        Create status and user information labels.

        Args:
            parent: Parent widget to contain the labels
        """
        self.login_status_label = ttk.Label(
            parent, textvariable=self.login_status_var, foreground="red"
        )
        self.login_status_label.grid(row=4, column=0, columnspan=3, pady=(5, 0))

        user_info_label = ttk.Label(
            parent,
            textvariable=self.user_info_var,
            foreground="green",
            font=("Arial", 9, "bold"),
        )
        user_info_label.grid(row=5, column=0, columnspan=3, pady=(5, 0))

    def on_login_mode_change(self) -> None:
        """Handle login mode radio button changes to show/hide appropriate sections."""
        mode = self.login_mode_var.get()
        if mode == "manual":
            self.manual_frame.grid()
            self.profile_frame.grid_remove()
        else:
            self.manual_frame.grid_remove()
            self.profile_frame.grid()
            self.refresh_profiles()

    def refresh_profiles(self) -> None:
        """Refresh the list of available configuration profiles."""
        try:
            profiles = self.config_manager.get_available_profiles()
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

    def on_profile_selected(self, event: tk.Event = None) -> None:
        """
        Handle profile selection from the combobox.

        Args:
            event: Tkinter event object (optional)
        """
        profile_name = self.profile_var.get()
        if profile_name:
            username = self.config_manager.get_profile_info(profile_name)
            if username:
                self.profile_info_var.set(f"Username: {username}")
            else:
                self.profile_info_var.set("No username found for this profile")
        else:
            self.profile_info_var.set("")

    def _handle_login_logout(self) -> None:
        """Handle login/logout button click based on current state."""
        if self.login_button["text"] == "Login":
            self._handle_login()
        else:
            self._handle_logout()

    def _handle_login(self) -> None:
        """Handle login attempt using the selected authentication method."""
        mode = self.login_mode_var.get()

        if mode == "manual":
            username = self.username_var.get().strip()
            token = self.token_var.get().strip()
            self.on_login("manual", {"username": username, "token": token})
        else:
            profile_name = self.profile_var.get()
            self.on_login("config", {"profile": profile_name})

    def _handle_logout(self) -> None:
        """Handle logout request."""
        self.on_logout()

    def update_login_state(
        self, is_logged_in: bool, username: str = "", error: str = ""
    ) -> None:
        """
        Update UI components based on current login state.

        Args:
            is_logged_in: Whether the user is currently logged in
            username: Username of the logged-in user (if applicable)
            error: Error message if login failed (if applicable)
        """
        if is_logged_in:
            self.login_status_var.set("Logged in successfully")
            self.login_status_label.config(foreground="green")
            self.login_button.config(text="Logout")
            self.user_info_var.set(f"Logged in as: {username}")
        else:
            if error:
                self.login_status_var.set(f"Login failed: {error}")
            else:
                self.login_status_var.set("Not logged in")
            self.login_status_label.config(foreground="red")
            self.login_button.config(text="Login")
            self.user_info_var.set("")

    def set_login_button_state(self, enabled: bool) -> None:
        """
        Enable or disable the login button.

        Args:
            enabled: True to enable the button, False to disable
        """
        self.login_button.config(state="normal" if enabled else "disabled")
