"""
Output component UI - separated from main window
"""
import tkinter as tk
from tkinter import scrolledtext, ttk
from typing import Optional


class OutputComponent:
    """Output/logging UI component"""

    def __init__(self, parent: tk.Widget):
        self.parent = parent

        # UI references
        self.output_text: Optional[scrolledtext.ScrolledText] = None
        self.status_var = tk.StringVar(value="Ready")
        self.frame: Optional[ttk.LabelFrame] = None
        self.status_bar: Optional[ttk.Label] = None

        self.create_ui()

    def create_ui(self) -> None:
        """Create output UI components"""
        # Output section
        self.frame = ttk.LabelFrame(self.parent, text="Output", padding="5")
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

        self.output_text = scrolledtext.ScrolledText(
            self.frame, height=20, wrap=tk.WORD, font=("Consolas", 9)
        )
        self.output_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Clear button
        clear_button = ttk.Button(
            self.frame, text="Clear Output", command=self.clear_output
        )
        clear_button.grid(row=1, column=0, pady=(5, 0))

        # Status bar (separate from output frame)
        self.status_bar = ttk.Label(
            self.parent, textvariable=self.status_var, relief=tk.SUNKEN
        )

    def log_message(self, message: str, error: bool = False) -> None:
        """Add message to output text widget"""
        if self.output_text:
            self.output_text.insert(tk.END, f"{message}\n")
            self.output_text.see(tk.END)

            if error:
                # Color the last line red for errors
                line_start = self.output_text.index("end-1c linestart")
                line_end = self.output_text.index("end-1c lineend")
                self.output_text.tag_add("error", line_start, line_end)
                self.output_text.tag_config("error", foreground="red")

            # Update the display
            self.parent.update_idletasks()

    def clear_output(self) -> None:
        """Clear the output text widget"""
        if self.output_text:
            self.output_text.delete(1.0, tk.END)

    def set_status(self, status: str) -> None:
        """Set the status bar message"""
        self.status_var.set(status)

    def get_frame(self) -> ttk.LabelFrame:
        """Get the main frame for this component"""
        return self.frame

    def get_status_bar(self) -> ttk.Label:
        """Get the status bar widget"""
        return self.status_bar
