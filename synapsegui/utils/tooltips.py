"""
Tooltip utility for GUI components
"""
import tkinter as tk


class ToolTip:
    """
    Create a tooltip for a given widget.
    
    Provides hover-based tooltips that appear when the mouse enters
    a widget and disappear when it leaves.
    """

    def __init__(self, widget: tk.Widget, text: str = "widget info") -> None:
        """
        Initialize the tooltip for a widget.

        Args:
            widget: The widget to attach the tooltip to
            text: The text to display in the tooltip
        """
        self.widget = widget
        self.text = text
        self.widget.bind("<Enter>", self.enter)
        self.widget.bind("<Leave>", self.leave)
        self.tipwindow = None

    def enter(self, event: tk.Event = None) -> None:
        """
        Handle mouse enter event to show tooltip.

        Args:
            event: Tkinter event object (optional)
        """
        self.show_tooltip()

    def leave(self, event: tk.Event = None) -> None:
        """
        Handle mouse leave event to hide tooltip.

        Args:
            event: Tkinter event object (optional)
        """
        self.hide_tooltip()

    def show_tooltip(self) -> None:
        """Display the tooltip window near the widget."""
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

    def hide_tooltip(self) -> None:
        """Hide and destroy the tooltip window."""
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()
