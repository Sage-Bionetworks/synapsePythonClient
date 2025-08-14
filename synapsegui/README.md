# Synapse GUI Developer Guide

Welcome to the Synapse GUI development guide. This document provides comprehensive information for developers who want to contribute to, maintain, or extend the Synapse desktop GUI application.


## Architecture Overview

The Synapse GUI follows a **Model-View-Controller (MVC)** architecture with component-based design:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Models      │    │   Controllers   │    │   Components    │
│                 │    │                 │    │      (Views)    │
│ • SynapseClient │◄──►│ AppController   │◄──►│ • LoginComponent│
│ • ConfigManager │    │                 │    │ • DownloadComp  │
│ • Data Models   │    │                 │    │ • UploadComp    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Key Principles

1. **Separation of Concerns**: Business logic, UI, and coordination are separate
2. **Dependency Injection**: Components receive dependencies via constructors
3. **Event-Driven**: Loose coupling through callbacks and events
4. **Async Operations**: Non-blocking UI with proper progress feedback
5. **Testability**: Each layer can be tested independently

## Development Setup

See the contributing guide at the root of the project for instructions to set up your local environment.

## Code Structure

```
build_gui.sh                   # Shell script to build the GUI to run on mac machines
build_windows_native_gui.bat   # Batch file to build an EXE to run on windows machines
gui_entrypoint.py              # Entrypoint for running and building the GUI
synapsegui/
├── synapse_gui.py             # Main application entry point
├── models/                    # Business logic and data
├── components/                # UI components (reusable)
├── controllers/               # Application logic coordination
├── utils/                     # Utilities and helpers

```

## Adding New Features

### 1. Creating a New UI Component

```python
# components/new_feature_component.py
from tkinter import ttk
from .base_component import BaseComponent

class NewFeatureComponent(BaseComponent):
    """Component for new feature functionality"""

    def __init__(self, parent, controller, **kwargs):
        super().__init__(parent, **kwargs)
        self.controller = controller
        self.create_ui()

    def create_ui(self):
        """Create the UI elements"""
        self.frame = ttk.LabelFrame(self.parent, text="New Feature")
        self.frame.grid(sticky="ew", padx=5, pady=5)

        # Add your UI elements here
        ttk.Button(
            self.frame,
            text="Action",
            command=self.handle_action
        ).grid(row=0, column=0)

    def handle_action(self):
        """Handle user action - delegate to controller"""
        self.controller.handle_new_feature_action()

    def update_state(self, state):
        """Update UI based on state changes"""
        # Update UI elements based on state
        pass
```

### 2. Adding Business Logic

```python
# models/new_feature_model.py
from typing import Dict, Any

class NewFeatureModel:
    """Model for new feature data and operations"""

    def __init__(self, synapse_client):
        self.synapse_client = synapse_client

    async def perform_operation(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Perform the business operation"""
        try:
            # Implement your business logic here
            result = await self._do_operation(params)
            return {"success": True, "data": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _do_operation(self, params):
        """Private method for actual operation"""
        # Implementation details
        pass
```

### 3. Integration Steps

1. **Add component to main window**:
   ```python
   # In main GUI class
   self.new_feature_component = NewFeatureComponent(parent, self.controller)
   ```

2. **Register with controller**:
   ```python
   self.controller.set_new_feature_component(self.new_feature_component)
   ```

3. **Add tests**:
   ```python
   # tests/test_components/test_new_feature_component.py
   def test_new_feature_component():
       # Test component functionality
       pass
   ```

## Performance Considerations

### Threading Best Practices

```python
# Good: Non-blocking operation
def handle_long_operation(self):
    def worker():
        try:
            # Long-running operation
            result = self.model.long_operation()

            # Update UI on main thread
            self.root.after(0, lambda: self.update_ui(result))
        except Exception as e:
            self.root.after(0, lambda: self.show_error(str(e)))

    threading.Thread(target=worker, daemon=True).start()

# Bad: Blocking UI
def handle_long_operation_bad(self):
    result = self.model.long_operation()  # Blocks UI!
    self.update_ui(result)
```


## Debugging and Troubleshooting

### Common Issues and Solutions

#### 1. UI Freezing
```python
# Problem: Long operation blocking UI
def bad_operation(self):
    time.sleep(10)  # Blocks UI thread
    self.update_ui()

# Solution: Use threading
def good_operation(self):
    def worker():
        time.sleep(10)  # Runs in background
        self.root.after(0, self.update_ui)  # Update UI on main thread

    threading.Thread(target=worker, daemon=True).start()
```

#### 2. Memory Leaks
```python
# Problem: Not cleaning up references
class BadComponent:
    def __init__(self):
        self.timer = self.root.after(1000, self.update)  # Never cancelled

# Solution: Proper cleanup
class GoodComponent:
    def __init__(self):
        self.timer = None
        self.start_timer()

    def start_timer(self):
        self.timer = self.root.after(1000, self.update)

    def destroy(self):
        if self.timer:
            self.root.after_cancel(self.timer)
```
