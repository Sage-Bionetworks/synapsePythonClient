# Synapse GUI Developer Guide

Welcome to the Synapse GUI development guide. This document provides comprehensive information for developers who want to contribute to, maintain, or extend the Synapse desktop GUI application.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Development Setup](#development-setup)
- [Code Structure](#code-structure)
- [Adding New Features](#adding-new-features)
- [Testing Guidelines](#testing-guidelines)
- [UI/UX Guidelines](#uiux-guidelines)
- [Performance Considerations](#performance-considerations)
- [Debugging and Troubleshooting](#debugging-and-troubleshooting)
- [Contributing Guidelines](#contributing-guidelines)

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

### Prerequisites

```bash
# Python 3.8+ required
python --version

# Required packages
pip install synapseclient
pip install tkinter  # Usually included with Python
pip install pytest   # For testing
pip install black    # For code formatting
pip install pylint   # For linting
```

### Environment Setup

```bash
# Clone the repository
git clone https://github.com/Sage-Bionetworks/synapsePythonClient.git
cd synapsePythonClient/synapsegui

# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the GUI
python gui_entrypoint.py
```

### Development Tools

```bash
# Code formatting
black --line-length 88 synapsegui/

# Linting
pylint synapsegui/

# Type checking (if using type hints)
mypy synapsegui/

# Testing
pytest tests/
```

## Code Structure

```
synapsegui/
├── __init__.py                # Package initialization
├── gui_entrypoint.py          # Main application entry point
├── README.md                  # This file
├── REFACTORING_GUIDE.md       # Architecture migration guide
│
├── models/                    # Business logic and data
│   ├── __init__.py
│   ├── synapse_client.py      # Synapse API operations
│   ├── config.py              # Configuration management
│   └── app_state.py           # Application state models
│
├── components/                # UI components (reusable)
│   ├── __init__.py
│   ├── login_component.py     # Authentication UI
│   ├── download_component.py  # File download UI
│   ├── upload_component.py    # File upload UI
│   ├── output_component.py    # Logging/output display
│   └── base_component.py      # Base component class
│
├── controllers/               # Application logic coordination
│   ├── __init__.py
│   ├── app_controller.py      # Main application controller
│   ├── login_controller.py    # Login logic
│   └── file_controller.py     # File operation logic
│
├── utils/                     # Utilities and helpers
│   ├── __init__.py
│   ├── progress.py           # Progress tracking
│   ├── tooltips.py           # UI helpers
│   ├── validators.py         # Input validation
│   └── threading_utils.py    # Threading helpers
│
├── tests/                     # Test files
│   ├── __init__.py
│   ├── test_models/
│   ├── test_components/
│   ├── test_controllers/
│   └── test_utils/
│
└── assets/                    # Static resources
    ├── icons/
    ├── images/
    └── styles/
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

## Testing Guidelines

### Unit Testing

```python
# tests/test_models/test_synapse_client.py
import pytest
from unittest.mock import Mock, patch
from synapsegui.models.synapse_client import SynapseClientManager

class TestSynapseClientManager:
    def setup_method(self):
        self.client = SynapseClientManager()

    @pytest.mark.asyncio
    async def test_login_manual_success(self):
        """Test successful manual login"""
        with patch('synapseclient.Synapse') as mock_synapse:
            mock_instance = Mock()
            mock_synapse.return_value = mock_instance
            mock_instance.username = "test@example.com"

            result = await self.client.login_manual("test@example.com", "token")

            assert result["success"] is True
            assert result["username"] == "test@example.com"
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_models/test_synapse_client.py

# Run with coverage
pytest --cov=synapsegui

# Run integration tests only
pytest tests/test_integration/

# Run with verbose output
pytest -v
```

## UI/UX Guidelines

### Design Principles

1. **Consistency**: Use consistent spacing, fonts, and colors
2. **Accessibility**: Support keyboard navigation and screen readers
3. **Responsiveness**: Handle window resizing gracefully
4. **Progressive Disclosure**: Show advanced options when needed
5. **Clear Feedback**: Provide immediate feedback for user actions

### Component Standards

```python
# Standard component structure
class StandardComponent:
    def __init__(self, parent, controller, **kwargs):
        self.parent = parent
        self.controller = controller
        self.frame = None
        self.create_ui()
        self.setup_bindings()

    def create_ui(self):
        """Create UI elements with consistent styling"""
        self.frame = ttk.LabelFrame(self.parent, text="Component Title", padding="10")
        self.frame.grid(sticky="ew", padx=5, pady=5)

        # Use grid layout with proper weights
        self.frame.columnconfigure(1, weight=1)

    def setup_bindings(self):
        """Setup keyboard and event bindings"""
        self.frame.bind("<Return>", self.handle_enter)
        self.frame.bind("<Escape>", self.handle_escape)

    def set_enabled(self, enabled: bool):
        """Standard method for enabling/disabling component"""
        state = "normal" if enabled else "disabled"
        for child in self.frame.winfo_children():
            if hasattr(child, 'config'):
                child.config(state=state)
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

### Memory Management

```python
# Proper cleanup in components
class Component:
    def __init__(self, parent):
        self.parent = parent
        self.cleanup_callbacks = []

    def add_cleanup_callback(self, callback):
        """Register cleanup callback"""
        self.cleanup_callbacks.append(callback)

    def destroy(self):
        """Clean up resources"""
        for callback in self.cleanup_callbacks:
            try:
                callback()
            except Exception:
                pass  # Log error but continue cleanup

        if self.frame:
            self.frame.destroy()
```

## Debugging and Troubleshooting

### Logging Setup

```python
# utils/logging_config.py
import logging
import sys

def setup_logging(level=logging.INFO):
    """Setup application logging"""
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler('synapse_gui.log')
    file_handler.setFormatter(formatter)

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
```

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

## Contributing Guidelines

### Code Standards

1. **PEP 8 Compliance**: Follow Python style guidelines
2. **Type Hints**: Use type hints for public APIs
3. **Docstrings**: Document all public classes and methods
4. **Error Handling**: Implement proper error handling and logging
5. **Testing**: Include tests for new functionality

### Git Workflow

```bash
# Create feature branch
git checkout -b feature/new-gui-component

# Make changes and commit
git add .
git commit -m "Add new GUI component for feature X"

# Push and create PR
git push origin feature/new-gui-component
```

### Code Review Checklist

- [ ] Code follows style guidelines
- [ ] Tests are included and pass
- [ ] Documentation is updated
- [ ] Error handling is implemented
- [ ] Performance impact is considered
- [ ] UI/UX guidelines are followed
- [ ] Backwards compatibility is maintained

### Pull Request Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Screenshots (if applicable)
Add screenshots of UI changes

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Tests added/updated
- [ ] Documentation updated
```

## Additional Resources

- [Tkinter Documentation](https://docs.python.org/3/library/tkinter.html)
- [Synapse Python Client Docs](https://python-docs.synapse.org/)
- [Python Threading](https://docs.python.org/3/library/threading.html)
- [Pytest Documentation](https://docs.pytest.org/)
- [Python Type Hints](https://docs.python.org/3/library/typing.html)

---

For questions or support, please:
1. Check existing [GitHub Issues](https://github.com/Sage-Bionetworks/synapsePythonClient/issues)
2. Create a new issue with detailed description
3. Contact the development team via Slack or email

Happy coding! 🚀
