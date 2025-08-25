# Synapse Desktop Client

ElectronJS desktop application for interacting with Synapse, built with FastAPI backend and modern web frontend.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Electron      │    │   Backend       │
│   (HTML/CSS/JS) │◄───┤   Main Process  │◄───┤   (FastAPI)     │
│                 │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │                        │
                                │                        │
                                ▼                        ▼
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Renderer      │    │   Synapse       │
                       │   Process       │    │   Python Client │
                       │                 │    │                 │
                       └─────────────────┘    └─────────────────┘
```

- **Frontend**: Modern web UI (HTML/CSS/JavaScript)
- **Electron**: Cross-platform desktop wrapper
- **FastAPI Backend**: Python REST API server
- **Synapse Client**: Official Synapse Python SDK integration

## Quick Start

### Development

#### Quick Start
```bash
# Windows
.\start.bat

# macOS/Linux
./start.sh
```

#### Advanced Development & Debugging

For developers who need to debug the underlying Python code or JavaScript frontend, follow these detailed instructions:

##### Python Backend Debugging

To debug the FastAPI backend server with breakpoints and step-through debugging:

1. **Start the Python backend manually** (before running the start script):
   ```bash
   # Navigate to the backend directory
   cd backend
   
   # Run the FastAPI server directly - Or through your regular debug session in VSCode
   python server.py
   ```
   
   The backend will start on `http://localhost:8000` by default.

2. **Set up your IDE for Python debugging**:
   - **VS Code**: Open `backend/server.py`, set breakpoints, and use F5 to start debugging
   - **Other IDEs**: Configure to run `server.py` with your Python interpreter

3. **Start the Electron frontend** (after the backend is running):
   ```bash
   # From the synapse-electron root directory
   npm start
   ```

4. **Test your application** - breakpoints will be hit in both environments

This setup allows you to:
- Step through Python code in your IDE
- Inspect FastAPI request/response cycles
- Debug JavaScript interactions in the Chromium console
- Test the full application flow with complete debugging capabilities

**Important**: If you run `server.py` manually, you may still use the `start.bat`/`start.sh` scripts as. It will skip creating it's own Python process, however, the order of this is important. If you run the `start.bat`/`start.sh` scripts first it conflict with your debugging session (port already in use).

##### JavaScript Frontend Debugging

The ElectronJS application provides access to Chromium's developer tools:

1. **Open Developer Console**:
   - In the running Electron app, press `Ctrl+Shift+I` (Windows/Linux) or `Cmd+Option+I` (macOS)

2. **Set breakpoints in JavaScript**:
   - Open the Sources tab in Developer Tools
   - Navigate to your JavaScript files (`app.js`, etc.)
   - Click on line numbers to set breakpoints
   - Use `debugger;` statements in your code for programmatic breakpoints

### Production Build
```bash
npm run build           # All platforms
npm run dist:win        # Windows only
npm run dist:mac        # macOS only
npm run dist:linux      # Linux only
```

## Project Structure

```
synapse-electron/
├── src/                    # Frontend source code
│   ├── index.html         # Main UI layout
│   ├── app.js             # Frontend logic
│   └── styles.css         # UI styling
├── backend/                # Python FastAPI server
│   ├── server.py          # Main FastAPI application
│   ├── models/            # Pydantic data models
│   ├── services/          # Business logic layer
│   └── utils/             # Shared utilities
├── assets/                 # Static resources
├── main.js                 # Electron main process
├── preload.js             # Electron security bridge
├── package.json           # Node.js dependencies
├── .electron-builder.json # Electron Builder configuration
├── start.bat/.sh          # Development startup scripts
└── README.md              # This file
```

## Directory Details

### `/src/` - Frontend
- **index.html**: Main application UI with login, file browser, and upload/download interfaces
- **app.js**: JavaScript application logic, API communication, UI event handling
- **styles.css**: CSS styling for modern, responsive interface

### `/backend/` - Python API Server
- **server.py**: FastAPI application with authentication, file operations, and system endpoints
- **models/**: Pydantic models for request/response validation
- **services/**: Core business logic (authentication, file operations, Synapse integration)
- **utils/**: Logging, WebSocket, and system utilities

### Root Files
- **main.js**: Electron main process - window management, backend lifecycle
- **preload.js**: Secure communication bridge between renderer and main process
- **start.bat/.sh**: Development scripts that handle backend startup and Electron launch

## Development Workflow

1. **Backend**: FastAPI server runs on `http://localhost:8000`
2. **Frontend**: Electron renderer loads from local HTML/CSS/JS
3. **Communication**: REST API calls from frontend to backend
4. **Authentication**: Token-based auth with Synapse
5. **File Operations**: Upload/download through Synapse Python SDK

## Key Features

- **Authentication**: Username/token and profile-based login
- **File Browser**: Navigate Synapse projects and folders
- **Bulk Operations**: Multi-file upload/download with progress tracking
- **Cross-Platform**: Windows, macOS, Linux support
- **Real-time Logging**: WebSocket-based log streaming to UI

## Requirements

- **Node.js**: 16+ for Electron
- **Python**: 3.8+ for backend
- **Dependencies**: Managed via npm (frontend) and pip (backend)

## Build Process

1. **Development**: Scripts handle both backend and frontend startup
2. **Production**: Electron Builder packages Python backend as executable
3. **Distribution**: Creates platform-specific installers/packages

## Configuration

- **Backend Port**: Configured in `backend/server.py` (default: 8000)
- **Build Settings**: Defined in package.json and `.electron-builder.json`
- **Python Environment**: Managed in `backend/` directory
