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
```bash
# Windows
.\start.bat

# macOS/Linux
./start.sh
```

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
