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

### Prerequisites

Before running the Synapse Desktop Client, ensure you have the following system dependencies installed:

#### Required Software
- **Node.js** (v14 or higher) - Download from [https://nodejs.org/](https://nodejs.org/)
  - Includes npm (Node Package Manager)
- **Python** (v3.7 or higher) - Download from [https://python.org/](https://python.org/)
  - On Windows: Use `python` command
  - On macOS/Linux: Use `python3` command

#### Environment Setup
- Ensure both Node.js and Python are added to your system PATH
- The start scripts will automatically check for these dependencies and guide you if anything is missing

### Development

#### Quick Start
```bash
# Make sure you are in the `synapse-electron` directory
# cd synapse-electron

# Windows
.\start.bat

# macOS/Linux
./start.sh
```

After starting the application you should see a few things. First, you should see console logs similar to what is shown below:


```
Starting Synapse Desktop Client...
Backend will start on http://localhost:8000
WebSocket will start on ws://localhost:8001


> synapse-desktop-client@0.1.0 dev
> electron .


12:22:17.807 > Starting Python backend...
12:22:17.821 > Attempt 1/30: Checking backend health at http://127.0.0.1:8000/health
12:22:17.841 > Backend not ready yet (attempt 1): connect ECONNREFUSED 127.0.0.1:8000
12:22:18.847 > Attempt 2/30: Checking backend health at http://127.0.0.1:8000/health
12:22:18.850 > Backend not ready yet (attempt 2): connect ECONNREFUSED 127.0.0.1:8000
12:22:19.852 > Attempt 3/30: Checking backend health at http://127.0.0.1:8000/health
12:22:19.856 > Python Backend: INFO:     127.0.0.1:52661 - "GET /health HTTP/1.1" 200 OK

12:22:19.858 > Python backend is ready
12:22:19.971 > Synapse Electron app initialized successfully
12:22:19.988 > WebSocket connected to Python backend
```

Secondly, an application window should pop up allowing you to interact with the GUI.

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

   The FastAPI server also automatically exposes API Docs at the `http://localhost:8000/docs` URL.

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

## Releasing

When incrementing the desktop application for a release, there are **2 versions** that need to be updated:

1. **`package.json`** - Update the `version` field:
   ```json
   {
     "name": "synapse-desktop-client",
     "version": "X.Y.Z",
     ...
   }
   ```

2. **`backend/server.py`** - Update the FastAPI `version` parameter:
   ```python
   app = FastAPI(
       title="Synapse Desktop Client API",
       description="Backend API for Synapse Desktop Client",
       version="X.Y.Z",
       lifespan=lifespan,
   )
   ```

### Release Process

1. **Create Release Branch**: Create a new release candidate branch from the develop branch using the release candidate naming convention:
   ```bash
   git checkout -b synapsedesktopclient/vX.Y.Z-rc develop
   ```
   where `X.Y.Z` is the semantic version (e.g., `synapsedesktopclient/v1.2.3-rc`)

2. **Update Versions**: Update both version locations mentioned above to match the release version.

3. **Pre-release Distribution**: At this point in time we will only use the "pre-release" portion to push out the desktop client. This is temporary while we look to migrate to the sage monorepo.

4. **Create and Deploy the Release Candidate**: Desktop client releases are created as GitHub releases.

   - Click the "Draft a new release" button and fill the following values:
     - **Tag version**: `synapsedesktopclient/vX.Y.Z-rc` where `X.Y.Z` is the semantic version
     - **Target**: the previously created `synapsedesktopclient/vX.Y.Z-rc` branch
     - **Release title**: Same as tag version (`synapsedesktopclient/vX.Y.Z-rc`)
   - **IMPORTANT**: Check the "Set as a pre-release" checkbox

**Note**: Replace `X.Y.Z` with the actual semantic version numbers (e.g., `1.2.3`).
