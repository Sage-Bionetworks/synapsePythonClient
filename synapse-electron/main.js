const { app, BrowserWindow, ipcMain, dialog, shell, session } = require('electron');
const path = require('path');
const { spawn } = require('child_process');
const axios = require('axios');
const WebSocket = require('ws');
const log = require('electron-log');

class SynapseElectronApp {
    constructor() {
        this.pythonProcess = null;
        this.mainWindow = null;
        this.backendPort = 8000;
        this.wsPort = 8001;
        this.websocketServer = null;
        this.connectedClients = new Set();
    }

    async startPythonBackend() {
        log.info('Starting Python backend...');

        // Determine Python executable path
        let pythonExe;
        let backendPath;

        if (app.isPackaged) {
            // In packaged app, use the compiled backend
            backendPath = path.join(process.resourcesPath, 'backend');

            if (process.platform === 'win32') {
                pythonExe = path.join(backendPath, 'synapse-backend.exe');
            } else {
                pythonExe = path.join(backendPath, 'synapse-backend');
            }

            log.info(`Using packaged backend: ${pythonExe}`);

            this.pythonProcess = spawn(pythonExe, [
                '--port', this.backendPort.toString(),
                '--ws-port', this.wsPort.toString()
            ], {
                stdio: ['pipe', 'pipe', 'pipe'],
                cwd: backendPath
            });
        } else {
            // In development, use the Python script
            backendPath = path.join(__dirname, 'backend');

            if (process.platform === 'win32') {
                // On Windows, try to use the venv first, fallback to system python
                const venvPython = path.join(backendPath, 'venv', 'Scripts', 'python.exe');
                if (require('fs').existsSync(venvPython)) {
                    pythonExe = venvPython;
                    log.info(`Using virtual environment Python: ${pythonExe}`);
                } else {
                    pythonExe = 'python.exe';
                    log.info('Virtual environment not found, using system Python');
                }
            } else {
                pythonExe = 'python3';
            }

            this.pythonProcess = spawn(pythonExe, [
                path.join(backendPath, 'server.py'),
                '--port', this.backendPort.toString(),
                '--ws-port', this.wsPort.toString()
            ], {
                stdio: ['pipe', 'pipe', 'pipe'],
                cwd: backendPath
            });
        }

        this.pythonProcess.stdout.on('data', (data) => {
            log.info(`Python Backend: ${data.toString()}`);
        });

        this.pythonProcess.stderr.on('data', (data) => {
            log.error(`Python Backend Error: ${data.toString()}`);
        });

        this.pythonProcess.on('close', (code) => {
            log.info(`Python backend exited with code ${code}`);
        });

        this.pythonProcess.on('error', (error) => {
            log.error(`Failed to start Python backend: ${error.message}`);
        });

        // Wait for backend to be ready
        await this.waitForBackend();

        // Start WebSocket server connection
        this.setupWebSocketConnection();
    }

    setupWebSocketConnection() {
        const wsUrl = `ws://127.0.0.1:${this.wsPort}`;

        const connectWebSocket = () => {
            try {
                this.ws = new WebSocket(wsUrl);

                this.ws.on('open', () => {
                    log.info('WebSocket connected to Python backend');
                });

                this.ws.on('message', (data) => {
                    try {
                        const message = JSON.parse(data.toString());
                        this.handleWebSocketMessage(message);
                    } catch (error) {
                        log.error('Error parsing WebSocket message:', error);
                    }
                });

                this.ws.on('close', () => {
                    log.warn('WebSocket connection closed, attempting to reconnect...');
                    setTimeout(connectWebSocket, 2000);
                });

                this.ws.on('error', (error) => {
                    log.error('WebSocket error:', error);
                });
            } catch (error) {
                log.error('Failed to connect WebSocket:', error);
                setTimeout(connectWebSocket, 2000);
            }
        };

        connectWebSocket();
    }

    handleWebSocketMessage(message) {
        // Forward WebSocket messages to renderer process
        if (this.mainWindow && !this.mainWindow.isDestroyed()) {
            this.mainWindow.webContents.send('websocket-message', message);
        }
    }

    async waitForBackend(maxRetries = 30) {
        for (let i = 0; i < maxRetries; i++) {
            try {
                log.info(`Attempt ${i + 1}/${maxRetries}: Checking backend health at http://127.0.0.1:${this.backendPort}/health`);
                await axios.get(`http://127.0.0.1:${this.backendPort}/health`);
                log.info('Python backend is ready');
                return;
            } catch (error) {
                log.info(`Backend not ready yet (attempt ${i + 1}): ${error.message}`);
                if (i === maxRetries - 1) {
                    throw new Error('Python backend failed to start within timeout');
                }
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
    }

    createWindow() {
        // Check if we're running in a headless environment
        const isHeadless = process.env.DISPLAY === ':99' || process.env.CI || process.env.HEADLESS;

        const webPrefs = {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, 'preload.js'),
            webSecurity: true,
            // Force software rendering to avoid GPU issues
            hardwareAcceleration: false,
            // Additional security settings
            allowRunningInsecureContent: false,
            experimentalFeatures: false
        };

        // Add offscreen rendering for headless mode
        if (isHeadless) {
            webPrefs.offscreen = true;
        }

        this.mainWindow = new BrowserWindow({
            width: 1200,
            height: 800,
            minWidth: 800,
            minHeight: 600,
            webPreferences: webPrefs,
            icon: path.join(__dirname, 'assets', 'icon.png'),
            show: !isHeadless // Don't show in headless mode
        });

        this.mainWindow.loadFile(path.join(__dirname, 'src', 'index.html'))
            .then(() => {
                log.info('HTML file loaded successfully');
            })
            .catch((error) => {
                log.error('Failed to load HTML file:', error);
            });

        // Show window when ready (only if not headless)
        if (!isHeadless) {
            this.mainWindow.once('ready-to-show', () => {
                this.mainWindow.show();

                // Center window on screen
                this.mainWindow.center();
            });
        } else {
            console.log('Running in headless mode - window will not be displayed');
        }

        // Handle window closed
        this.mainWindow.on('closed', () => {
            this.mainWindow = null;
        });

        // Add debugging for web contents
        this.mainWindow.webContents.on('did-finish-load', () => {
            log.info('WebContents finished loading');
        });

        this.mainWindow.webContents.on('did-fail-load', (event, errorCode, errorDescription) => {
            log.error('WebContents failed to load:', errorCode, errorDescription);
        });

        // Development: Open DevTools
        if (!app.isPackaged) {
            this.mainWindow.webContents.openDevTools();
        }

        // Configure CORS security for the renderer process
        this.setupCORSSecurity();
    }

    setupCORSSecurity() {
        const session = this.mainWindow.webContents.session;

        // Define allowed origins that match the backend CORS configuration
        const allowedOrigins = [
            `http://localhost:${this.backendPort}`,
            `http://127.0.0.1:${this.backendPort}`,
            'file://',
            // Allow any localhost port for development flexibility
            /^http:\/\/localhost:\d+$/,
            /^http:\/\/127\.0\.0\.1:\d+$/
        ];

        // Define the Content Security Policy
        const cspPolicy = [
            `default-src 'self' http://localhost:* http://127.0.0.1:* file:`,
            `script-src 'self' 'unsafe-inline' http://localhost:* http://127.0.0.1:* https://cdnjs.cloudflare.com`,
            `style-src 'self' 'unsafe-inline' http://localhost:* http://127.0.0.1:* https://cdnjs.cloudflare.com`,
            `img-src 'self' data: blob: http://localhost:* http://127.0.0.1:* https://cdnjs.cloudflare.com`,
            `connect-src 'self' ws://localhost:* ws://127.0.0.1:* http://localhost:* http://127.0.0.1:*`,
            `font-src 'self' data: https://cdnjs.cloudflare.com`,
            `object-src 'none'`,
            `base-uri 'self'`,
            `form-action 'self'`
        ].join('; ');

        // Set up request filtering for enhanced security
        session.webRequest.onBeforeRequest((details, callback) => {
            const url = new URL(details.url);

            // Allow local file protocol for the app's own files
            if (url.protocol === 'file:') {
                callback({ cancel: false });
                return;
            }

            // Allow DevTools protocol and related resources
            if (url.protocol === 'devtools:' ||
                url.hostname === 'chrome-devtools-frontend.appspot.com') {
                callback({ cancel: false });
                return;
            }

            // Allow requests to backend server
            if (url.hostname === 'localhost' || url.hostname === '127.0.0.1') {
                callback({ cancel: false });
                return;
            }

            // Allow requests to trusted CDNs
            if (url.hostname === 'cdnjs.cloudflare.com') {
                callback({ cancel: false });
                return;
            }

            // Block all other external requests for security
            log.warn(`Blocked request to external origin: ${details.url}`);
            callback({ cancel: true });
        });

        // Set up response headers for additional security
        session.webRequest.onHeadersReceived((details, callback) => {
            const responseHeaders = details.responseHeaders || {};
            const url = new URL(details.url);

            // Don't apply CSP to DevTools resources
            if (url.protocol === 'devtools:' || url.hostname === 'chrome-devtools-frontend.appspot.com') {
                callback({ responseHeaders });
                return;
            }

            // Add security headers for app resources only
            responseHeaders['X-Content-Type-Options'] = ['nosniff'];
            responseHeaders['X-Frame-Options'] = ['DENY'];
            responseHeaders['X-XSS-Protection'] = ['1; mode=block'];
            responseHeaders['Referrer-Policy'] = ['strict-origin-when-cross-origin'];

            // Set Content Security Policy for app resources only
            // Force override any existing CSP
            responseHeaders['Content-Security-Policy'] = [cspPolicy];

            callback({ responseHeaders });
        });

        // Also set CSP on the main frame directly
        session.webRequest.onBeforeSendHeaders((details, callback) => {
            // Inject CSP header for main frame requests
            if (details.resourceType === 'mainFrame') {
                const requestHeaders = details.requestHeaders || {};
                callback({ requestHeaders });
            } else {
                callback({});
            }
        });

        // Set CSP directly on webContents for immediate effect
        this.mainWindow.webContents.on('dom-ready', () => {
            // Only apply to main window, not DevTools
            if (this.mainWindow.webContents.getURL().startsWith('file://')) {
                this.mainWindow.webContents.executeJavaScript(`
                    (function(cspContent) {
                        // Remove any existing CSP meta tags that might conflict
                        const existingCSPs = document.querySelectorAll('meta[http-equiv="Content-Security-Policy"]');
                        existingCSPs.forEach(csp => csp.remove());

                        // Add our CSP meta tag
                        const meta = document.createElement('meta');
                        meta.setAttribute('http-equiv', 'Content-Security-Policy');
                        meta.setAttribute('content', cspContent);
                        document.head.appendChild(meta);

                        // Log for debugging
                        console.log('CSP applied:', cspContent);
                    })('${cspPolicy.replace(/'/g, "\\'")}');
                `).catch(err => {
                    log.error('Failed to inject CSP:', err);
                });
            }
        });

        log.info('CORS security configuration applied');
    }

    setupIPC() {
        // Authentication endpoints
        ipcMain.handle('synapse-login', async (event, credentials) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/auth/login`, credentials);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        ipcMain.handle('synapse-logout', async (event) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/auth/logout`);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        ipcMain.handle('synapse-get-profiles', async (event) => {
            try {
                const response = await axios.get(`http://127.0.0.1:${this.backendPort}/auth/profiles`);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        // File operations
        ipcMain.handle('synapse-upload', async (event, uploadData) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/files/upload`, uploadData);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        ipcMain.handle('synapse-download', async (event, downloadData) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/files/download`, downloadData);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        // Bulk operations
        ipcMain.handle('synapse-scan-directory', async (event, scanData) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/files/scan-directory`, scanData);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        ipcMain.handle('synapse-enumerate', async (event, containerData) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/bulk/enumerate`, containerData);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        ipcMain.handle('synapse-bulk-download', async (event, bulkData) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/bulk/download`, bulkData);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        ipcMain.handle('synapse-bulk-upload', async (event, bulkData) => {
            try {
                const response = await axios.post(`http://127.0.0.1:${this.backendPort}/bulk/upload`, bulkData);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });

        // File dialogs
        ipcMain.handle('show-open-dialog', async (event, options) => {
            const result = await dialog.showOpenDialog(this.mainWindow, options);
            return result;
        });

        ipcMain.handle('show-save-dialog', async (event, options) => {
            const result = await dialog.showSaveDialog(this.mainWindow, options);
            return result;
        });

        // External links
        ipcMain.handle('open-external', async (event, url) => {
            await shell.openExternal(url);
        });

        // Application info
        ipcMain.handle('get-app-version', () => {
            return app.getVersion();
        });

        // System utilities
        ipcMain.handle('get-home-directory', async (event) => {
            try {
                const response = await axios.get(`http://127.0.0.1:${this.backendPort}/system/home-directory`);
                return { success: true, data: response.data };
            } catch (error) {
                return {
                    success: false,
                    error: error.response?.data?.detail || error.message
                };
            }
        });
    }

    async initialize() {
        await app.whenReady();

        // Configure global security settings
        this.setupGlobalSecurity();

        try {
            await this.startPythonBackend();
            this.createWindow();
            this.setupIPC();

            log.info('Synapse Electron app initialized successfully');
        } catch (error) {
            log.error('Failed to initialize app:', error);

            // Show error dialog and exit
            dialog.showErrorBox(
                'Startup Error',
                `Failed to start the application: ${error.message}`
            );
            app.quit();
        }
    }

    setupGlobalSecurity() {
        // Prevent new window creation with insecure settings
        app.on('web-contents-created', (event, contents) => {
            contents.on('new-window', (event, navigationUrl) => {
                const parsedUrl = new URL(navigationUrl);

                // Allow DevTools protocol
                if (parsedUrl.protocol === 'devtools:') {
                    return;
                }

                // Only allow localhost and local file URLs
                if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:' && parsedUrl.protocol !== 'file:') {
                    event.preventDefault();
                    log.warn(`Blocked new window with protocol: ${parsedUrl.protocol}`);
                    return;
                }

                if (parsedUrl.protocol === 'http:' || parsedUrl.protocol === 'https:') {
                    if (parsedUrl.hostname !== 'localhost' && parsedUrl.hostname !== '127.0.0.1') {
                        event.preventDefault();
                        log.warn(`Blocked new window to external host: ${parsedUrl.hostname}`);
                        return;
                    }
                }
            });

            // Prevent navigation to external URLs
            contents.on('will-navigate', (event, navigationUrl) => {
                const parsedUrl = new URL(navigationUrl);

                // Allow file protocol for local files
                if (parsedUrl.protocol === 'file:') {
                    return;
                }

                // Allow DevTools protocol
                if (parsedUrl.protocol === 'devtools:') {
                    return;
                }

                // Allow localhost/127.0.0.1 only
                if (parsedUrl.protocol === 'http:' || parsedUrl.protocol === 'https:') {
                    if (parsedUrl.hostname !== 'localhost' && parsedUrl.hostname !== '127.0.0.1') {
                        event.preventDefault();
                        log.warn(`Blocked navigation to external URL: ${navigationUrl}`);
                        return;
                    }
                }
            });
        });

        // Set additional app-level security
        app.on('certificate-error', (event, webContents, url, error, certificate, callback) => {
            // For localhost development, we might need to handle self-signed certificates
            const parsedUrl = new URL(url);
            if (parsedUrl.hostname === 'localhost' || parsedUrl.hostname === '127.0.0.1') {
                // Allow certificate errors for localhost in development
                if (!app.isPackaged) {
                    event.preventDefault();
                    callback(true);
                    return;
                }
            }

            // Deny all other certificate errors
            callback(false);
        });

        log.info('Global security configuration applied');
    }

    cleanup() {
        log.info('Cleaning up application...');

        if (this.ws) {
            this.ws.close();
        }

        if (this.pythonProcess) {
            this.pythonProcess.kill('SIGTERM');

            // Force kill after 5 seconds if still running
            setTimeout(() => {
                if (this.pythonProcess && !this.pythonProcess.killed) {
                    this.pythonProcess.kill('SIGKILL');
                }
            }, 5000);
        }
    }
}

// Initialize the application
const synapseApp = new SynapseElectronApp();

// Add command line arguments for better headless support on Linux
if (process.platform === 'linux') {
    app.commandLine.appendSwitch('disable-dev-shm-usage');
    app.commandLine.appendSwitch('disable-gpu');
    app.commandLine.appendSwitch('disable-software-rasterizer');
    app.commandLine.appendSwitch('disable-background-timer-throttling');
    app.commandLine.appendSwitch('disable-renderer-backgrounding');
    app.commandLine.appendSwitch('disable-features', 'VizDisplayCompositor');

    // Check if we're in a headless environment
    if (process.env.DISPLAY === ':99' || process.env.CI || process.env.HEADLESS) {
        app.commandLine.appendSwitch('headless');
        app.commandLine.appendSwitch('disable-gpu-sandbox');
        app.commandLine.appendSwitch('no-sandbox');
    }
}

// Add Windows-specific GPU debugging flags
if (process.platform === 'win32') {
    // Fix GPU issues on Windows by disabling GPU acceleration
    app.commandLine.appendSwitch('disable-gpu');
    app.commandLine.appendSwitch('disable-gpu-sandbox');
    app.commandLine.appendSwitch('disable-software-rasterizer');
    app.commandLine.appendSwitch('disable-gpu-compositing');
    app.commandLine.appendSwitch('disable-gpu-rasterization');
    app.commandLine.appendSwitch('disable-gpu-memory-buffer-compositor-resources');
    app.commandLine.appendSwitch('disable-gpu-memory-buffer-video-frames');
    app.commandLine.appendSwitch('enable-logging');
    app.commandLine.appendSwitch('log-level', '0');

    // Use software rendering and disable GPU context creation
    app.commandLine.appendSwitch('disable-features', 'VizDisplayCompositor');
    app.commandLine.appendSwitch('disable-d3d11');
    app.commandLine.appendSwitch('disable-d3d9');
    app.commandLine.appendSwitch('disable-webgl');
    app.commandLine.appendSwitch('disable-webgl2');
    app.commandLine.appendSwitch('disable-accelerated-2d-canvas');
    app.commandLine.appendSwitch('disable-accelerated-jpeg-decoding');
    app.commandLine.appendSwitch('disable-accelerated-mjpeg-decode');
    app.commandLine.appendSwitch('disable-accelerated-video-decode');

    log.info('Applied Windows GPU compatibility flags');
}

// Disable hardware acceleration completely
app.disableHardwareAcceleration();

// App event handlers
app.on('ready', () => {
    synapseApp.initialize();
});

app.on('window-all-closed', () => {
    synapseApp.cleanup();
    if (process.platform !== 'darwin') {
        app.quit();
    }
});

app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
        synapseApp.createWindow();
    }
});

app.on('before-quit', () => {
    synapseApp.cleanup();
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
    log.error('Uncaught Exception:', error);
});

process.on('unhandledRejection', (reason, promise) => {
    log.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
