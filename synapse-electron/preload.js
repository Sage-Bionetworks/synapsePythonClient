const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods that allow the renderer process to use
// the ipcRenderer without exposing the entire object
contextBridge.exposeInMainWorld('electronAPI', {
    // Authentication
    login: (credentials) => ipcRenderer.invoke('synapse-login', credentials),
    logout: () => ipcRenderer.invoke('synapse-logout'),
    getProfiles: () => ipcRenderer.invoke('synapse-get-profiles'),

    // File operations
    uploadFile: (uploadData) => ipcRenderer.invoke('synapse-upload', uploadData),
    downloadFile: (downloadData) => ipcRenderer.invoke('synapse-download', downloadData),

    // Bulk operations
    enumerate: (containerData) => ipcRenderer.invoke('synapse-enumerate', containerData),
    bulkDownload: (bulkData) => ipcRenderer.invoke('synapse-bulk-download', bulkData),
    bulkUpload: (bulkData) => ipcRenderer.invoke('synapse-bulk-upload', bulkData),
    scanDirectory: (scanData) => ipcRenderer.invoke('synapse-scan-directory', scanData),

    // File dialogs
    showOpenDialog: (options) => ipcRenderer.invoke('show-open-dialog', options),
    showSaveDialog: (options) => ipcRenderer.invoke('show-save-dialog', options),

    // External links
    openExternal: (url) => ipcRenderer.invoke('open-external', url),

    // App info
    getAppVersion: () => ipcRenderer.invoke('get-app-version'),
    getHomeDirectory: () => ipcRenderer.invoke('get-home-directory'),

    // WebSocket events
    onWebSocketMessage: (callback) => ipcRenderer.on('websocket-message', callback),
    removeWebSocketListener: (callback) => ipcRenderer.removeListener('websocket-message', callback),

    // Window events
    onWindowClose: (callback) => ipcRenderer.on('window-close', callback),
    removeWindowCloseListener: (callback) => ipcRenderer.removeListener('window-close', callback)
});
