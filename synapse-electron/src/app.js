// Synapse Desktop Client - Electron Frontend Application
class SynapseDesktopClient {
    constructor() {
        this.isLoggedIn = false;
        this.currentUser = null;
        this.activeTab = 'download';
        this.websocketConnected = false;
        this.containerItems = [];
        this.selectedItems = new Set();

        // Upload-specific properties
        this.uploadFileItems = [];
        this.selectedUploadFiles = new Set();

        // Auto-scroll related properties
        this.autoScrollEnabled = true;
        this.lastScrollTime = 0;
        this.scrollThrottleDelay = 50; // Minimum ms between scroll operations
        this.pendingScroll = false;

        this.initializeApp();
    }

    async initializeApp() {
        try {
            await this.setupEventListeners();
            await this.loadAppVersion();
            await this.loadProfiles();
            await this.setupWebSocketListener();

            // Initialize auto-scroll UI
            this.updateAutoScrollUI();
            this.updateLogCount();

            // Start polling for log messages
            this.startLogPolling();

            // Set default download location
            const homeResult = await window.electronAPI.getHomeDirectory();
            let defaultDownloadPath = 'Downloads'; // fallback

            if (homeResult.success && homeResult.data.downloads_directory) {
                defaultDownloadPath = homeResult.data.downloads_directory;
            } else {
                console.error('Error getting home directory:', homeResult.error);
            }

            const downloadLocationEl = document.getElementById('download-location');
            const bulkDownloadLocationEl = document.getElementById('bulk-download-location');

            if (downloadLocationEl) {
                downloadLocationEl.value = defaultDownloadPath;
            }
            if (bulkDownloadLocationEl) {
                bulkDownloadLocationEl.value = defaultDownloadPath;
            }

            // Initialize button states based on current form values
            this.updateUploadButtonState();
            this.updateDownloadButtonState();
            this.updateBulkUploadButtonState();
            this.updateBulkDownloadButtonState();

            console.log('Synapse Desktop Client initialized');
        } catch (error) {
            console.error('Error in initializeApp:', error);
        }
    }

    async loadAppVersion() {
        try {
            const version = await window.electronAPI.getAppVersion();
            document.getElementById('app-version').textContent = `v${version}`;
        } catch (error) {
            console.error('Error loading app version:', error);
        }
    }

    setupEventListeners() {
        // Cleanup on window close
        window.addEventListener('beforeunload', () => {
            this.stopLogPolling();
        });

        // Login mode toggle
        document.querySelectorAll('input[name="loginMode"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.toggleLoginMode(e.target.value);
            });
        });

        // Login button
        document.getElementById('login-btn').addEventListener('click', () => {
            this.handleLogin();
        });

        // Logout button
        document.getElementById('logout-btn').addEventListener('click', () => {
            this.handleLogout();
        });

        // Profile selection
        document.getElementById('profile').addEventListener('change', (e) => {
            this.updateProfileInfo(e.target.value);
        });

        // Tab navigation
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.switchTab(e.currentTarget.dataset.tab);
            });
        });

        // File browser buttons
        document.getElementById('browse-download-location').addEventListener('click', () => {
            this.browseDirectory('download-location');
        });

        document.getElementById('browse-upload-file').addEventListener('click', () => {
            this.browseFile('upload-file');
        });

        document.getElementById('browse-bulk-download-location').addEventListener('click', () => {
            this.browseDirectory('bulk-download-location');
        });

        document.getElementById('browse-bulk-upload-location').addEventListener('click', () => {
            this.browseDirectory('bulk-upload-location');
        });

        // Upload mode toggle
        document.querySelectorAll('input[name="uploadMode"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.toggleUploadMode(e.target.value);
            });
        });

        // Operation buttons
        document.getElementById('download-btn').addEventListener('click', () => {
            this.handleDownload();
        });

        document.getElementById('upload-btn').addEventListener('click', () => {
            this.handleUpload();
        });

        document.getElementById('enumerate-btn').addEventListener('click', () => {
            this.handleEnumerate();
        });

        document.getElementById('scan-upload-btn').addEventListener('click', () => {
            this.handleScanUploadDirectory();
        });

        document.getElementById('bulk-download-btn').addEventListener('click', () => {
            this.handleBulkDownload();
        });

        document.getElementById('bulk-upload-btn').addEventListener('click', () => {
            this.handleBulkUpload();
        });

        // Selection controls
        document.getElementById('select-all-btn').addEventListener('click', () => {
            this.selectAllItems(true);
        });

        document.getElementById('deselect-all-btn').addEventListener('click', () => {
            this.selectAllItems(false);
        });

        document.getElementById('expand-all-btn').addEventListener('click', () => {
            this.expandAllFolders();
        });

        document.getElementById('collapse-all-btn').addEventListener('click', () => {
            this.collapseAllFolders();
        });

        // Upload selection controls
        document.getElementById('select-all-upload-btn').addEventListener('click', () => {
            this.selectAllUploadFiles(true);
        });

        document.getElementById('deselect-all-upload-btn').addEventListener('click', () => {
            this.selectAllUploadFiles(false);
        });

        document.getElementById('expand-all-upload-btn').addEventListener('click', () => {
            this.expandAllUploadFolders();
        });

        document.getElementById('collapse-all-upload-btn').addEventListener('click', () => {
            this.collapseAllUploadFolders();
        });

        // Clear output
        document.getElementById('clear-output-btn').addEventListener('click', () => {
            this.clearOutput();
        });

        // Auto-scroll toggle
        document.getElementById('auto-scroll-toggle').addEventListener('click', () => {
            this.toggleAutoScroll();
        });

        // Scroll to bottom
        document.getElementById('scroll-to-bottom-btn').addEventListener('click', () => {
            this.scrollToBottom();
        });

        // Detect manual scrolling to temporarily disable auto-scroll
        document.getElementById('output-log').addEventListener('scroll', (e) => {
            this.handleManualScroll(e);
        });

        // File input change for auto-naming
        document.getElementById('upload-file').addEventListener('change', (e) => {
            if (e.target.value && !document.getElementById('upload-name').value) {
                const fileName = e.target.value.split('\\').pop().split('/').pop();
                document.getElementById('upload-name').value = fileName;
            }
            // Re-enable upload button if form is now valid
            this.updateUploadButtonState();
        });

        // Download input changes
        document.getElementById('download-id').addEventListener('input', () => {
            this.updateDownloadButtonState();
        });

        document.getElementById('download-location').addEventListener('change', () => {
            this.updateDownloadButtonState();
        });

        // Upload mode and ID input changes
        document.querySelectorAll('input[name="uploadMode"]').forEach(radio => {
            radio.addEventListener('change', () => {
                this.updateUploadButtonState();
            });
        });

        document.getElementById('parent-id').addEventListener('input', () => {
            this.updateUploadButtonState();
        });

        document.getElementById('entity-id').addEventListener('input', () => {
            this.updateUploadButtonState();
        });

        // Bulk upload directory change
        document.getElementById('bulk-upload-location').addEventListener('change', () => {
            this.handleScanUploadDirectory();
        });

        // Bulk operation form field changes
        document.getElementById('bulk-parent-id').addEventListener('input', () => {
            this.updateBulkUploadButtonState();
        });

        document.getElementById('bulk-download-location').addEventListener('change', () => {
            this.updateBulkDownloadButtonState();
        });
    }

    setupWebSocketListener() {
        window.electronAPI.onWebSocketMessage((event, message) => {
            this.handleWebSocketMessage(message);
        });
    }

    startLogPolling() {
        // Poll for log messages every 500ms
        this.logPollingInterval = setInterval(async () => {
            try {
                const response = await fetch('http://localhost:8000/logs/poll');
                if (response.ok) {
                    const result = await response.json();
                    if (result.success && result.messages && result.messages.length > 0) {
                        // Process each log message
                        result.messages.forEach(message => {
                            this.handleLogMessage(message);
                        });
                    }
                }
            } catch (error) {
                // Silently ignore polling errors to avoid spam
                // Only log if we're having connection issues
                if (error.message && !error.message.includes('Failed to fetch')) {
                    console.debug('Log polling error:', error);
                }
            }
        }, 500);
    }

    stopLogPolling() {
        if (this.logPollingInterval) {
            clearInterval(this.logPollingInterval);
            this.logPollingInterval = null;
        }
    }

    handleWebSocketMessage(message) {
        console.log('WebSocket message received:', message);
        switch (message.type) {
            case 'log':
                this.handleLogMessage(message);
                break;
            case 'complete':
                console.log('Handling completion message:', message);
                this.handleOperationComplete(message.operation, message.success, message.data);
                break;
            case 'connection_status':
                this.updateConnectionStatus(message.connected);
                break;
            case 'scroll_command':
                this.handleScrollCommand(message);
                break;
            default:
                console.log('Unknown WebSocket message:', message);
        }
    }

    handleLogMessage(logMessage) {
        // Display in the UI log with enhanced formatting
        this.logMessageAdvanced(logMessage);

        // Auto-scroll if enabled and requested
        if (logMessage.auto_scroll) {
            this.autoScrollIfEnabled();
        }

        // Also log to browser console with more details
        const timestamp = logMessage.timestamp ?
            new Date(logMessage.timestamp * 1000).toISOString() :
            new Date().toISOString();
        const consoleMessage = `[${timestamp}] [${logMessage.logger_name || 'backend'}] ${logMessage.message}`;

        switch (logMessage.level) {
            case 'error':
            case 'critical':
                console.error(consoleMessage);
                break;
            case 'warning':
            case 'warn':
                console.warn(consoleMessage);
                break;
            case 'debug':
                console.debug(consoleMessage);
                break;
            case 'info':
            default:
                console.info(consoleMessage);
                break;
        }

        // For synapse client logs, add special formatting
        if (logMessage.logger_name && logMessage.logger_name.includes('synapse')) {
            console.group(`🔬 Synapse Client Log [${logMessage.level.toUpperCase()}]`);
            console.log(`Logger: ${logMessage.logger_name}`);
            console.log(`Message: ${logMessage.message}`);
            console.log(`Timestamp: ${timestamp}`);
            if (logMessage.operation) {
                console.log(`Operation: ${logMessage.operation}`);
            }
            console.groupEnd();
        }
    }

    updateConnectionStatus(connected) {
        this.websocketConnected = connected;
        const statusElement = document.getElementById('connection-status');
        const icon = statusElement.querySelector('i');

        if (connected) {
            icon.className = 'fas fa-circle status-connected';
            statusElement.lastChild.textContent = 'Connected';
        } else {
            icon.className = 'fas fa-circle status-disconnected';
            statusElement.lastChild.textContent = 'Disconnected';
        }
    }

    async loadProfiles() {
        try {
            const result = await window.electronAPI.getProfiles();
            if (result.success) {
                this.populateProfiles(result.data.profiles);
                this.updateLoginModeAvailability(result.data.profiles.length > 0);
            }
        } catch (error) {
            console.error('Error loading profiles:', error);
            this.updateLoginModeAvailability(false);
        }
    }

    populateProfiles(profiles) {
        const profileSelect = document.getElementById('profile');
        profileSelect.innerHTML = '<option value="">Select a profile...</option>';

        profiles.forEach(profile => {
            const option = document.createElement('option');
            option.value = profile.name;
            option.textContent = profile.display_name || profile.name;
            profileSelect.appendChild(option);
        });
    }

    updateLoginModeAvailability(hasProfiles) {
        const configMode = document.getElementById('config-mode');
        const manualMode = document.getElementById('manual-mode');

        if (!hasProfiles) {
            configMode.disabled = true;
            manualMode.checked = true;
            this.toggleLoginMode('manual');
        } else {
            configMode.disabled = false;
        }
    }

    updateProfileInfo(profileName) {
        const infoElement = document.getElementById('profile-info');
        if (profileName) {
            infoElement.textContent = `Using profile: ${profileName}`;
        } else {
            infoElement.textContent = '';
        }
    }

    toggleLoginMode(mode) {
        const manualForm = document.getElementById('manual-login');
        const configForm = document.getElementById('config-login');

        if (mode === 'manual') {
            manualForm.classList.add('active');
            configForm.classList.remove('active');
        } else {
            manualForm.classList.remove('active');
            configForm.classList.add('active');
        }
    }

    toggleUploadMode(mode) {
        const parentIdGroup = document.getElementById('parent-id-group');
        const entityIdGroup = document.getElementById('entity-id-group');

        if (mode === 'new') {
            parentIdGroup.style.display = 'block';
            entityIdGroup.style.display = 'none';
            document.getElementById('entity-id').value = '';
        } else {
            parentIdGroup.style.display = 'none';
            entityIdGroup.style.display = 'block';
            document.getElementById('parent-id').value = '';
        }
    }

    switchTab(tabName) {
        if (!tabName) {
            console.error('switchTab called with invalid tabName:', tabName);
            return;
        }

        // Update tab buttons
        document.querySelectorAll('.tab-btn').forEach(btn => {
            btn.classList.remove('active');
        });

        const activeTabBtn = document.querySelector(`[data-tab="${tabName}"]`);
        if (activeTabBtn) {
            activeTabBtn.classList.add('active');
        } else {
            console.error(`No tab button found with data-tab="${tabName}"`);
        }

        // Update tab panels
        document.querySelectorAll('.tab-panel').forEach(panel => {
            panel.classList.remove('active');
        });

        const activeTabPanel = document.getElementById(`${tabName}-tab`);
        if (activeTabPanel) {
            activeTabPanel.classList.add('active');
        } else {
            console.error(`No tab panel found with id="${tabName}-tab"`);
        }

        this.activeTab = tabName;
    }

    async handleLogin() {
        const loginBtn = document.getElementById('login-btn');
        const statusDiv = document.getElementById('login-status');

        loginBtn.disabled = true;
        loginBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Logging in...';

        try {
            const mode = document.querySelector('input[name="loginMode"]:checked').value;
            let credentials = { mode };

            if (mode === 'manual') {
                credentials.username = document.getElementById('username').value.trim();
                credentials.token = document.getElementById('token').value.trim();

                if (!credentials.username || !credentials.token) {
                    throw new Error('Username and token are required');
                }
            } else {
                credentials.profile = document.getElementById('profile').value;

                if (!credentials.profile) {
                    throw new Error('Please select a profile');
                }
            }

            const result = await window.electronAPI.login(credentials);

            if (result.success) {
                this.handleLoginSuccess(result.data);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.showStatus('login-status', error.message, 'error');
        } finally {
            loginBtn.disabled = false;
            loginBtn.innerHTML = '<i class="fas fa-sign-in-alt"></i> Login';
        }
    }

    handleLoginSuccess(userData) {
        this.isLoggedIn = true;
        this.currentUser = userData;

        // Hide login section and show main app
        document.getElementById('login-section').style.display = 'none';
        document.getElementById('main-section').style.display = 'flex';

        // Update user info
        document.getElementById('user-name').textContent = userData.username || userData.name || 'User';

        this.logMessage(`Successfully logged in as ${userData.username || userData.name}`, false);
        this.setStatus('Logged in successfully');
    }

    async handleLogout() {
        try {
            await window.electronAPI.logout();

            this.isLoggedIn = false;
            this.currentUser = null;

            // Show login section and hide main app
            document.getElementById('login-section').style.display = 'flex';
            document.getElementById('main-section').style.display = 'none';

            // Clear form data
            document.getElementById('username').value = '';
            document.getElementById('token').value = '';
            document.getElementById('profile').value = '';

            // Clear status
            document.getElementById('login-status').classList.remove('success', 'error', 'info');
            document.getElementById('login-status').style.display = 'none';

            this.logMessage('Logged out successfully', false);
            this.setStatus('Ready');
        } catch (error) {
            console.error('Logout error:', error);
            this.logMessage(`Logout error: ${error.message}`, true);
        }
    }

    async browseDirectory(inputId) {
        try {
            const result = await window.electronAPI.showOpenDialog({
                properties: ['openDirectory'],
                defaultPath: document.getElementById(inputId).value || ''
            });

            if (!result.canceled && result.filePaths.length > 0) {
                document.getElementById(inputId).value = result.filePaths[0];

                // Trigger scan for bulk upload if it's the upload location
                if (inputId === 'bulk-upload-location') {
                    this.handleScanUploadDirectory();
                }
            }
        } catch (error) {
            console.error('Error browsing directory:', error);
            this.logMessage(`Error selecting directory: ${error.message}`, true);
        }
    }

    async browseFile(inputId) {
        try {
            const result = await window.electronAPI.showOpenDialog({
                properties: ['openFile'],
                defaultPath: document.getElementById(inputId).value || ''
            });

            if (!result.canceled && result.filePaths.length > 0) {
                document.getElementById(inputId).value = result.filePaths[0];

                // Auto-populate upload name if empty
                if (inputId === 'upload-file' && !document.getElementById('upload-name').value) {
                    const fileName = result.filePaths[0].split('\\').pop().split('/').pop();
                    document.getElementById('upload-name').value = fileName;
                }
            }
        } catch (error) {
            console.error('Error browsing file:', error);
            this.logMessage(`Error selecting file: ${error.message}`, true);
        }
    }

    async handleDownload() {
        const synapseId = document.getElementById('download-id').value.trim();
        const version = document.getElementById('download-version').value.trim();
        const downloadPath = document.getElementById('download-location').value.trim();

        if (!synapseId) {
            this.logMessage('Synapse ID is required for download', true);
            return;
        }

        if (!downloadPath) {
            this.logMessage('Download location is required', true);
            return;
        }

        try {
            const result = await window.electronAPI.downloadFile({
                synapse_id: synapseId,
                version: version || null,
                download_path: downloadPath
            });

            if (result.success) {
                this.logMessage(`Download initiated for ${synapseId}`, false);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.logMessage(`Download error: ${error.message}`, true);}
    }

    async handleUpload() {
        const filePath = document.getElementById('upload-file').value.trim();
        const mode = document.querySelector('input[name="uploadMode"]:checked').value;
        const parentId = document.getElementById('parent-id').value.trim();
        const entityId = document.getElementById('entity-id').value.trim();
        const name = document.getElementById('upload-name').value.trim();

        if (!filePath) {
            this.logMessage('File path is required for upload', true);
            return;
        }

        if (mode === 'new' && !parentId) {
            this.logMessage('Parent ID is required for new file upload', true);
            return;
        }

        if (mode === 'update' && !entityId) {
            this.logMessage('Entity ID is required for file update', true);
            return;
        }

        try {
            const result = await window.electronAPI.uploadFile({
                file_path: filePath,
                mode: mode,
                parent_id: parentId || null,
                entity_id: entityId || null,
                name: name || null
            });

            if (result.success) {
                this.logMessage(`Upload initiated for ${filePath}`, false);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.logMessage(`Upload error: ${error.message}`, true);}
    }

    async handleEnumerate() {
        const containerId = document.getElementById('container-id').value.trim();
        const recursive = document.getElementById('recursive').checked;

        if (!containerId) {
            this.logMessage('Container ID is required for enumeration', true);
            return;
        }

        try {
            const result = await window.electronAPI.enumerate({
                container_id: containerId,
                recursive: recursive
            });

            if (result.success) {
                this.displayContainerContents(result.data.items);
                this.logMessage(`Found ${result.data.items.length} items in container`, false);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.logMessage(`Enumeration error: ${error.message}`, true);
        } finally {}
    }

    displayContainerContents(items) {
        this.containerItems = items;
        this.selectedItems.clear();

        const contentsDiv = document.getElementById('container-contents');
        const treeDiv = document.getElementById('items-tree');

        treeDiv.innerHTML = '';

        // Build hierarchical structure
        const { rootItems, parentMap } = this.buildHierarchy(items);

        // Use JSTree for better tree functionality
        this.initializeJSTree(treeDiv, rootItems, parentMap);

        contentsDiv.style.display = 'block';

        // Update status
        const folderCount = items.filter(item => {
            const itemType = item.type || item.item_type || 'file';
            return itemType === 'folder';
        }).length;
        const fileCount = items.length - folderCount;
        this.logMessage(`Loaded ${items.length} items (${folderCount} folders, ${fileCount} files)`, false);
    }

    initializeJSTree(container, rootItems, parentMap) {
        // Clear any existing JSTree
        if ($.fn.jstree && $(container).jstree) {
            $(container).jstree('destroy');
        }

        // Build JSTree data structure
        const treeData = this.buildJSTreeData(rootItems, parentMap);

        // Initialize JSTree with enhanced features
        $(container).jstree({
            'core': {
                'data': treeData,
                'themes': {
                    'name': 'default',
                    'responsive': true,
                    'icons': true
                },
                'check_callback': true
            },
            'checkbox': {
                'three_state': true,      // Enable three-state checkboxes (checked, unchecked, indeterminate)
                'whole_node': true,       // Click anywhere on the node to check/uncheck
                'tie_selection': false,   // Don't tie selection to checkbox state
                'cascade': 'up+down'      // Cascade checkbox state up and down the tree
            },
            'types': {
                'folder': {
                    'icon': 'fas fa-folder',
                    'a_attr': { 'class': 'tree-folder' }
                },
                'file': {
                    'icon': 'fas fa-file',
                    'a_attr': { 'class': 'tree-file' }
                }
            },
            'plugins': ['checkbox', 'types', 'wholerow']
        })
        .on('check_node.jstree uncheck_node.jstree', (e, data) => {
            this.handleJSTreeSelection(data);
        })
        .on('ready.jstree', () => {
            // Expand first level by default
            $(container).jstree('open_all', null, 1);
            this.updateSelectionCount();
        });
    }

    buildJSTreeData(items, parentMap, parentPath = '') {
        const treeData = [];

        items.forEach(item => {
            const hasChildren = parentMap[item.id] && parentMap[item.id].length > 0;
            const currentPath = parentPath ? `${parentPath}/${item.name}` : item.name;
            const sizeStr = this.formatFileSize(item.size);
            const itemType = item.type || item.item_type || 'file';

            const nodeData = {
                'id': item.id,
                'text': `${item.name} <span class="tree-item-details">(${itemType}${sizeStr ? ', ' + sizeStr : ''})</span>`,
                'type': itemType,
                'data': {
                    'item': item,
                    'path': currentPath,
                    'size': item.size
                },
                'state': {
                    'opened': false
                }
            };

            if (hasChildren) {
                nodeData.children = this.buildJSTreeData(parentMap[item.id], parentMap, currentPath);
            }

            treeData.push(nodeData);
        });

        return treeData;
    }

    handleJSTreeSelection(data) {
        console.log('handleJSTreeSelection called');
        const treeDiv = document.getElementById('items-tree');

        // Get all checked nodes to rebuild the selection set
        if ($.fn.jstree && $(treeDiv).jstree) {
            const checkedNodes = $(treeDiv).jstree('get_checked', true);
            this.selectedItems.clear();

            checkedNodes.forEach(node => {
                if (node.data && node.data.item) {
                    this.selectedItems.add(node.data.item.id);
                }
            });

            console.log(`handleJSTreeSelection: ${this.selectedItems.size} items selected`);
        }

        this.updateSelectionCount();
    }

    buildHierarchy(items) {
        const parentMap = {};
        const rootItems = [];
        const allItemIds = new Set(items.map(item => item.id));

        // Build parent-child relationships
        items.forEach(item => {
            if (item.parent_id) {
                if (!parentMap[item.parent_id]) {
                    parentMap[item.parent_id] = [];
                }
                parentMap[item.parent_id].push(item);
            } else {
                rootItems.push(item);
            }
        });

        // Handle orphaned items (parent not in current item set)
        if (rootItems.length === 0 && items.length > 0) {
            items.forEach(item => {
                if (item.parent_id && !allItemIds.has(item.parent_id)) {
                    rootItems.push(item);
                }
            });
        }

        // Sort items: folders first, then alphabetically
        rootItems.sort((a, b) => {
            const aType = a.type || a.item_type || 'file';
            const bType = b.type || b.item_type || 'file';
            if (aType !== bType) {
                return aType === 'folder' ? -1 : 1;
            }
            return a.name.localeCompare(b.name);
        });

        Object.values(parentMap).forEach(children => {
            children.sort((a, b) => {
                const aType = a.type || a.item_type || 'file';
                const bType = b.type || b.item_type || 'file';
                if (aType !== bType) {
                    return aType === 'folder' ? -1 : 1;
                }
                return a.name.localeCompare(b.name);
            });
        });

        return { rootItems, parentMap };
    }

    populateTreeLevel(container, items, parentMap, level, parentPath = '') {
        items.forEach(item => {
            const hasChildren = parentMap[item.id] && parentMap[item.id].length > 0;
            const currentPath = parentPath ? `${parentPath}/${item.name}` : item.name;

            // Create tree item element
            const itemDiv = this.createTreeItem(item, level, hasChildren, currentPath);
            container.appendChild(itemDiv);

            // Create children container
            if (hasChildren) {
                const childrenDiv = document.createElement('div');
                childrenDiv.className = 'tree-children';
                childrenDiv.id = `children-${item.id}`;

                this.populateTreeLevel(
                    childrenDiv,
                    parentMap[item.id],
                    parentMap,
                    level + 1,
                    currentPath
                );

                container.appendChild(childrenDiv);
            }
        });
    }

    createTreeItem(item, level, hasChildren, currentPath) {
        const itemDiv = document.createElement('div');
        itemDiv.className = `tree-item level-${level}`;
        itemDiv.dataset.itemId = item.id;
        itemDiv.dataset.level = level;
        itemDiv.style.setProperty('--indent-level', level);

        // Format size for display
        const sizeStr = this.formatFileSize(item.size);
        const itemType = item.type || item.item_type || 'file';

        itemDiv.innerHTML = `
            <button class="tree-toggle ${hasChildren ? 'expanded' : 'leaf'}"
                    ${hasChildren ? `onclick="window.synapseApp.toggleTreeNode('${item.id}')"` : ''}>
            </button>
            <div class="tree-item-content">
                <input type="checkbox" id="item-${item.id}" data-item-id="${item.id}">
                <i class="fas ${itemType === 'file' ? 'fa-file' : 'fa-folder'} tree-item-icon ${itemType}"></i>
                <span class="tree-item-name">${this.escapeHtml(item.name)}</span>
                <span class="tree-item-type">${itemType}</span>
                <span class="tree-item-size">${sizeStr}</span>
                <span class="tree-item-path" title="${currentPath}">${currentPath}</span>
            </div>
        `;

        // Add checkbox event listener
        const checkbox = itemDiv.querySelector('input[type="checkbox"]');
        checkbox.addEventListener('change', (e) => {
            this.handleItemSelection(e, item, itemDiv);
        });

        // Add context menu support
        itemDiv.addEventListener('contextmenu', (e) => {
            e.preventDefault();
            this.showTreeItemContextMenu(e, item, itemDiv);
        });

        return itemDiv;
    }

    toggleTreeNode(itemId) {
        const toggleButton = document.querySelector(`.tree-item[data-item-id="${itemId}"] .tree-toggle`);
        const childrenDiv = document.getElementById(`children-${itemId}`);

        if (toggleButton && childrenDiv) {
            const isExpanded = toggleButton.classList.contains('expanded');

            if (isExpanded) {
                toggleButton.classList.remove('expanded');
                toggleButton.classList.add('collapsed');
                childrenDiv.style.display = 'none';
            } else {
                toggleButton.classList.remove('collapsed');
                toggleButton.classList.add('expanded');
                childrenDiv.style.display = 'block';
            }
        }
    }

    handleItemSelection(event, item, itemDiv) {
        if (event.target.checked) {
            this.selectedItems.add(item.id);
            itemDiv.classList.add('selected');
        } else {
            this.selectedItems.delete(item.id);
            itemDiv.classList.remove('selected');
        }

        // Update selection count display
        this.updateSelectionCount();
    }

    updateSelectionCount() {
        const count = this.selectedItems.size;
        console.log(`updateSelectionCount called: ${count} items selected`);

        // Calculate stats for selected items
        const selectedItemsData = this.containerItems.filter(item =>
            this.selectedItems.has(item.id)
        );

        const fileCount = selectedItemsData.filter(item => {
            const itemType = item.type || item.item_type || 'file';
            return itemType === 'file';
        }).length;

        const folderCount = selectedItemsData.filter(item => {
            const itemType = item.type || item.item_type || 'file';
            return itemType === 'folder';
        }).length;

        const totalSize = selectedItemsData.reduce((sum, item) => {
            return sum + (item.size || 0);
        }, 0);

        // Update selection info
        let selectionInfo = document.querySelector('.selection-info');
        if (!selectionInfo) {
            // Create selection info if it doesn't exist
            selectionInfo = document.createElement('div');
            selectionInfo.className = 'selection-info';
            selectionInfo.style.cssText = 'margin: 0.5rem 0; font-size: 0.875rem; color: var(--text-muted);';

            const controlsDiv = document.querySelector('.selection-controls');
            if (controlsDiv) {
                controlsDiv.parentNode.insertBefore(selectionInfo, controlsDiv.nextSibling);
            }
        }

        // Format the selection information
        if (count === 0) {
            selectionInfo.textContent = 'No items selected';
        } else {
            const sizeStr = this.formatFileSize(totalSize);
            const parts = [];

            if (fileCount > 0) {
                parts.push(`${fileCount} file${fileCount !== 1 ? 's' : ''}`);
            }
            if (folderCount > 0) {
                parts.push(`${folderCount} folder${folderCount !== 1 ? 's' : ''}`);
            }

            let infoText = `${count} item${count !== 1 ? 's' : ''} selected`;
            if (parts.length > 0) {
                infoText += ` (${parts.join(', ')})`;
            }
            if (sizeStr) {
                infoText += ` - Total size: ${sizeStr}`;
            }

            selectionInfo.textContent = infoText;
        }

        // Update download button text and state
        const downloadBtn = document.getElementById('bulk-download-btn');
        if (downloadBtn) {
            console.log(`Updating download button: count=${count}, disabled=${count === 0}`);
            if (count === 0) {
                downloadBtn.innerHTML = '<i class="fas fa-cloud-download-alt"></i> Download Selected Items';
            } else {
                const sizeStr = this.formatFileSize(totalSize);
                const btnText = sizeStr
                    ? `Download ${count} item${count !== 1 ? 's' : ''} (${sizeStr})`
                    : `Download ${count} item${count !== 1 ? 's' : ''}`;
                downloadBtn.innerHTML = `<i class="fas fa-cloud-download-alt"></i> ${btnText}`;
            }
            // Use the validation function to determine button state
            this.updateBulkDownloadButtonState();
        } else {
            console.error('Download button not found!');
        }
    }

    formatFileSize(bytes) {
        if (!bytes || bytes === 0) return '';

        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let size = bytes;
        let unitIndex = 0;

        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }

        return `${size.toFixed(1)} ${units[unitIndex]}`;
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Make tree items focusable for better accessibility
    makeTreeItemsFocusable() {
        const treeItems = document.querySelectorAll('.tree-item');
        treeItems.forEach((item, index) => {
            item.setAttribute('tabindex', index === 0 ? '0' : '-1');
            item.addEventListener('focus', () => {
                // Remove focus from other tree items
                treeItems.forEach(otherItem => {
                    if (otherItem !== item) {
                        otherItem.setAttribute('tabindex', '-1');
                    }
                });
                item.setAttribute('tabindex', '0');
            });
        });
    }

    showTreeItemContextMenu(event, item, itemDiv) {
        // Remove any existing context menu
        const existingMenu = document.querySelector('.tree-context-menu');
        if (existingMenu) {
            existingMenu.remove();
        }

        // Create context menu
        const contextMenu = document.createElement('div');
        contextMenu.className = 'tree-context-menu';
        contextMenu.style.cssText = `
            position: fixed;
            left: ${event.clientX}px;
            top: ${event.clientY}px;
            background: white;
            border: 1px solid var(--border-color);
            border-radius: var(--border-radius-sm);
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            z-index: 1000;
            min-width: 150px;
        `;

        const checkbox = itemDiv.querySelector('input[type="checkbox"]');
        const isSelected = checkbox.checked;

        const menuItems = [
            {
                label: isSelected ? 'Deselect Item' : 'Select Item',
                action: () => {
                    checkbox.checked = !checkbox.checked;
                    checkbox.dispatchEvent(new Event('change'));
                }
            }
        ];

        // Add folder-specific options
        const itemType = item.type || item.item_type || 'file';
        if (itemType === 'folder') {
            const toggleButton = itemDiv.querySelector('.tree-toggle');
            const isExpanded = toggleButton && toggleButton.classList.contains('expanded');

            menuItems.push({
                label: isExpanded ? 'Collapse Folder' : 'Expand Folder',
                action: () => {
                    if (toggleButton && !toggleButton.classList.contains('leaf')) {
                        toggleButton.click();
                    }
                }
            });
        }

        menuItems.push(
            { separator: true },
            {
                label: 'Show Info',
                action: () => {
                    this.showItemInfo(item);
                }
            }
        );

        // Build menu HTML
        const menuHTML = menuItems.map(menuItem => {
            if (menuItem.separator) {
                return '<div class="context-menu-separator"></div>';
            }
            return `<div class="context-menu-item" data-action="${menuItems.indexOf(menuItem)}">${menuItem.label}</div>`;
        }).join('');

        contextMenu.innerHTML = menuHTML;

        // Add event listeners
        contextMenu.addEventListener('click', (e) => {
            const actionIndex = e.target.dataset.action;
            if (actionIndex !== undefined) {
                menuItems[actionIndex].action();
            }
            contextMenu.remove();
        });

        // Close menu when clicking outside
        const closeMenu = (e) => {
            if (!contextMenu.contains(e.target)) {
                contextMenu.remove();
                document.removeEventListener('click', closeMenu);
            }
        };

        document.addEventListener('click', closeMenu);
        document.body.appendChild(contextMenu);
    }

    showItemInfo(item) {
        const itemType = item.type || item.item_type || 'file';
        const infoText = `
Item Information:

Name: ${item.name}
Type: ${itemType}
Synapse ID: ${item.id}
Size: ${item.size ? this.formatFileSize(item.size) : 'N/A'}
Parent ID: ${item.parent_id || 'N/A'}
        `.trim();

        alert(infoText);
    }

    selectAllItems(select) {
        const treeDiv = document.getElementById('items-tree');

        if ($.fn.jstree && $(treeDiv).jstree) {
            if (select) {
                $(treeDiv).jstree('check_all');
            } else {
                $(treeDiv).jstree('uncheck_all');
            }

            // Manually update the selection after JSTree operations
            // Use a short timeout to ensure JSTree has completed its operations
            setTimeout(() => {
                const checkedNodes = $(treeDiv).jstree('get_checked', true);
                this.selectedItems.clear();

                checkedNodes.forEach(node => {
                    if (node.data && node.data.item) {
                        this.selectedItems.add(node.data.item.id);
                    }
                });

                console.log(`Select all ${select ? 'enabled' : 'disabled'}: ${this.selectedItems.size} items selected`);
                this.updateSelectionCount();
            }, 10); // Slightly longer timeout to ensure completion
        } else {
            // Fallback to original method if JSTree is not available
            const checkboxes = document.querySelectorAll('#items-tree input[type="checkbox"]');
            this.selectedItems.clear();

            checkboxes.forEach(checkbox => {
                checkbox.checked = select;
                const itemDiv = checkbox.closest('.tree-item');

                if (select) {
                    this.selectedItems.add(checkbox.dataset.itemId);
                    itemDiv.classList.add('selected');
                } else {
                    itemDiv.classList.remove('selected');
                }
            });

            console.log(`Select all ${select ? 'enabled' : 'disabled'} (fallback): ${this.selectedItems.size} items selected`);
            this.updateSelectionCount();
        }
    }

    // Add convenience methods for expand/collapse all
    expandAllFolders() {
        const treeDiv = document.getElementById('items-tree');

        if ($.fn.jstree && $(treeDiv).jstree) {
            $(treeDiv).jstree('open_all');
        } else {
            // Fallback to original method
            const toggleButtons = document.querySelectorAll('.tree-toggle.collapsed');
            toggleButtons.forEach(button => {
                button.click();
            });
        }
    }

    collapseAllFolders() {
        const treeDiv = document.getElementById('items-tree');

        if ($.fn.jstree && $(treeDiv).jstree) {
            $(treeDiv).jstree('close_all');
        } else {
            // Fallback to original method
            const toggleButtons = document.querySelectorAll('.tree-toggle.expanded');
            toggleButtons.forEach(button => {
                button.click();
            });
        }
    }

    async handleBulkDownload() {
        const downloadPath = document.getElementById('bulk-download-location').value.trim();

        if (this.selectedItems.size === 0) {
            this.logMessage('Please select items to download', true);
            return;
        }

        if (!downloadPath) {
            this.logMessage('Download location is required', true);
            return;
        }

        try {
            const selectedItemsData = this.containerItems.filter(item =>
                this.selectedItems.has(item.id)
            );

            const result = await window.electronAPI.bulkDownload({
                items: selectedItemsData,
                download_path: downloadPath,
                create_subfolders: document.getElementById('recursive').checked
            });

            if (result.success) {
                this.logMessage(`Bulk download initiated for ${selectedItemsData.length} items`, false);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.logMessage(`Bulk download error: ${error.message}`, true);}
    }

    async handleScanUploadDirectory() {
        const uploadPath = document.getElementById('bulk-upload-location').value.trim();

        if (!uploadPath) {
            this.logMessage('Please select a directory to scan', true);
            return;
        }

        try {
            this.logMessage('Scanning directory for files...', false);

            const recursive = document.getElementById('bulk-preserve-structure').checked;

            // Call backend API to scan directory
            const result = await window.electronAPI.scanDirectory({
                directory_path: uploadPath,
                recursive: recursive
            });

            if (result.success) {
                this.displayUploadFileContents(result.data.files);
                const summary = result.data.summary;
                this.logMessage(
                    `Found ${summary.file_count} files and ${summary.folder_count} folders ` +
                    `(${this.formatFileSize(summary.total_size)} total)`, false
                );
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.logMessage(`Directory scan error: ${error.message}`, true);
        }
    }

    displayUploadFileContents(files) {
        this.uploadFileItems = files;
        this.selectedUploadFiles.clear();

        const contentsDiv = document.getElementById('bulk-upload-files');
        const treeDiv = document.getElementById('upload-files-tree');

        treeDiv.innerHTML = '';

        // Build hierarchical structure for files
        const { rootItems, parentMap } = this.buildUploadHierarchy(files);

        // Create tree elements
        this.createUploadTreeElements(rootItems, treeDiv, parentMap);

        contentsDiv.style.display = 'block';
        this.updateUploadSelectionDisplay();
    }

    buildUploadHierarchy(files) {
        const itemsMap = new Map();
        const parentMap = new Map();
        const rootItems = [];

        // Separate files and folders
        const actualFiles = files.filter(item => item.type === 'file');
        const actualFolders = files.filter(item => item.type === 'folder');

        // First, add all actual folders to the maps
        actualFolders.forEach(folder => {
            const normalizedPath = folder.relative_path.replace(/\\/g, '/');
            if (!itemsMap.has(normalizedPath)) {
                const folderObj = {
                    ...folder,
                    id: `folder_${folder.path}`,
                    name: folder.name,
                    type: 'folder',
                    path: normalizedPath,
                    isVirtual: false
                };
                itemsMap.set(normalizedPath, folderObj);
            }
        });

        // Create virtual folders for any missing parent directories of files
        actualFiles.forEach(file => {
            // Handle both Windows (\) and Unix (/) path separators
            const pathParts = file.relative_path.replace(/\\/g, '/').split('/').filter(part => part.length > 0);

            // Create all parent directories as virtual folders (only if they don't already exist as actual folders)
            let currentPath = '';
            for (let i = 0; i < pathParts.length - 1; i++) {
                const part = pathParts[i];
                currentPath = currentPath ? `${currentPath}/${part}` : part;

                if (!itemsMap.has(currentPath)) {
                    const virtualFolder = {
                        id: `folder_${currentPath}`,
                        name: part,
                        type: 'folder',
                        path: currentPath,
                        isVirtual: true
                    };
                    itemsMap.set(currentPath, virtualFolder);
                }
            }
        });

        // Now build the hierarchy relationships
        itemsMap.forEach((item, path) => {
            const pathParts = path.split('/');
            if (pathParts.length === 1) {
                // Root level item
                rootItems.push(item);
            } else {
                // Find parent
                const parentPath = pathParts.slice(0, -1).join('/');
                if (!parentMap.has(parentPath)) {
                    parentMap.set(parentPath, []);
                }
                parentMap.get(parentPath).push(item);
            }
        });

        // Add files to their parent directories
        actualFiles.forEach(file => {
            const pathParts = file.relative_path.replace(/\\/g, '/').split('/').filter(part => part.length > 0);
            const fileName = pathParts[pathParts.length - 1];
            const fileObj = {
                ...file,
                id: `file_${file.path}`,
                name: fileName,
                type: 'file'
            };

            const parentPath = pathParts.length > 1 ? pathParts.slice(0, -1).join('/') : '';

            if (parentPath) {
                if (!parentMap.has(parentPath)) {
                    parentMap.set(parentPath, []);
                }
                parentMap.get(parentPath).push(fileObj);
            } else {
                rootItems.push(fileObj);
            }
        });

        return { rootItems, parentMap };
    }

    createUploadTreeElements(items, container, parentMap, level = 0) {
        items.forEach(item => {
            const itemElement = this.createUploadTreeItem(item, level);
            container.appendChild(itemElement);

            // Add children if this is a folder and it has children
            if (item.type === 'folder') {
                const children = parentMap.get(item.path) || [];
                const childrenContainer = document.createElement('div');
                childrenContainer.className = 'tree-children';
                // Create a safe ID by replacing problematic characters
                const safeId = item.id.replace(/[^a-zA-Z0-9_-]/g, '_');
                childrenContainer.id = `upload-children-${safeId}`;
                childrenContainer.style.display = 'block'; // Start expanded like download tree

                if (children.length > 0) {
                    this.createUploadTreeElements(
                        children,
                        childrenContainer,
                        parentMap,
                        level + 1
                    );
                } else {
                    // Handle empty folders - add a placeholder or just leave empty
                    // The folder will still be expandable but show as empty
                    const emptyMessage = document.createElement('div');
                    emptyMessage.className = 'tree-item empty-folder level-' + (level + 1);
                    emptyMessage.innerHTML = `
                        <div class="tree-item-content upload-compact" style="opacity: 0.6; font-style: italic;">
                            <span style="margin-left: 20px;">Empty folder</span>
                        </div>
                    `;
                    childrenContainer.appendChild(emptyMessage);
                }

                // Update the toggle button class based on whether folder has children
                const toggleButton = itemElement.querySelector('.tree-toggle');
                if (toggleButton) {
                    if (children.length === 0) {
                        toggleButton.classList.remove('expanded');
                        toggleButton.classList.add('leaf');
                    }
                }

                container.appendChild(childrenContainer);
            }
        });
    }

    createUploadTreeItem(item, level) {
        const itemDiv = document.createElement('div');
        itemDiv.className = `tree-item level-${level} upload-tree-item ${item.type === 'folder' ? 'folder' : 'file'}`;
        itemDiv.dataset.id = item.id;
        itemDiv.dataset.type = item.type;
        itemDiv.dataset.level = level;

        let content = '';

        if (item.type === 'folder') {
            // For folders: "Folder Name (folder)" - we'll add count later if needed
            content = `
                <button class="tree-toggle expanded" data-folder-id="${this.escapeHtml(item.id)}">
                </button>
                <div class="tree-item-content upload-compact">
                    <i class="fas fa-folder tree-item-icon folder"></i>
                    <span class="upload-item-info">${this.escapeHtml(item.name)} <span class="item-meta">(folder)</span></span>
                </div>
            `;
        } else {
            // For files: "File Name (file, size)"
            const sizeStr = this.formatFileSize(item.size);
            content = `
                <button class="tree-toggle leaf">
                </button>
                <div class="tree-item-content upload-compact">
                    <input type="checkbox" class="item-checkbox" data-id="${this.escapeHtml(item.id)}">
                    <i class="fas fa-file tree-item-icon file"></i>
                    <span class="upload-item-info">${this.escapeHtml(item.name)} <span class="item-meta">(${sizeStr})</span></span>
                </div>
            `;
        }

        itemDiv.innerHTML = content;

        // Add event listeners instead of inline onclick handlers
        if (item.type === 'folder') {
            const toggleButton = itemDiv.querySelector('.tree-toggle');
            if (toggleButton) {
                toggleButton.addEventListener('click', () => {
                    this.toggleUploadFolder(item.id);
                });
            }
        } else {
            // Add event listener for file checkbox
            itemDiv.addEventListener('change', (e) => {
                if (e.target.classList.contains('item-checkbox')) {
                    this.handleUploadFileSelection(item.id, e.target.checked);
                }
            });
        }

        return itemDiv;
    }

    handleUploadFileSelection(fileId, selected) {
        if (selected) {
            this.selectedUploadFiles.add(fileId);
        } else {
            this.selectedUploadFiles.delete(fileId);
        }
        this.updateUploadSelectionDisplay();
    }

    updateUploadSelectionDisplay() {
        const selectedCount = this.selectedUploadFiles.size;
        const totalFiles = this.uploadFileItems.filter(item => item.type === 'file').length;

        const button = document.getElementById('bulk-upload-btn');
        if (button) {
            button.textContent = `Upload Selected Files (${selectedCount})`;
            // Use the validation function instead of just checking count
            this.updateBulkUploadButtonState();
        }
    }

    selectAllUploadFiles(select) {
        const checkboxes = document.querySelectorAll('#upload-files-tree .item-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.checked = select;
            const fileId = checkbox.dataset.id;
            if (select) {
                this.selectedUploadFiles.add(fileId);
            } else {
                this.selectedUploadFiles.delete(fileId);
            }
        });
        this.updateUploadSelectionDisplay();
    }

    toggleUploadFolder(itemId) {
        // Find the tree item and toggle button using the dataset id
        const treeItem = document.querySelector(`#upload-files-tree .tree-item[data-id="${CSS.escape(itemId)}"]`);
        if (!treeItem) {
            console.warn('Could not find tree item for ID:', itemId);
            return;
        }

        const toggleButton = treeItem.querySelector('.tree-toggle');
        // Create the same safe ID used when creating the container
        const safeId = itemId.replace(/[^a-zA-Z0-9_-]/g, '_');
        const childrenDiv = document.getElementById(`upload-children-${safeId}`);

        if (toggleButton && childrenDiv) {
            const isExpanded = toggleButton.classList.contains('expanded');

            if (isExpanded) {
                toggleButton.classList.remove('expanded');
                toggleButton.classList.add('collapsed');
                childrenDiv.style.display = 'none';
            } else {
                toggleButton.classList.remove('collapsed');
                toggleButton.classList.add('expanded');
                childrenDiv.style.display = 'block';
            }
        } else {
            console.warn('Could not find toggle button or children container for ID:', itemId, 'Safe ID:', safeId);
        }
    }

    expandAllUploadFolders() {
        const toggles = document.querySelectorAll('#upload-files-tree .tree-toggle');
        toggles.forEach(toggle => {
            if (!toggle.classList.contains('leaf')) {
                const treeItem = toggle.closest('.tree-item');
                const itemId = treeItem.dataset.id;
                const safeId = itemId.replace(/[^a-zA-Z0-9_-]/g, '_');
                const childrenContainer = document.getElementById(`upload-children-${safeId}`);
                if (childrenContainer) {
                    childrenContainer.style.display = 'block';
                    toggle.classList.remove('collapsed');
                    toggle.classList.add('expanded');
                }
            }
        });
    }

    collapseAllUploadFolders() {
        const toggles = document.querySelectorAll('#upload-files-tree .tree-toggle');
        toggles.forEach(toggle => {
            if (!toggle.classList.contains('leaf')) {
                const treeItem = toggle.closest('.tree-item');
                const itemId = treeItem.dataset.id;
                const safeId = itemId.replace(/[^a-zA-Z0-9_-]/g, '_');
                const childrenContainer = document.getElementById(`upload-children-${safeId}`);
                if (childrenContainer) {
                    childrenContainer.style.display = 'none';
                    toggle.classList.remove('expanded');
                    toggle.classList.add('collapsed');
                }
            }
        });
    }

    formatFileSize(bytes) {
        if (!bytes) return '';
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(bytes) / Math.log(1024));
        return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + sizes[i];
    }

    async handleBulkUpload() {
        const parentId = document.getElementById('bulk-parent-id').value.trim();
        const preserveStructure = document.getElementById('bulk-preserve-structure').checked;

        if (!parentId) {
            this.logMessage('Parent ID is required for bulk upload', true);
            return;
        }

        if (this.selectedUploadFiles.size === 0) {
            this.logMessage('Please select files to upload', true);
            return;
        }

        try {
            // Get selected file data - handle both old and new ID formats
            const selectedFileData = this.uploadFileItems.filter(file => {
                const fileId = `file_${file.path}`;
                return this.selectedUploadFiles.has(fileId) || this.selectedUploadFiles.has(file.id);
            });

            const result = await window.electronAPI.bulkUpload({
                parent_id: parentId,
                files: selectedFileData,
                preserve_folder_structure: preserveStructure
            });

            if (result.success) {
                this.logMessage(`Bulk upload initiated for ${selectedFileData.length} files`, false);
            } else {
                throw new Error(result.error);
            }
        } catch (error) {
            this.logMessage(`Bulk upload error: ${error.message}`, true);}
    }

    setOperationInProgress(operation, message) {
        const progressText = document.getElementById(`${operation}-progress-text`);
        const progressBar = document.getElementById(`${operation}-progress`);
        const button = document.getElementById(`${operation}-btn`);

        if (progressText) progressText.textContent = message;
        if (progressBar) {
            progressBar.style.width = '0%';
            progressBar.classList.remove('error'); // Remove any error styling
        }
        if (button) {
            // Button state management removed
            // Add visual indicator that operation is in progress
            if (!button.innerHTML.includes('fa-spinner')) {
                const originalText = button.innerHTML;
                button.dataset.originalText = originalText;
                button.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';
            }
        }

        this.setStatus(message);
    }

    updateProgress(operation, progress, message) {
        const progressText = document.getElementById(`${operation}-progress-text`);
        const progressBar = document.getElementById(`${operation}-progress`);

        if (progressText) progressText.textContent = message;
        if (progressBar) progressBar.style.width = `${progress}%`;

        this.setStatus(`${operation}: ${progress}%`);
    }

    setOperationComplete(operation, success) {
        const progressText = document.getElementById(`${operation}-progress-text`);
        const progressBar = document.getElementById(`${operation}-progress`);
        const button = document.getElementById(`${operation}-btn`);

        if (progressText) {
            progressText.textContent = success ? 'Operation completed' : 'Operation failed';
        }
        if (progressBar) {
            progressBar.style.width = success ? '100%' : '0%';
            if (success) {
                progressBar.classList.remove('error');
            } else {
                progressBar.classList.add('error');
            }
        }
        if (button) {
            // Restore original button text if it was saved
            if (button.dataset.originalText) {
                button.innerHTML = button.dataset.originalText;
                delete button.dataset.originalText;
            }
            // Don't just enable the button - check if it should be enabled based on current form state
        }

        this.setStatus(success ? 'Operation completed' : 'Operation failed');
    }

    updateButtonStateForOperation(operation) {
        switch (operation) {
            case 'upload':
                this.updateUploadButtonState();
                break;
            case 'download':
                this.updateDownloadButtonState();
                break;
            case 'bulk-upload':
                this.updateBulkUploadButtonState();
                break;
            case 'bulk-download':
                this.updateBulkDownloadButtonState();
                break;
            default:
                // For other operations, just enable the button
                const button = document.getElementById(`${operation}-btn`);
                if (button) {
                    // Button state management removed - no action needed
                }
                break;
        }
    }

    validateUploadForm() {
        const filePathEl = document.getElementById('upload-file');
        const modeEl = document.querySelector('input[name="uploadMode"]:checked');
        const parentIdEl = document.getElementById('parent-id');
        const entityIdEl = document.getElementById('entity-id');

        if (!filePathEl || !parentIdEl || !entityIdEl) return false;

        const filePath = filePathEl.value.trim();
        const mode = modeEl?.value;
        const parentId = parentIdEl.value.trim();
        const entityId = entityIdEl.value.trim();

        if (!filePath) return false;
        if (mode === 'new' && !parentId) return false;
        if (mode === 'update' && !entityId) return false;

        return true;
    }

    validateDownloadForm() {
        const synapseIdEl = document.getElementById('download-id');
        const downloadPathEl = document.getElementById('download-location');

        if (!synapseIdEl || !downloadPathEl) return false;

        const synapseId = synapseIdEl.value.trim();
        const downloadPath = downloadPathEl.value.trim();

        return synapseId && downloadPath;
    }

    updateUploadButtonState() {
        const button = document.getElementById('upload-btn');
        if (button) {
            // Only update state if not currently processing (no spinner)
            const isProcessing = button.innerHTML.includes('fa-spinner') || button.disabled && button.innerHTML.includes('Processing');
            if (!isProcessing) {
                // Button state management removed
            }
        }
    }

    updateDownloadButtonState() {
        const button = document.getElementById('download-btn');
        if (button) {
            // Only update state if not currently processing (no spinner)
            const isProcessing = button.innerHTML.includes('fa-spinner') || button.disabled && button.innerHTML.includes('Processing');
            if (!isProcessing) {
                // Button state management removed
            }
        }
    }

    updateBulkUploadButtonState() {
        const button = document.getElementById('bulk-upload-btn');
        const parentIdEl = document.getElementById('bulk-parent-id');

        if (button && parentIdEl) {
            // Only update state if not currently processing
            const isProcessing = button.innerHTML.includes('fa-spinner') || button.disabled && button.innerHTML.includes('Processing');
            if (!isProcessing) {
                const parentId = parentIdEl.value.trim();
                const hasSelectedFiles = this.selectedUploadFiles && this.selectedUploadFiles.size > 0;
                // Button state management removed
            }
        }
    }

    updateBulkDownloadButtonState() {
        const button = document.getElementById('bulk-download-btn');
        const downloadPathEl = document.getElementById('bulk-download-location');

        if (button && downloadPathEl) {
            // Only update state if not currently processing
            const isProcessing = button.innerHTML.includes('fa-spinner') || button.disabled && button.innerHTML.includes('Processing');
            if (!isProcessing) {
                const downloadPath = downloadPathEl.value.trim();
                const hasSelectedItems = this.selectedItems && this.selectedItems.size > 0;
                // Button state management removed
            }
        }
    }

    handleOperationComplete(operation, success, data) {
        console.log(`Operation complete: ${operation}, success: ${success}`, data);

        if (success) {
            this.logMessage(`${operation} completed successfully`, false);
            if (data && data.message) {
                this.logMessage(data.message, false);
            }
        } else {
            this.logMessage(`${operation} failed`, true);
            if (data && data.error) {
                this.logMessage(data.error, true);
            }
        }
    }

    logMessage(message, isError = false) {
        const outputLog = document.getElementById('output-log');
        const timestamp = new Date().toLocaleTimeString();
        const logClass = isError ? 'log-error' : 'log-success';
        const icon = isError ? '❌' : '✅';

        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${logClass}`;
        logEntry.textContent = `[${timestamp}] ${icon} ${message}`;

        outputLog.appendChild(logEntry);
        this.updateLogCount();
        this.autoScrollIfEnabled();
    }

    logMessageAdvanced(logMessage) {
        const outputLog = document.getElementById('output-log');
        const timestamp = logMessage.timestamp ?
            new Date(logMessage.timestamp * 1000).toLocaleTimeString() :
            new Date().toLocaleTimeString();

        // Determine icon based on level
        const icons = {
            'error': '❌',
            'critical': '🚨',
            'warning': '⚠️',
            'warn': '⚠️',
            'info': 'ℹ️',
            'debug': '🔍',
            'success': '✅'
        };

        const icon = icons[logMessage.level] || 'ℹ️';
        const logClass = `log-${logMessage.level || 'info'}`;

        const logEntry = document.createElement('div');
        logEntry.className = `log-entry ${logClass}`;

        // Create structured log entry
        let logContent = '';
        logContent += `<span class="log-timestamp">[${timestamp}]</span>`;

        if (logMessage.operation) {
            logContent += `<span class="log-operation">[${logMessage.operation}]</span>`;
        }

        if (logMessage.source && logMessage.source !== 'synapse-backend') {
            logContent += `<span class="log-source">[${logMessage.source}]</span>`;
        }

        logContent += ` ${icon} ${this.escapeHtml(logMessage.message)}`;

        logEntry.innerHTML = logContent;
        outputLog.appendChild(logEntry);

        this.updateLogCount();

        // Always attempt auto-scroll for real-time updates
        this.autoScrollIfEnabled();
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    updateLogCount() {
        const outputLog = document.getElementById('output-log');
        const count = outputLog.children.length;
        const logCountElement = document.getElementById('log-count');
        if (logCountElement) {
            logCountElement.textContent = `${count} message${count !== 1 ? 's' : ''}`;
        }
    }

    autoScrollIfEnabled() {
        if (!this.isAutoScrollEnabled()) {
            return;
        }

        const now = Date.now();

        // Throttle scroll operations to prevent excessive scrolling
        if (now - this.lastScrollTime < this.scrollThrottleDelay) {
            if (!this.pendingScroll) {
                this.pendingScroll = true;
                setTimeout(() => {
                    this.pendingScroll = false;
                    if (this.isAutoScrollEnabled()) {
                        this.performScroll();
                    }
                }, this.scrollThrottleDelay);
            }
            return;
        }

        this.performScroll();
    }

    performScroll() {
        this.lastScrollTime = Date.now();

        // Use requestAnimationFrame to ensure DOM has updated before scrolling
        requestAnimationFrame(() => {
            this.scrollToBottom();
        });
    }

    isAutoScrollEnabled() {
        return this.autoScrollEnabled !== false; // Default to true
    }

    scrollToBottom() {
        const outputLog = document.getElementById('output-log');
        if (!outputLog) return;

        // For real-time scrolling, we want immediate results, so use instant scroll
        // but also ensure it gets to the very bottom
        const targetScrollTop = outputLog.scrollHeight - outputLog.clientHeight;

        // Set scroll position immediately
        outputLog.scrollTop = targetScrollTop;

        // Double-check after a short delay to handle any DOM updates
        setTimeout(() => {
            const newTargetScrollTop = outputLog.scrollHeight - outputLog.clientHeight;
            if (outputLog.scrollTop < newTargetScrollTop - 5) { // 5px tolerance
                outputLog.scrollTop = newTargetScrollTop;
            }
        }, 10);
    }

    toggleAutoScroll() {
        this.autoScrollEnabled = !this.isAutoScrollEnabled();
        this.updateAutoScrollUI();
    }

    updateAutoScrollUI() {
        const toggleBtn = document.getElementById('auto-scroll-toggle');
        const statusSpan = document.getElementById('auto-scroll-status');

        if (this.isAutoScrollEnabled()) {
            toggleBtn.classList.add('auto-scroll-enabled');
            toggleBtn.classList.remove('auto-scroll-disabled');
            statusSpan.textContent = 'Auto-scroll: ON';
            statusSpan.className = 'auto-scroll-enabled';
        } else {
            toggleBtn.classList.remove('auto-scroll-enabled');
            toggleBtn.classList.add('auto-scroll-disabled');
            statusSpan.textContent = 'Auto-scroll: OFF';
            statusSpan.className = 'auto-scroll-disabled';
        }
    }

    handleScrollCommand(message) {
        switch (message.action) {
            case 'scroll_to_bottom':
                this.scrollToBottom();
                break;
            default:
                console.log('Unknown scroll command:', message.action);
        }
    }

    clearOutput() {
        document.getElementById('output-log').innerHTML = '';
        this.updateLogCount();

        // Reset scroll state
        this.pendingScroll = false;
        this.lastScrollTime = 0;
    }

    handleManualScroll(event) {
        // Manual scrolling no longer affects auto-scroll state
        // Auto-scroll will continue to work regardless of manual scrolling
        // Only the explicit toggle button can disable auto-scroll
    }

    setStatus(message) {
        document.getElementById('status-message').textContent = message;
    }

    showStatus(elementId, message, type) {
        const element = document.getElementById(elementId);
        element.textContent = message;
        element.className = `status-message ${type}`;
        element.style.display = 'block';

        // Auto-hide after 5 seconds
        setTimeout(() => {
            element.style.display = 'none';
        }, 5000);
    }
}

// Initialize the application when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.synapseApp = new SynapseDesktopClient();
});
