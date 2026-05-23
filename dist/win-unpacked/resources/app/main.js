const { app, BrowserWindow, Tray, Menu, nativeImage, shell, dialog } = require('electron');
const path = require('path');
const net  = require('net');

let mainWindow = null;
let tray = null;
let appPort = 3000;

function findFreePort(start) {
  return new Promise(resolve => {
    const s = net.createServer();
    s.listen(start, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
    s.on('error', () => resolve(findFreePort(start + 1)));
  });
}

function startServer(port) {
  process.env.MESHNATTER_PORT = String(port);
  try {
    require('./server.bundle.cjs');
    console.log('[main] Server started on port', port);
  } catch (err) {
    dialog.showErrorBox('Meshnatter Error', 'The server failed to start.\n\n' + err.message);
    console.error('[main] Server error:', err.stack);
    app.quit();
  }
}

function waitForWS(port, retries = 50) {
  return new Promise((resolve, reject) => {
    const attempt = n => {
      const s = net.createConnection({ port, host: '127.0.0.1' });
      s.on('connect', () => { s.destroy(); resolve(); });
      s.on('error', () => {
        if (n <= 0) return reject(new Error('Server did not start on port ' + port));
        setTimeout(() => attempt(n - 1), 200);
      });
    };
    attempt(retries);
  });
}

function createWindow(port) {
  const indexPath = path.join(__dirname, 'index.html');
  mainWindow = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 800,
    minHeight: 600,
    backgroundColor: '#000000',
    frame: true,
    title: 'Meshnatter',
    webPreferences: {
      nodeIntegration: false,           // Never expose Node.js to renderer
      contextIsolation: true,           // Renderer isolated from main process
      webSecurity: true,                // Same-origin policy enforced
      allowRunningInsecureContent: false,
      experimentalFeatures: false,
      devTools: false,                  // No devtools in production
      additionalArguments: [`--ws-port=${port}`],
    },
    icon: path.join(__dirname, 'assets', 'icon.ico'),
    show: false,
  });

  mainWindow.setMenuBarVisibility(false);


  mainWindow.loadFile(indexPath);

  mainWindow.once('ready-to-show', () => {
    mainWindow.show();
    console.log('[main] Window shown');
  });

  mainWindow.webContents.on('did-finish-load', () => {
    console.log('[main] Page loaded OK');
  });

  mainWindow.webContents.on('did-fail-load', (e, code, desc, url) => {
    console.error('[main] Load failed:', code, desc, url);
    dialog.showErrorBox('Load Failed',
      `Code: ${code}\nDesc: ${desc}\nURL: ${url}\nindex.html: ${indexPath}`);
  });

  mainWindow.webContents.on('render-process-gone', (e, details) => {
    console.error('[main] Renderer crashed:', details.reason);
    dialog.showErrorBox('Renderer Crashed', JSON.stringify(details));
  });

  mainWindow.webContents.on('console-message', (e, level, msg, line, src) => {
    const levels = ['verbose','info','warning','error'];
    console.log(`[renderer:${levels[level]||level}] ${msg}`);
  });

  // Validate URLs before opening externally — prevent file://, javascript: etc
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    const allowed = /^https?:\/\//i.test(url);
    if (allowed) shell.openExternal(url);
    return { action: 'deny' }; // always deny Electron opening a new window
  });

  // Block navigation away from the local file
  mainWindow.webContents.on('will-navigate', (e, navUrl) => {
    if (!navUrl.startsWith('file://')) {
      e.preventDefault();
      console.warn('[main] Blocked navigation to:', navUrl);
    }
  });

  // Block new window creation from renderer
  mainWindow.webContents.on('new-window', (e) => {
    e.preventDefault();
  });

  mainWindow.on('closed', () => { mainWindow = null; });
}

function createTray() {
  try {
    const img = nativeImage.createFromPath(path.join(__dirname, 'assets', 'icon.ico'));
    tray = new Tray(img.isEmpty() ? nativeImage.createEmpty() : img);
    tray.setToolTip('Meshnatter');
    tray.setContextMenu(Menu.buildFromTemplate([
      { label: 'Meshnatter', enabled: false }, { type: 'separator' },
      { label: 'Show', click: () => mainWindow?.show() },
      { type: 'separator' },
      { label: 'Quit', click: () => { app.isQuitting = true; app.quit(); } },
    ]));
    tray.on('click', () => mainWindow?.show());
  } catch (e) { console.error('[main] Tray:', e.message); }
}

// Required for Windows taskbar pinning — keeps icon linked to the installed app
app.setAppUserModelId('com.meshnatter.app');

if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on('second-instance', () => mainWindow?.show());

  app.whenReady().then(async () => {
    appPort = await findFreePort(3000);
    startServer(appPort);

    try { await waitForWS(appPort); }
    catch (e) {
      dialog.showErrorBox('Meshnatter', 'Server did not start:\n\n' + e.message);
      app.quit(); return;
    }

    createWindow(appPort);
    createTray();
  });

  app.on('window-all-closed', () => { if (process.platform !== 'darwin') app.quit(); });
  app.on('activate', () => { if (!mainWindow) createWindow(appPort); else mainWindow.show(); });
  app.on('before-quit', () => { app.isQuitting = true; });
}
