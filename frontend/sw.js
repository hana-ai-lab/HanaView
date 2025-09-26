const CACHE_NAME = 'hanaview-cache-v2';
const APP_SHELL_URLS = [
  './',
  './index.html',
  './style.css',
  './app.js',
  './manifest.json',
  './icons/icon-192x192.png',
  './icons/icon-512x512.png',
  'https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js',
  'https://d3js.org/d3.v7.min.js'
];
const DATA_URL = '/api/data';
const DB_NAME = 'HanaViewDB';
const DB_VERSION = 1;
const TOKEN_STORE_NAME = 'auth-tokens';
let cachedToken = null;

// --- IndexedDB Helper ---
function openDB() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);
        request.onerror = (event) => reject("Error opening DB: " + event.target.errorCode);
        request.onsuccess = (event) => resolve(event.target.result);
        request.onupgradeneeded = event => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(TOKEN_STORE_NAME)) {
                db.createObjectStore(TOKEN_STORE_NAME, { keyPath: 'id' });
            }
        };
    });
}

async function saveTokenToDB(token) {
    try {
        const db = await openDB();
        const transaction = db.transaction([TOKEN_STORE_NAME], 'readwrite');
        const store = transaction.objectStore(TOKEN_STORE_NAME);
        await store.put({ id: 'auth_token', value: token });
        console.log('[SW] Token saved to IndexedDB.');
    } catch (error) {
        console.error('[SW] Failed to save token to DB:', error);
    }
}

async function getTokenFromDB() {
    try {
        const db = await openDB();
        const transaction = db.transaction([TOKEN_STORE_NAME], 'readonly');
        const store = transaction.objectStore(TOKEN_STORE_NAME);
        const request = await store.get('auth_token');
        return request ? request.value : null;
    } catch (error) {
        console.error('[SW] Failed to get token from DB:', error);
        return null;
    }
}

// --- Message Listener for Token ---
self.addEventListener('message', async (event) => {
    if (event.data && event.data.type === 'SET_TOKEN') {
        cachedToken = event.data.token;
        console.log('[SW] Token received and cached.');
        await saveTokenToDB(event.data.token);
    }
});

// --- Install and Activate ---
self.addEventListener('install', event => {
  console.log('Service Worker: Installing...');
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(APP_SHELL_URLS))
  );
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  console.log('Service Worker: Activating...');
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.filter(cache => cache !== CACHE_NAME).map(cache => caches.delete(cache))
      );
    })
  );
  self.clients.claim();
});

// --- Fetch Event Handler ---
self.addEventListener('fetch', event => {
    const { request } = event;

    // For API requests that need authentication
    if (request.url.includes('/api/') && !request.url.includes('/api/auth/') && !request.url.includes('/api/vapid-public-key')) {
        event.respondWith(
            (async () => {
                let token = cachedToken;
                if (!token) {
                    token = await getTokenFromDB();
                    if (token) cachedToken = token;
                }

                const headers = new Headers(request.headers);
                if (token) {
                    headers.append('Authorization', `Bearer ${token}`);
                }

                const modifiedRequest = new Request(request, { headers });

                try {
                    const networkResponse = await fetch(modifiedRequest);
                    // Handle data caching for the main data URL
                    if (request.url.includes(DATA_URL) && networkResponse.ok) {
                        const cache = await caches.open(CACHE_NAME);
                        cache.put(DATA_URL, networkResponse.clone());
                    }
                    return networkResponse;
                } catch (error) {
                    console.error('[SW] Fetch failed, trying cache for', request.url);
                    const cachedResponse = await caches.match(request.url.includes(DATA_URL) ? DATA_URL : request);
                    if (cachedResponse) return cachedResponse;
                    // Return the error if nothing is in cache
                    throw error;
                }
            })()
        );
        return;
    }

    // For other requests, use a cache-first strategy
    event.respondWith(
        caches.match(request).then(cachedResponse => {
            return cachedResponse || fetch(request);
        })
    );
});

// --- Push and Sync ---
self.addEventListener('push', event => {
  console.log('[SW] Push notification received');
  let data = { title: 'HanaView 更新', body: '新しいデータがあります' };
  try { if (event.data) data = event.data.json(); } catch(e) {}

  const options = {
    body: data.body,
    icon: './icons/icon-192x192.png',
    badge: './icons/icon-192x192.png',
    actions: [{ action: 'view', title: '表示' }]
  };

  event.waitUntil(
    self.registration.showNotification(data.title, options)
      .then(() => self.registration.sync.register('data-sync'))
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    syncData().then(() => {
      return clients.matchAll({ type: 'window', includeUncontrolled: true })
        .then(windowClients => {
          return windowClients.find(c => c.url.endsWith('/'))?.focus() || clients.openWindow('/');
        });
    })
  );
});

self.addEventListener('sync', event => {
  if (event.tag === 'data-sync') {
    event.waitUntil(syncData());
  }
});

async function syncData() {
  console.log('[SW] Starting data sync...');
  try {
    // The fetch event listener will automatically add the auth header
    const response = await fetch(DATA_URL);
    if (response.ok) {
      const data = await response.json();
      const allClients = await self.clients.matchAll();
      allClients.forEach(client => {
        client.postMessage({ type: 'data-updated', data: data, timestamp: new Date().toLocaleTimeString() });
      });
      console.log('[SW] Data sync completed successfully.');
      return true;
    }
    console.error('[SW] Sync fetch failed with status:', response.status);
  } catch (error) {
    console.error('[SW] Sync failed with error:', error);
    throw error;
  }
}