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
const TOKEN_STORE = 'auth-tokens';
let cachedToken = null;

// メッセージリスナーでトークンを受け取る
self.addEventListener('message', async (event) => {
    if (event.data.type === 'SET_TOKEN') {
        cachedToken = event.data.token;
        console.log('[SW] Token cached');

        // IndexedDBに保存（永続化）
        await saveTokenToIndexedDB(event.data.token);
    }
});

// IndexedDBヘルパー関数
async function saveTokenToIndexedDB(token) {
    const db = await openDB();
    const tx = db.transaction([TOKEN_STORE], 'readwrite');
    const store = tx.objectStore(TOKEN_STORE);
    await store.put({ id: 'main', token: token, timestamp: Date.now() });
    console.log('[SW] Token saved to IndexedDB');
}

async function getTokenFromIndexedDB() {
    try {
        const db = await openDB();
        const tx = db.transaction([TOKEN_STORE], 'readonly');
        const store = tx.objectStore(TOKEN_STORE);
        const data = await store.get('main');
        return data ? data.token : null;
    } catch (error) {
        console.error('[SW] Failed to get token from IndexedDB:', error);
        return null;
    }
}

async function openDB() {
    return new Promise((resolve, reject) => {
        const request = indexedDB.open('HanaViewDB', 1);

        request.onerror = () => reject(request.error);
        request.onsuccess = () => resolve(request.result);

        request.onupgradeneeded = (event) => {
            const db = event.target.result;
            if (!db.objectStoreNames.contains(TOKEN_STORE)) {
                db.createObjectStore(TOKEN_STORE, { keyPath: 'id' });
            }
        };
    });
}


// Install event
self.addEventListener('install', event => {
  console.log('Service Worker: Installing...');
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('Service Worker: Caching app shell');
      return cache.addAll(APP_SHELL_URLS);
    })
  );
  self.skipWaiting();
});

// Activate event
self.addEventListener('activate', event => {
  console.log('Service Worker: Activating...');
  event.waitUntil(
    caches.keys().then(cacheNames => {
      return Promise.all(
        cacheNames.map(cache => {
          if (cache !== CACHE_NAME) {
            console.log('Service Worker: Clearing old cache', cache);
            return caches.delete(cache);
          }
        })
      );
    })
  );
  self.clients.claim();
});

// Fetch イベントの修正
self.addEventListener('fetch', async (event) => {
    const { request } = event;

    // APIリクエストの場合、認証ヘッダーを追加
    if (request.url.includes('/api/') &&
        !request.url.includes('/api/auth/') &&
        !request.url.includes('/api/vapid-public-key')) {

        event.respondWith(
            (async () => {
                // トークンを取得（キャッシュ優先）
                let token = cachedToken;
                if (!token) {
                    token = await getTokenFromIndexedDB();
                    cachedToken = token;  // キャッシュ
                }

                if (token) {
                    const modifiedRequest = new Request(request, {
                        headers: new Headers({
                            ...Object.fromEntries(request.headers.entries()),
                            'Authorization': `Bearer ${token}`
                        })
                    });
                    return fetch(modifiedRequest);
                }

                return fetch(request);
            })()
        );
        return;
    }

    // 既存のキャッシュ処理
    event.respondWith(
        caches.match(request).then(cachedResponse => {
            return cachedResponse || fetch(request);
        })
    );
});

// Push notification handler
self.addEventListener('push', event => {
  console.log('Service Worker: Push notification received');
  let data = { title: 'HanaView更新', body: '新しいデータがあります' };
  if (event.data) {
    try { data = event.data.json(); } catch (e) { /* ignore */ }
  }

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

// Notification click handler
self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    syncData().then(() => {
      return clients.matchAll({ type: 'window', includeUncontrolled: true })
        .then(windowClients => {
          const client = windowClients.find(c => c.url.endsWith('/'));
          if (client) return client.focus();
          return clients.openWindow('/');
        });
    })
  );
});

// Background sync handler
self.addEventListener('sync', event => {
  console.log('Service Worker: Background sync triggered', event.tag);
  if (event.tag === 'data-sync') {
    event.waitUntil(syncData());
  }
});

// --- UPDATED: Background sync function ---
async function syncData() {
  try {
    console.log('Service Worker: Syncing data...');
    // The new fetch handler will add auth headers automatically
    const response = await fetch(DATA_URL);
    if (response.ok) {
      const data = await response.json();
      const cache = await caches.open(CACHE_NAME);
      // Use a fixed URL for the data cache key
      await cache.put(DATA_URL, new Response(JSON.stringify(data)));

      const allClients = await self.clients.matchAll();
      allClients.forEach(client => {
        client.postMessage({ type: 'data-updated', data: data, timestamp: new Date().toLocaleTimeString() });
      });
      console.log('Service Worker: Data sync completed.');
      return true;
    }
    console.error('Service Worker: Sync failed with status', response.status);
  } catch (error) {
    console.error('Service Worker: Sync failed', error);
    throw error;
  }
}

// Periodic background sync (if supported)
self.addEventListener('periodicsync', event => {
  if (event.tag === 'data-update') {
    event.waitUntil(syncData());
  }
});