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

// --- IndexedDB Helper ---
function openDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onerror = () => reject("Error opening DB");
    request.onsuccess = () => resolve(request.result);
    request.onupgradeneeded = event => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(TOKEN_STORE_NAME)) {
        db.createObjectStore(TOKEN_STORE_NAME, { keyPath: 'id' });
      }
    };
  });
}

async function getTokenFromDB() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction([TOKEN_STORE_NAME], 'readonly');
    const store = transaction.objectStore(TOKEN_STORE_NAME);
    const request = store.get('auth_token');
    request.onerror = () => reject("Error fetching token");
    request.onsuccess = () => {
      resolve(request.result ? request.result.value : null);
    };
  });
}

// --- Authenticated Fetch for Service Worker ---
async function fetchWithAuthFromSW(url, options = {}) {
  const token = await getTokenFromDB();
  const headers = new Headers(options.headers || {});
  if (token) {
    headers.append('Authorization', `Bearer ${token}`);
  }
  return fetch(url, { ...options, headers });
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

// Fetch event (Stale-while-revalidate for data)
self.addEventListener('fetch', event => {
  const { request } = event;

  // For data API, use stale-while-revalidate.
  // The request from the app already has auth headers.
  if (request.url.includes(DATA_URL)) {
    event.respondWith(
      caches.open(CACHE_NAME).then(cache => {
        const networkResponsePromise = fetch(request).then(networkResponse => {
          if (networkResponse.ok) {
            cache.put(request, networkResponse.clone());
          }
          return networkResponse;
        });

        return cache.match(request).then(cachedResponse => {
          return cachedResponse || networkResponsePromise;
        });
      })
    );
    return;
  }

  // For app shell, use cache-first.
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
    // Use the authenticated fetch function
    const response = await fetchWithAuthFromSW(DATA_URL);
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