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

// Install event
self.addEventListener('install', event => {
  console.log('Service Worker: Installing...');
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('Service Worker: Caching app shell');
      return cache.addAll(APP_SHELL_URLS);
    })
  );
  self.skipWaiting(); // Activate worker immediately
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
  self.clients.claim(); // Take control of all clients immediately
});

// Fetch event
self.addEventListener('fetch', event => {
  const { request } = event;

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

  event.respondWith(
    caches.match(request).then(cachedResponse => {
      if (cachedResponse) {
        return cachedResponse;
      }
      return fetch(request);
    })
  );
});

// Push notification handler
self.addEventListener('push', event => {
  console.log('Service Worker: Push notification received at 6:30 AM');

  let data = {};
  if (event.data) {
    try {
      data = event.data.json();
    } catch (e) {
      data = {
        title: 'HanaView更新',
        body: event.data.text()
      };
    }
  }

  const options = {
    body: data.body || '朝6:30の市況データが更新されました',
    icon: './icons/icon-192x192.png',
    badge: './icons/icon-192x192.png',
    vibrate: [100, 50, 100],
    data: {
      dateOfArrival: Date.now(),
      primaryKey: 1,
      type: data.type || 'data-update'
    },
    actions: [
      {
        action: 'view',
        title: '表示',
        icon: './icons/icon-192x192.png'
      },
      {
        action: 'close',
        title: '閉じる',
        icon: './icons/icon-192x192.png'
      }
    ]
  };

  event.waitUntil(
    self.registration.showNotification(
      data.title || 'HanaView更新通知',
      options
    ).then(() => {
      // Trigger background sync after showing notification
      if ('sync' in self.registration) {
        return self.registration.sync.register('data-sync');
      }
    })
  );
});

// Notification click handler
self.addEventListener('notificationclick', event => {
  console.log('Notification clicked:', event.action);
  event.notification.close();

  if (event.action === 'view' || !event.action) {
    event.waitUntil(
      clients.openWindow('/')
    );
  }
});

// Background sync handler
self.addEventListener('sync', event => {
  console.log('Service Worker: Background sync triggered');

  if (event.tag === 'data-sync') {
    event.waitUntil(syncData());
  }
});

// Background sync function
async function syncData() {
  try {
    console.log('Service Worker: Syncing data from 6:30 AM report...');

    // Fetch latest data
    const response = await fetch('/api/data');
    if (response.ok) {
      const data = await response.json();

      // Update cache
      const cache = await caches.open(CACHE_NAME);
      await cache.put('/api/data', new Response(JSON.stringify(data)));

      // Notify all clients to refresh
      const clients = await self.clients.matchAll();
      clients.forEach(client => {
        client.postMessage({
          type: 'data-updated',
          data: data,
          timestamp: '6:30'
        });
      });

      console.log('Service Worker: Data sync completed for 6:30 AM update');
      return true;
    }
  } catch (error) {
    console.error('Service Worker: Sync failed', error);
    throw error; // Retry sync later
  }
}

// Periodic background sync (if supported)
self.addEventListener('periodicsync', event => {
  if (event.tag === 'data-update') {
    event.waitUntil(syncData());
  }
});
