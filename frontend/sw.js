const CACHE_NAME = 'hanaview-cache-v1';
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

// Install event: cache the app shell
self.addEventListener('install', event => {
  console.log('Service Worker: Installing...');
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      console.log('Service Worker: Caching app shell');
      return cache.addAll(APP_SHELL_URLS);
    })
  );
});

// Activate event: clean up old caches
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
});

// Fetch event: apply caching strategies
self.addEventListener('fetch', event => {
    const { request } = event;

    // Strategy 1: Stale-While-Revalidate for API data
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
                    // Return cached response if available, otherwise wait for network
                    // This provides a fast response while updating the cache in the background.
                    return cachedResponse || networkResponsePromise;
                });
            })
        );
        return; // IMPORTANT: End execution for this strategy
    }

    // Strategy 2: Cache First for App Shell and other assets
    event.respondWith(
        caches.match(request).then(cachedResponse => {
            if (cachedResponse) {
                return cachedResponse;
            }
            // If not in cache, fetch from network
            return fetch(request).then(networkResponse => {
                // Optionally, cache newly fetched assets if they are important
                // For now, we only pre-cache the app shell, so we just return the response.
                return networkResponse;
            });
        }).catch(error => {
            // This catch handles errors like the user being offline.
            // You could return a custom offline page here if you had one.
            console.error('Service Worker: Fetch failed; user is likely offline.', error);
            // The browser will show its default offline page.
        })
    );
});
