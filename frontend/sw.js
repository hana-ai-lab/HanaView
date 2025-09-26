// frontend/sw.js - 通知受信部分のみに集中

// Push通知受信
self.addEventListener('push', event => {
    console.log('Push notification received');

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
        body: data.body || '市況データが更新されました',
        icon: './icons/icon-192x192.png',
        badge: './icons/icon-192x192.png',
        vibrate: [100, 50, 100],
        data: {
            dateOfArrival: Date.now(),
            type: data.type || 'data-update'
        }
    };

    event.waitUntil(
        self.registration.showNotification(
            data.title || 'HanaView更新通知',
            options
        )
    );
});

// 通知クリック
self.addEventListener('notificationclick', event => {
    event.notification.close();

    event.waitUntil(
        clients.openWindow('/')
    );
});