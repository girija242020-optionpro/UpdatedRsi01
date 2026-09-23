const CACHE_NAME = 'rsi-dema-volume-alarm-v3';

self.addEventListener('install', event => {
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)));
    await self.clients.claim();
  })());
});

// This is the important part: Web Push is delivered by the OS/browser even
// when the PWA page is not running. The page itself does NOT need to stay open.
self.addEventListener('push', event => {
  let data = { title: 'RSI DEMA VOLUME ALERT', body: 'Entry trigger confirmed', type: 'RSI_ALARM' };
  try {
    if (event.data) data = { ...data, ...event.data.json() };
  } catch (_) {
    try { if (event.data) data.body = event.data.text(); } catch (_) {}
  }

  const options = {
    body: data.body || 'Entry trigger confirmed',
    icon: './icons/icon-192.png',
    badge: './icons/icon-192.png',
    vibrate: [400,150,400,150,400,150,400,150,400],
    requireInteraction: true,
    renotify: true,
    silent: false,
    tag: 'rsi-dema-volume-entry',
    data
  };

  event.waitUntil((async () => {
    await self.registration.showNotification(data.title || 'RSI Entry Alert', options);
    const clientsList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const client of clientsList) {
      client.postMessage({ type: 'RSI_ALARM', data });
    }
  })());
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil((async () => {
    const list = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of list) {
      if ('focus' in c) { await c.focus(); return; }
    }
    if (self.clients.openWindow) await self.clients.openWindow('./');
  })());
});
