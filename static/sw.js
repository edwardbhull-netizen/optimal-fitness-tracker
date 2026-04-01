/* ── Optimal Fitness Tracker — Service Worker ─────────────────────────────── */

const CACHE_NAME = 'of-tracker-v1';
const STATIC_ASSETS = [
  '/',
  '/static/style.css',
  '/static/app.js',
  '/static/manifest.json',
  '/static/logo.png',
];

// Install: cache static assets
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(STATIC_ASSETS).catch(err => {
        console.log('SW: some assets failed to cache', err);
      });
    })
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    )
  );
  self.clients.claim();
});

// Fetch: network-first for API, cache-first for static
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // Skip non-GET requests
  if (event.request.method !== 'GET') return;

  // Skip API calls — always go to network
  if (url.pathname.startsWith('/client/') ||
      url.pathname.startsWith('/coach') ||
      url.pathname === '/login' ||
      url.pathname === '/logout') {
    event.respondWith(
      fetch(event.request).catch(() => {
        return new Response(
          '<html><body style="font-family:system-ui;padding:40px;text-align:center"><h2>Offline</h2><p>You are offline. Please reconnect to log workouts.</p></body></html>',
          { headers: { 'Content-Type': 'text/html' } }
        );
      })
    );
    return;
  }

// Push notifications
self.addEventListener('push', event => {
  let data = { title: 'Optimal Fitness', body: 'You have a new message.' };
  if (event.data) {
    try { data = event.data.json(); } catch(e) { data.body = event.data.text(); }
  }
  event.waitUntil(
    self.registration.showNotification(data.title || 'Optimal Fitness', {
      body: data.body || '',
      icon: '/static/logo.png',
      badge: '/static/logo.png',
      vibrate: [200, 100, 200],
      data: { url: data.url || '/client/home' },
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = event.notification.data?.url || '/client/home';
  event.waitUntil(clients.openWindow(url));
});

  // Static assets: cache-first

  event.respondWith(
    caches.match(event.request).then(cached => {
      return cached || fetch(event.request).then(response => {
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(event.request, clone));
        }
        return response;
      });
    })
  );
});
