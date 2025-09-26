document.addEventListener('DOMContentLoaded', () => {
    console.log("HanaView App Initializing...");

    // --- DOM Element References ---
    const authContainer = document.getElementById('auth-container');
    const dashboardContainer = document.querySelector('.container');
    const pinInputsContainer = document.getElementById('pin-inputs');
    const pinInputs = pinInputsContainer ? Array.from(pinInputsContainer.querySelectorAll('input')) : [];
    const authErrorMessage = document.getElementById('auth-error-message');
    const authSubmitButton = document.getElementById('auth-submit-button');
    const authLoadingSpinner = document.getElementById('auth-loading');

    // --- State ---
    let failedAttempts = 0;
    const MAX_ATTEMPTS = 5;

    // --- IndexedDB & Auth Config ---
    const DB_NAME = 'HanaViewDB';
    const DB_VERSION = 1;
    const TOKEN_STORE_NAME = 'auth-tokens';

    // --- NEW: Authentication Management (with IndexedDB support) ---
    class AuthManager {
        static TOKEN_KEY = 'auth_token';
        static EXPIRY_KEY = 'auth_expiry';

        static async setTokenInDB(token) {
            return new Promise((resolve, reject) => {
                const request = indexedDB.open(DB_NAME, DB_VERSION);
                request.onerror = () => reject("Error opening DB for token storage");
                request.onupgradeneeded = event => {
                    const db = event.target.result;
                    if (!db.objectStoreNames.contains(TOKEN_STORE_NAME)) {
                        db.createObjectStore(TOKEN_STORE_NAME, { keyPath: 'id' });
                    }
                };
                request.onsuccess = event => {
                    const db = event.target.result;
                    const transaction = db.transaction([TOKEN_STORE_NAME], 'readwrite');
                    const store = transaction.objectStore(TOKEN_STORE_NAME);
                    if (token) {
                        store.put({ id: 'auth_token', value: token });
                    } else {
                        store.delete('auth_token');
                    }
                    transaction.oncomplete = () => resolve();
                    transaction.onerror = () => reject("Error storing token in DB");
                };
            });
        }

        static async setAuthData(token, expiresIn) {
            localStorage.setItem(this.TOKEN_KEY, token);
            const expiryTime = Date.now() + (expiresIn * 1000);
            localStorage.setItem(this.EXPIRY_KEY, expiryTime.toString());
            try {
                await this.setTokenInDB(token);
                console.log('Auth token stored in localStorage and IndexedDB. Expires at:', new Date(expiryTime).toLocaleString());
            } catch (error) {
                console.error("Failed to store token in IndexedDB:", error);
            }
        }

        static getToken() {
            const token = localStorage.getItem(this.TOKEN_KEY);
            const expiry = localStorage.getItem(this.EXPIRY_KEY);
            if (!token || !expiry || Date.now() > parseInt(expiry)) {
                if (token) this.clearAuthData(); // Clear if expired
                return null;
            }
            return token;
        }

        static async clearAuthData() {
            localStorage.removeItem(this.TOKEN_KEY);
            localStorage.removeItem(this.EXPIRY_KEY);
            try {
                await this.setTokenInDB(null); // Remove from DB
                console.log('Auth data cleared from localStorage and IndexedDB');
            } catch (error) {
                console.error("Failed to clear token from IndexedDB:", error);
            }
        }

        static isAuthenticated() {
            return this.getToken() !== null;
        }

        static getAuthHeaders() {
            const token = this.getToken();
            return token ? { 'Authorization': `Bearer ${token}` } : {};
        }
    }

    // --- NEW: Authenticated Fetch Wrapper ---
    async function fetchWithAuth(url, options = {}) {
        const authHeaders = AuthManager.getAuthHeaders();
        const response = await fetch(url, {
            ...options,
            headers: { ...options.headers, ...authHeaders }
        });

        if (response.status === 401) {
            console.log('Authentication failed (401), redirecting to auth screen');
            await AuthManager.clearAuthData();
            showAuthScreen();
            throw new Error('Authentication required');
        }
        return response;
    }

    // --- Main App Logic (Updated) ---

    let notificationManagerInstance = null;

    async function initializeApp() {
        try {
            if (AuthManager.isAuthenticated()) {
                const response = await fetchWithAuth('/api/auth/check');
                const data = await response.json();
                if (data.authenticated) {
                    showDashboard();
                } else {
                    await AuthManager.clearAuthData();
                    showAuthScreen();
                }
            } else {
                showAuthScreen();
            }
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error('Error during authentication check:', error);
                if (authErrorMessage) authErrorMessage.textContent = 'サーバーとの通信に失敗しました。';
            }
            // Ensure auth screen is shown even on fetch failure
            showAuthScreen();
        }
    }

    function showDashboard() {
        if (authContainer) authContainer.style.display = 'none';
        if (dashboardContainer) dashboardContainer.style.display = 'block';

        if (!dashboardContainer.dataset.initialized) {
            console.log("HanaView Dashboard Initialized");
            initTabs();
            fetchDataAndRender();
            initSwipeNavigation();
            dashboardContainer.dataset.initialized = 'true';
        }

        // 通知マネージャーの初期化（遅延実行でiOS PWAの問題を回避）
        setTimeout(() => {
            if (!notificationManagerInstance) {
                console.log("Creating NotificationManager instance");
                notificationManagerInstance = new NotificationManager();
                notificationManagerInstance.init();
            } else {
                // トークンをService Workerに再送信
                notificationManagerInstance.sendTokenToServiceWorker();
            }
        }, 1000);  // 1秒待つ
    }

    function showAuthScreen() {
        if (authContainer) authContainer.style.display = 'flex';
        if (dashboardContainer) dashboardContainer.style.display = 'none';
        setupAuthForm();
    }

    function setupAuthForm() {
        if (!pinInputsContainer) return;
        pinInputs.forEach(input => { input.value = ''; input.disabled = false; });
        if(authSubmitButton) authSubmitButton.disabled = false;
        if(authErrorMessage) authErrorMessage.textContent = '';
        failedAttempts = 0;
        pinInputs[0]?.focus();

        pinInputs.forEach((input, index) => {
            input.addEventListener('input', () => {
                if (input.value.length === 1 && index < pinInputs.length - 1) {
                    pinInputs[index + 1].focus();
                }
            });
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                    pinInputs[index - 1].focus();
                }
            });
            input.addEventListener('paste', (e) => {
                e.preventDefault();
                const pasteData = e.clipboardData.getData('text').trim();
                if (/^\d{6}$/.test(pasteData)) {
                    pasteData.split('').forEach((char, i) => { if (pinInputs[i]) pinInputs[i].value = char; });
                    handleAuthSubmit();
                }
            });
        });

        if (authSubmitButton) {
            const newButton = authSubmitButton.cloneNode(true);
            authSubmitButton.parentNode.replaceChild(newButton, authSubmitButton);
            newButton.addEventListener('click', handleAuthSubmit);
        }
    }

    async function handleAuthSubmit() {
        const pin = pinInputs.map(input => input.value).join('');
        if (pin.length !== 6) {
            if (authErrorMessage) authErrorMessage.textContent = '6桁のコードを入力してください。';
            return;
        }
        setLoading(true);
        try {
            const response = await fetch('/api/auth/verify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pin: pin })
            });
            const data = await response.json();
            if (response.ok && data.success) {
                await AuthManager.setAuthData(data.token, data.expires_in);
                showDashboard();
            } else {
                failedAttempts++;
                pinInputs.forEach(input => input.value = '');
                pinInputs[0].focus();
                if (failedAttempts >= MAX_ATTEMPTS) {
                    if (authErrorMessage) authErrorMessage.textContent = '認証に失敗しました。';
                    pinInputs.forEach(input => input.disabled = true);
                    document.getElementById('auth-submit-button').disabled = true;
                } else {
                    if (authErrorMessage) authErrorMessage.textContent = '正しい認証コードを入力してください。';
                }
            }
        } catch (error) {
            console.error('Error during PIN verification:', error);
            if (authErrorMessage) authErrorMessage.textContent = '認証中にエラーが発生しました。';
        } finally {
            setLoading(false);
        }
    }

    async function logout() {
        await AuthManager.clearAuthData();
        showAuthScreen();
        console.log('Logged out successfully');
        try {
            await fetchWithAuth('/api/auth/logout', { method: 'POST' });
        } catch (e) { /* Ignore */ }
    }

    function setLoading(isLoading) {
        if (authLoadingSpinner) authLoadingSpinner.style.display = isLoading ? 'block' : 'none';
        const submitBtn = document.getElementById('auth-submit-button');
        if (submitBtn) submitBtn.style.display = isLoading ? 'none' : 'block';
    }

    // --- Existing Dashboard Functions (Now using fetchWithAuth) ---

    async function fetchDataAndRender() {
        try {
            const response = await fetchWithAuth('/api/data');
            const data = await response.json();
            renderAllData(data);
        } catch (error) {
            if (error.message !== 'Authentication required') {
                console.error("Failed to fetch data:", error);
                document.getElementById('dashboard-content').innerHTML =
                    `<div class="card"><p>データの読み込みに失敗しました: ${error.message}</p></div>`;
            }
        }
    }

    // --- Service Worker Registration ---
    if ('serviceWorker' in navigator) {
        window.addEventListener('load', () => {
            navigator.serviceWorker.register('/sw.js')
                .then(reg => console.log('Service Worker registered.', reg))
                .catch(err => console.log('Service Worker registration failed: ', err));
        });
    }

    // --- Tab-switching logic ---
    function initTabs() {
        const tabContainer = document.querySelector('.tab-container');
        tabContainer.addEventListener('click', (e) => {
            if (!e.target.matches('.tab-button')) return;
            const targetTab = e.target.dataset.tab;
            document.querySelectorAll('.tab-button').forEach(b => b.classList.toggle('active', b.dataset.tab === targetTab));
            document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === `${targetTab}-content`));
            setTimeout(() => window.scrollTo(0, 0), 0);
        });
    }

    // --- Date Formatting Helper ---
    function formatDateForDisplay(dateInput) {
        if (!dateInput) return '';
        try {
            const date = new Date(dateInput);
            if (isNaN(date.getTime())) return '';
            return `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${String(date.getHours()).padStart(2, '0')}:${String(date.getMinutes()).padStart(2, '0')}`;
        } catch (e) { return ''; }
    }

    // --- Rendering Functions (Unchanged) ---
    function renderLightweightChart(containerId, data, title) {
        const container = document.getElementById(containerId);
        if (!container || !data || data.length === 0) {
            container.innerHTML = `<p>Chart data for ${title} is not available.</p>`;
            return;
        }
        container.innerHTML = '';
        const chart = LightweightCharts.createChart(container, { width: container.clientWidth, height: 300, layout: { backgroundColor: '#ffffff', textColor: '#333333' }, grid: { vertLines: { color: '#e1e1e1' }, horzLines: { color: '#e1e1e1' } }, crosshair: { mode: LightweightCharts.CrosshairMode.Normal }, timeScale: { borderColor: '#cccccc', timeVisible: true, secondsVisible: false }, handleScroll: false, handleScale: false });
        const candlestickSeries = chart.addSeries(LightweightCharts.CandlestickSeries, { upColor: '#26a69a', downColor: '#ef5350', borderDownColor: '#ef5350', borderUpColor: '#26a69a', wickDownColor: '#ef5350', wickUpColor: '#26a69a' });
        const chartData = data.map(item => ({ time: (new Date(item.time).getTime() / 1000), open: item.open, high: item.high, low: item.low, close: item.close }));
        candlestickSeries.setData(chartData);
        chart.timeScale().fitContent();
        new ResizeObserver(entries => { if (entries.length > 0 && entries[0].contentRect.width > 0) { chart.applyOptions({ width: entries[0].contentRect.width }); } }).observe(container);
    }
    function renderMarketOverview(container, marketData, lastUpdated) {
        if (!container) return;
        container.innerHTML = '';
        const card = document.createElement('div');
        card.className = 'card';
        let content = '';
        if (marketData.fear_and_greed) { content += `<div class="market-section"><h3>Fear & Greed Index</h3><div class="fg-container" style="display: flex; justify-content: center; align-items: center; min-height: 400px;"><img src="/fear_and_greed_gauge.png?v=${new Date().getTime()}" alt="Fear and Greed Index Gauge" style="max-width: 100%; height: auto;"></div></div>`; }
        content += `<div class="market-grid"><div class="market-section"><h3>VIX (4h足)</h3><div class="chart-container" id="vix-chart-container"></div></div><div class="market-section"><h3>米国10年債金利 (4h足)</h3><div class="chart-container" id="t-note-chart-container"></div></div></div>`;
        if (marketData.ai_commentary) { const dateHtml = formatDateForDisplay(lastUpdated) ? `<p class="ai-date">${formatDateForDisplay(lastUpdated)}</p>` : ''; content += `<div class="market-section"><div class="ai-header"><h3>AI解説</h3>${dateHtml}</div><p>${marketData.ai_commentary.replace(/\n/g, '<br>')}</p></div>`; }
        card.innerHTML = content;
        container.appendChild(card);
        if (marketData.vix && marketData.vix.history) { renderLightweightChart('vix-chart-container', marketData.vix.history, 'VIX'); }
        if (marketData.t_note_future && marketData.t_note_future.history) { renderLightweightChart('t-note-chart-container', marketData.t_note_future.history, '10y T-Note'); }
    }
    function renderNews(container, newsData, lastUpdated) {
        if (!container || !newsData || (!newsData.summary && (!newsData.topics || newsData.topics.length === 0))) { container.innerHTML = '<div class="card"><p>ニュースデータがありません。</p></div>'; return; }
        container.innerHTML = '';
        const card = document.createElement('div');
        card.className = 'card news-card';
        if (newsData.summary) {
            const summaryContainer = document.createElement('div');
            summaryContainer.className = 'news-summary';
            const summaryHeader = document.createElement('div');
            summaryHeader.className = 'news-summary-header';
            let title = '<h3>今朝のサマリー</h3>';
            let dateString = '';
            if (lastUpdated) { const date = new Date(lastUpdated); if (date.getDay() === 1) title = '<h3>先週のサマリー</h3>'; dateString = `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${date.getHours()}:${String(date.getMinutes()).padStart(2, '0')}`; }
            summaryHeader.innerHTML = `${title}<p class="summary-date">${dateString}</p>`;
            const summaryBody = document.createElement('div');
            summaryBody.className = 'news-summary-body';
            summaryBody.innerHTML = `<p>${newsData.summary.replace(/\n/g, '<br>')}</p><img src="icons/suit.PNG" alt="suit" class="summary-image">`;
            summaryContainer.appendChild(summaryHeader);
            summaryContainer.appendChild(summaryBody);
            card.appendChild(summaryContainer);
        }
        if (newsData.topics && newsData.topics.length > 0) {
            const topicsOuterContainer = document.createElement('div');
            topicsOuterContainer.className = 'main-topics-outer-container';
            topicsOuterContainer.innerHTML = '<h3>主要トピック</h3>';
            const topicsContainer = document.createElement('div');
            topicsContainer.className = 'main-topics-container';
            newsData.topics.forEach((topic, index) => {
                const topicBox = document.createElement('div');
                topicBox.className = `topic-box topic-${index + 1}`;
                const topicContent = topic.analysis ? `<p>${topic.analysis.replace(/\n/g, '<br>')}</p>` : `<p>${topic.body}</p>`;
                const sourceIcon = `<a href="${topic.url}" target="_blank" class="source-link"><img src="${topic.source_icon_url || 'icons/external-link.svg'}" alt="Source" class="source-icon" onerror="this.onerror=null;this.src='icons/external-link.svg';"></a>`;
                topicBox.innerHTML = `<div class="topic-number-container"><div class="topic-number">${index + 1}</div></div><div class="topic-details"><p class="topic-title">${topic.title}</p><div class="topic-content">${topicContent}${sourceIcon}</div></div>`;
                topicsContainer.appendChild(topicBox);
            });
            topicsOuterContainer.appendChild(topicsContainer);
            card.appendChild(topicsOuterContainer);
        }
        container.appendChild(card);
    }
    function getPerformanceColor(p) { if (p >= 3) return '#00c853'; if (p > 1) return '#66bb6a'; if (p > 0) return '#2e7d32'; if (p == 0) return '#888888'; if (p > -1) return '#e53935'; if (p > -3) return '#ef5350'; return '#c62828'; }
    function renderGridHeatmap(container, title, heatmapData) {
        if (!container) return;
        container.innerHTML = '';
        let items = heatmapData?.items || heatmapData?.stocks || [];
        const isSP500 = title.includes('SP500');
        if (items.length > 0) {
            if (isSP500) { const stocks = items.filter(d => d.market_cap).sort((a, b) => b.market_cap - a.market_cap).slice(0, 30); const etfs = items.filter(d => !d.market_cap); items = [...stocks, ...etfs]; }
            else { items.sort((a, b) => b.market_cap - a.market_cap); items = items.slice(0, 30); }
        }
        if (items.length === 0) return;
        const card = document.createElement('div');
        card.className = 'card';
        const heatmapWrapper = document.createElement('div');
        heatmapWrapper.className = 'heatmap-wrapper';
        heatmapWrapper.innerHTML = `<h2 class="heatmap-main-title">${title}</h2>`;
        const itemsPerRow = 6, margin = { top: 10, right: 10, bottom: 10, left: 10 }, containerWidth = container.clientWidth || 1000, width = containerWidth - margin.left - margin.right, tilePadding = 5, tileWidth = (width - (itemsPerRow - 1) * tilePadding) / itemsPerRow, tileHeight = tileWidth, etfGap = isSP500 ? tileHeight * 0.5 : 0;
        let yPos = 0; const yPositions = [];
        for (let i = 0; i < items.length; i++) { if (isSP500 && i === items.findIndex(d => !d.market_cap)) { if (i % itemsPerRow !== 0) yPos += tileHeight + tilePadding; yPos += etfGap; } yPositions.push(yPos); if ((i + 1) % itemsPerRow === 0 && i + 1 < items.length) yPos += tileHeight + tilePadding; }
        const totalHeight = yPos + tileHeight;
        const svg = d3.create("svg").attr("viewBox", `0 0 ${containerWidth} ${totalHeight + margin.top + margin.bottom}`).attr("width", "100%").attr("height", "auto");
        const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);
        const tooltip = d3.select("body").append("div").attr("class", "heatmap-tooltip").style("opacity", 0);
        const nodes = g.selectAll("g").data(items).enter().append("g").attr("transform", (d, i) => `translate(${(i % itemsPerRow) * (tileWidth + tilePadding)},${yPositions[i]})`);
        nodes.append("rect").attr("width", tileWidth).attr("height", tileHeight).attr("fill", d => getPerformanceColor(d.performance)).on("mouseover", (e, d) => { tooltip.transition().duration(200).style("opacity", .9); tooltip.html(`<strong>${d.ticker}</strong><br/>Perf: ${d.performance.toFixed(2)}%`).style("left", `${e.pageX + 5}px`).style("top", `${e.pageY - 28}px`); }).on("mouseout", () => tooltip.transition().duration(500).style("opacity", 0));
        const text = nodes.append("text").attr("class", "stock-label").attr("x", tileWidth / 2).attr("y", tileHeight / 2).attr("text-anchor", "middle").attr("dominant-baseline", "central").style("pointer-events", "none");
        text.append("tspan").attr("class", "ticker-label").style("font-size", `${Math.max(10, Math.min(tileWidth / 3, 24)) * 1.5}px`).text(d => d.ticker);
        text.append("tspan").attr("class", "performance-label").attr("x", tileWidth / 2).attr("dy", "1.2em").style("font-size", `${Math.max(8, Math.min(tileWidth / 4, 18)) * 1.5}px`).text(d => `${d.performance.toFixed(2)}%`);
        heatmapWrapper.appendChild(svg.node());
        card.appendChild(heatmapWrapper);
        container.appendChild(card);
    }
    function renderIndicators(container, indicatorsData) {
        if (!container) return;
        container.innerHTML = '';
        const { economic = [], us_earnings = [], jp_earnings = [], economic_commentary, earnings_commentary } = indicatorsData || {};
        const economicCard = document.createElement('div');
        economicCard.className = 'card';
        economicCard.innerHTML = '<h3>経済指標カレンダー (重要度★★以上)</h3>';
        const relevantIndicators = economic.filter(ind => (ind.importance?.match(/★/g) || []).length >= 2);
        if (relevantIndicators.length > 0) { const table = document.createElement('table'); table.className = 'indicators-table'; table.innerHTML = `<thead><tr><th>発表日</th><th>発表時刻</th><th>指標名</th><th>重要度</th><th>前回</th><th>予測</th></tr></thead>`; const tbody = document.createElement('tbody'); relevantIndicators.forEach(ind => { const row = document.createElement('tr'); const [date, time] = (ind.datetime || ' / ').split(' '); row.innerHTML = `<td>${date||'--'}</td><td>${time||'--'}</td><td>${ind.name||'--'}</td><td class="importance-${(ind.importance.match(/★/g)||[]).length}">${ind.importance}</td><td>${ind.previous||'--'}</td><td>${ind.forecast||'--'}</td>`; tbody.appendChild(row); }); table.appendChild(tbody); economicCard.appendChild(table); } else { economicCard.innerHTML += '<p>予定されている重要経済指標はありません。</p>'; }
        if (economic_commentary) { const div = document.createElement('div'); div.className = 'ai-commentary'; div.innerHTML = `<div class="ai-header"><h3>AI解説</h3></div><p>${economic_commentary.replace(/\n/g, '<br>')}</p>`; economicCard.appendChild(div); }
        container.appendChild(economicCard);
        const allEarnings = [...us_earnings, ...jp_earnings].sort((a,b) => (a.datetime||'').localeCompare(b.datetime||''));
        const earningsCard = document.createElement('div');
        earningsCard.className = 'card';
        earningsCard.innerHTML = '<h3>注目決算</h3>';
        if (allEarnings.length > 0) { const table = document.createElement('table'); table.className = 'indicators-table'; table.innerHTML = `<thead><tr><th>発表日時</th><th>ティッカー</th><th>企業名</th></tr></thead>`; const tbody = document.createElement('tbody'); allEarnings.forEach(e => { const row = document.createElement('tr'); row.innerHTML = `<td>${e.datetime||'--'}</td><td>${e.ticker||'--'}</td><td>${e.company||''}</td>`; tbody.appendChild(row); }); table.appendChild(tbody); earningsCard.appendChild(table); } else { earningsCard.innerHTML += '<p>予定されている注目決算はありません。</p>'; }
        if (earnings_commentary) { const div = document.createElement('div'); div.className = 'ai-commentary'; div.innerHTML = `<div class="ai-header"><h3>AI解説</h3></div><p>${earnings_commentary.replace(/\n/g, '<br>')}</p>`; earningsCard.appendChild(div); }
        container.appendChild(earningsCard);
    }
    function renderColumn(container, columnData) {
        if (!container) return;
        container.innerHTML = '';
        if (typeof columnData === 'string') { container.innerHTML = `<div class="card"><p>${columnData}</p></div>`; return; }
        const report = columnData ? (columnData.daily_report || columnData.weekly_report) : null;
        if (report && report.content) { const card = document.createElement('div'); card.className = 'card'; const dateHtml = formatDateForDisplay(report.date) ? `<p class="ai-date">${formatDateForDisplay(report.date)}</p>` : ''; card.innerHTML = `<div class="column-container"><div class="ai-header"><h3>${report.title || 'AI解説'}</h3>${dateHtml}</div><div class="column-content">${report.content.replace(/\n/g, '<br>')}</div></div>`; container.appendChild(card); }
        else { container.innerHTML = `<div class="card"><p>${report && report.error ? '生成が失敗しました。' : 'AI解説はまだありません。（月曜日に週間分、火〜金曜日に当日分が生成されます）'}</p></div>`; }
    }
    function renderHeatmapCommentary(container, commentary, lastUpdated) {
        if (!container || !commentary) return;
        const card = document.createElement('div');
        card.className = 'card';
        const dateHtml = formatDateForDisplay(lastUpdated) ? `<p class="ai-date">${formatDateForDisplay(lastUpdated)}</p>` : '';
        card.innerHTML = `<div class="ai-commentary"><div class="ai-header"><h3>AI解説</h3>${dateHtml}</div><p>${commentary.replace(/\n/g, '<br>')}</p></div>`;
        container.appendChild(card);
    }
    function renderAllData(data) {
        console.log("Rendering all data:", data);
        const lastUpdatedEl = document.getElementById('last-updated');
        if (lastUpdatedEl && data.last_updated) { lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`; }
        renderMarketOverview(document.getElementById('market-content'), data.market, data.last_updated);
        renderNews(document.getElementById('news-content'), data.news, data.last_updated);
        renderGridHeatmap(document.getElementById('nasdaq-heatmap-1d'), 'Nasdaq (1-Day)', data.nasdaq_heatmap_1d);
        renderGridHeatmap(document.getElementById('nasdaq-heatmap-1w'), 'Nasdaq (1-Week)', data.nasdaq_heatmap_1w);
        renderGridHeatmap(document.getElementById('nasdaq-heatmap-1m'), 'Nasdaq (1-Month)', data.nasdaq_heatmap_1m);
        renderHeatmapCommentary(document.getElementById('nasdaq-commentary'), data.nasdaq_heatmap?.ai_commentary, data.last_updated);
        renderGridHeatmap(document.getElementById('sp500-heatmap-1d'), 'SP500 & Sector ETFs (1-Day)', data.sp500_combined_heatmap_1d);
        renderGridHeatmap(document.getElementById('sp500-heatmap-1w'), 'SP500 & Sector ETFs (1-Week)', data.sp500_combined_heatmap_1w);
        renderGridHeatmap(document.getElementById('sp500-heatmap-1m'), 'SP500 & Sector ETFs (1-Month)', data.sp500_combined_heatmap_1m);
        renderHeatmapCommentary(document.getElementById('sp500-commentary'), data.sp500_heatmap?.ai_commentary, data.last_updated);
        renderIndicators(document.getElementById('indicators-content'), data.indicators, data.last_updated);
        renderColumn(document.getElementById('column-content'), data.column);
    }

    // --- Swipe Navigation ---
    function initSwipeNavigation() {
        const contentArea = document.getElementById('dashboard-content');
        let touchstartX = 0, touchendX = 0;
        contentArea.addEventListener('touchstart', e => { touchstartX = e.changedTouches[0].screenX; }, { passive: true });
        contentArea.addEventListener('touchend', e => {
            touchendX = e.changedTouches[0].screenX;
            if (Math.abs(touchendX - touchstartX) < 100) return;
            const tabButtons = Array.from(document.querySelectorAll('.tab-button'));
            const currentIndex = tabButtons.findIndex(b => b.classList.contains('active'));
            let nextIndex = (touchendX > touchstartX) ? currentIndex - 1 : currentIndex + 1;
            if (nextIndex < 0) nextIndex = tabButtons.length - 1;
            else if (nextIndex >= tabButtons.length) nextIndex = 0;
            tabButtons[nextIndex]?.click();
        });
    }

    // --- App Initialization ---
    initializeApp();
});

class NotificationManager {
    constructor() {
        this.isSupported = 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
        this.vapidPublicKey = null;
        this.isInitialized = false;
        this.subscription = null;
    }

    async init() {
        if (!this.isSupported) {
            console.log('Push notifications are not supported');
            this.updateUI('非対応', false);
            return;
        }

        // 認証されていない場合はスキップ
        if (!AuthManager.isAuthenticated()) {
            console.log('Not authenticated, skipping push notification setup');
            return;
        }

        console.log('Initializing NotificationManager...');

        // VAPID公開鍵を取得（認証不要）
        try {
            const response = await fetch('/api/vapid-public-key');
            const data = await response.json();
            this.vapidPublicKey = data.public_key;
            console.log('VAPID public key obtained');
        } catch (error) {
            console.error('Failed to get VAPID public key:', error);
            this.updateUI('エラー', false);
            return;
        }

        // 現在の通知権限を確認
        await this.checkNotificationStatus();

        // Service Workerメッセージリスナー（一度だけ登録）
        if (!this.isInitialized) {
            this.setupServiceWorkerListener();
            this.isInitialized = true;
        }
    }

    async checkNotificationStatus() {
        const permission = Notification.permission;
        console.log('Current notification permission:', permission);

        if (permission === 'granted') {
            // すでに許可されている場合は自動で登録
            await this.setupPushSubscription();
            this.updateUI('有効', true);
        } else if (permission === 'denied') {
            this.updateUI('拒否（設定から変更してください）', false);
        } else {
            // デフォルトの場合は手動設定ボタンを表示
            this.showSetupButton();
            this.updateUI('未設定', false);
        }
    }

    showSetupButton() {
        const btn = document.getElementById('notification-setup-btn');
        if (btn) {
            btn.style.display = 'inline-block';
            btn.onclick = () => this.requestPermissionManually();
        }
    }

    async requestPermissionManually() {
        console.log('Manual permission request initiated');

        // iOS PWAの場合、ユーザーインタラクションから呼ばれるので成功しやすい
        try {
            const permission = await Notification.requestPermission();
            console.log('Permission result:', permission);

            if (permission === 'granted') {
                await this.setupPushSubscription();
                this.updateUI('有効', true);

                // ボタンを隠す
                const btn = document.getElementById('notification-setup-btn');
                if (btn) btn.style.display = 'none';

                // テスト通知
                new Notification('HanaView', {
                    body: '通知が有効になりました',
                    icon: '/icons/icon-192x192.png'
                });
            } else {
                this.updateUI('拒否されました', false);
            }
        } catch (error) {
            console.error('Permission request failed:', error);
            this.updateUI('エラーが発生しました', false);
        }
    }

    async setupPushSubscription() {
        try {
            console.log('Setting up push subscription...');

            // Service Workerの準備を待つ
            const registration = await navigator.serviceWorker.ready;
            console.log('Service Worker ready');

            // 既存のサブスクリプションを確認
            let subscription = await registration.pushManager.getSubscription();
            console.log('Existing subscription:', subscription);

            if (subscription) {
                // 既存のものがあれば一度解除して新規作成
                console.log('Unsubscribing old subscription...');
                await subscription.unsubscribe();
            }

            // 新規サブスクリプション作成
            console.log('Creating new subscription...');
            const convertedVapidKey = this.urlBase64ToUint8Array(this.vapidPublicKey);

            subscription = await registration.pushManager.subscribe({
                userVisibleOnly: true,
                applicationServerKey: convertedVapidKey
            });

            console.log('New subscription created:', subscription);
            this.subscription = subscription;

            // Service Workerにトークンを送信（重要！）
            await this.sendTokenToServiceWorker();

            // サーバーに送信
            await this.sendSubscriptionToServer(subscription);

            // Background syncの登録
            if ('sync' in registration) {
                await registration.sync.register('data-sync');
                console.log('Background sync registered');
            }

            // Periodic Background syncの登録
            if ('periodicSync' in registration) {
                const status = await navigator.permissions.query({ name: 'periodic-background-sync' });
                if (status.state === 'granted') {
                    try {
                        await registration.periodicSync.register('data-update', {
                            minInterval: 3600 * 1000, // 1時間ごと
                        });
                        console.log('Periodic background sync registered.');
                    } catch (error) {
                        console.error('Failed to register periodic sync:', error);
                    }
                }
            }

        } catch (error) {
            console.error('Failed to setup push subscription:', error);
            this.updateUI('登録エラー', false);
        }
    }

    async sendTokenToServiceWorker() {
        // Service WorkerにJWTトークンを送信
        const token = AuthManager.getToken();
        if (token && navigator.serviceWorker.controller) {
            navigator.serviceWorker.controller.postMessage({
                type: 'SET_TOKEN',
                token: token
            });
            console.log('Token sent to Service Worker');
        }
    }

    async sendSubscriptionToServer(subscription) {
        try {
            console.log('Sending subscription to server...');

            const response = await fetchWithAuth('/api/subscribe', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(subscription)
            });

            if (!response.ok) {
                throw new Error(`Server returned ${response.status}`);
            }

            const result = await response.json();
            console.log('Subscription registered:', result);

            // 成功をユーザーに通知
            this.updateUI('登録完了（毎朝6:30に通知）', true);

        } catch (error) {
            console.error('Failed to send subscription:', error);
            throw error;
        }
    }

    setupServiceWorkerListener() {
        navigator.serviceWorker.addEventListener('message', event => {
            if (event.data.type === 'data-updated') {
                console.log('Data updated notification received');
                if (typeof renderAllData === 'function' && event.data.data) {
                    renderAllData(event.data.data);
                }
                this.showInAppNotification('データが更新されました');
            }
        });
    }

    updateUI(status, isActive) {
        const statusEl = document.getElementById('notification-status');
        if (statusEl) {
            statusEl.textContent = `通知: ${status}`;
            statusEl.style.color = isActive ? '#4caf50' : '#757575';
        }
    }

    urlBase64ToUint8Array(base64String) {
        const padding = '='.repeat((4 - base64String.length % 4) % 4);
        const base64 = (base64String + padding)
            .replace(/\-/g, '+')
            .replace(/_/g, '/');

        const rawData = window.atob(base64);
        const outputArray = new Uint8Array(rawData.length);

        for (let i = 0; i < rawData.length; ++i) {
            outputArray[i] = rawData.charCodeAt(i);
        }
        return outputArray;
    }

    showInAppNotification(message) {
        const toast = document.createElement('div');
        toast.className = 'toast-notification';
        toast.textContent = message;
        toast.style.cssText = `
            position: fixed;
            bottom: 20px;
            right: 20px;
            background: #006B6B;
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 10000;
            animation: slideIn 0.3s ease-out;
        `;

        document.body.appendChild(toast);

        setTimeout(() => {
            toast.style.animation = 'slideOut 0.3s ease-out';
            setTimeout(() => {
                document.body.removeChild(toast);
            }, 300);
        }, 3000);
    }
}