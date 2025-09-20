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

    // --- Main App Logic ---

    async function initializeApp() {
        try {
            const response = await fetch('/api/auth/check');
            if (!response.ok) {
                // If the check fails for reasons other than 404 or 401, treat as unauthenticated
                console.error('Auth check failed with status:', response.status);
                showAuthScreen();
                return;
            }
            const data = await response.json();
            if (data.authenticated) {
                showDashboard();
            } else {
                showAuthScreen();
            }
        } catch (error) {
            console.error('Error during authentication check:', error);
            showAuthScreen(); // Default to showing auth screen on error
            if(authErrorMessage) authErrorMessage.textContent = 'サーバーとの通信に失敗しました。';
        }
    }

    function showDashboard() {
        if (authContainer) authContainer.style.display = 'none';
        if (dashboardContainer) dashboardContainer.style.display = 'block';

        // Initialize dashboard features only once
        if (!dashboardContainer.dataset.initialized) {
            console.log("HanaView Dashboard Initialized");
            initTabs();
            fetchDataAndRender();
            initSwipeNavigation();
            dashboardContainer.dataset.initialized = 'true';

            // Initialize notifications after dashboard is shown
            const notificationManager = new NotificationManager();
            notificationManager.init();
        }
    }

    function showAuthScreen() {
        if (authContainer) authContainer.style.display = 'flex';
        if (dashboardContainer) dashboardContainer.style.display = 'none';
        setupAuthForm();
    }

    function setupAuthForm() {
        if (!pinInputsContainer) return;

        pinInputs.forEach((input, index) => {
            input.addEventListener('input', () => {
                // Move to next input if a digit is entered
                if (input.value.length === 1 && index < pinInputs.length - 1) {
                    pinInputs[index + 1].focus();
                }
            });

            input.addEventListener('keydown', (e) => {
                // Move to previous input on backspace if current is empty
                if (e.key === 'Backspace' && input.value.length === 0 && index > 0) {
                    pinInputs[index - 1].focus();
                }
            });

            input.addEventListener('paste', (e) => {
                e.preventDefault();
                const pasteData = e.clipboardData.getData('text').trim();
                if (/^\d{6}$/.test(pasteData)) {
                    pasteData.split('').forEach((char, i) => {
                        if (pinInputs[i]) {
                            pinInputs[i].value = char;
                        }
                    });
                    pinInputs[pinInputs.length - 1].focus();
                    handleAuthSubmit(); // Automatically submit on successful paste
                }
            });
        });

        if (authSubmitButton) {
            authSubmitButton.addEventListener('click', handleAuthSubmit);
        }
    }

    async function handleAuthSubmit() {
        const pin = pinInputs.map(input => input.value).join('');

        if (pin.length !== 6) {
            if(authErrorMessage) authErrorMessage.textContent = '6桁のコードを入力してください。';
            return;
        }

        setLoading(true);

        try {
            const response = await fetch('/api/auth/verify', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pin: pin }),
            });

            if (response.ok) {
                showDashboard();
            } else {
                failedAttempts++;
                pinInputs.forEach(input => input.value = '');
                pinInputs[0].focus();

                if (failedAttempts >= MAX_ATTEMPTS) {
                    if(authErrorMessage) authErrorMessage.textContent = '認証に失敗しました。';
                    pinInputs.forEach(input => input.disabled = true);
                    if(authSubmitButton) authSubmitButton.disabled = true;
                } else {
                    if(authErrorMessage) authErrorMessage.textContent = '正しい認証コードを入力してください。';
                }
            }
        } catch (error) {
            console.error('Error during PIN verification:', error);
            if(authErrorMessage) authErrorMessage.textContent = '認証中にエラーが発生しました。';
        } finally {
            setLoading(false);
        }
    }

    function setLoading(isLoading) {
        if (authLoadingSpinner) authLoadingSpinner.style.display = isLoading ? 'block' : 'none';
        if (authSubmitButton) authSubmitButton.style.display = isLoading ? 'none' : 'block';
    }


    // --- Existing Dashboard Functions ---

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

            document.querySelectorAll('.tab-button').forEach(button => {
                button.classList.toggle('active', button.dataset.tab === targetTab);
            });
            document.querySelectorAll('.tab-pane').forEach(pane => {
                pane.classList.toggle('active', pane.id === `${targetTab}-content`);
            });

            // Scroll to the top of the page after a short delay.
            // This ensures the browser has processed the tab change before scrolling,
            // which is more reliable, especially after swipe gestures.
            setTimeout(() => {
                window.scrollTo(0, 0);
            }, 0);
        });
    }

    // --- Date Formatting Helper ---
    function formatDateForDisplay(dateInput) {
        if (!dateInput) return '';
        try {
            const date = new Date(dateInput);
            if (isNaN(date.getTime())) {
                console.error("Invalid date input for formatting:", dateInput);
                return '';
            }
            const year = date.getFullYear();
            const month = date.getMonth() + 1;
            const day = date.getDate();
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            return `${year}年${month}月${day}日 ${hours}:${minutes}`;
        } catch (e) {
            console.error("Error formatting date:", dateInput, e);
            return '';
        }
    }

    // --- Rendering Functions ---

    function renderLightweightChart(containerId, data, title) {
        const container = document.getElementById(containerId);
        if (!container || !data || data.length === 0) {
            container.innerHTML = `<p>Chart data for ${title} is not available.</p>`;
            return;
        }
        container.innerHTML = ''; // Clear previous content

        const chart = LightweightCharts.createChart(container, {
            width: container.clientWidth,
            height: 300, // Fixed height for chart
            layout: {
                backgroundColor: '#ffffff',
                textColor: '#333333',
            },
            grid: {
                vertLines: { color: '#e1e1e1' },
                horzLines: { color: '#e1e1e1' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            timeScale: {
                borderColor: '#cccccc',
                timeVisible: true,
                secondsVisible: false,
            },
            handleScroll: {
                mouseWheel: false,
                pressedMouseMove: false,
                horzTouchDrag: false,
                vertTouchDrag: false,
            },
            handleScale: {
                mouseWheel: false,
                pinch: false,
                axisPressedMouseMove: false,
                axisDoubleClickReset: false,
            },
        });

        const candlestickSeries = chart.addSeries(LightweightCharts.CandlestickSeries, {
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderDownColor: '#ef5350',
            borderUpColor: '#26a69a',
            wickDownColor: '#ef5350',
            wickUpColor: '#26a69a',
        });

        // Convert backend time string to UTC timestamp for the chart
        const chartData = data.map(item => ({
            time: (new Date(item.time).getTime() / 1000), // Convert to UNIX timestamp (seconds)
            open: item.open,
            high: item.high,
            low: item.low,
            close: item.close,
        }));

        candlestickSeries.setData(chartData);
        chart.timeScale().fitContent();

        // Handle resizing
        new ResizeObserver(entries => {
            if (entries.length > 0 && entries[0].contentRect.width > 0) {
                chart.applyOptions({ width: entries[0].contentRect.width });
            }
        }).observe(container);
    }

    function renderMarketOverview(container, marketData, lastUpdated) {
        if (!container) return;
        container.innerHTML = ''; // Clear content

        const card = document.createElement('div');
        card.className = 'card';

        let content = '';

        // Fear & Greed Index
        const fgData = marketData.fear_and_greed;
        if (fgData) {
            // Add a cache-busting query parameter
            const timestamp = new Date().getTime();
            content += `
                <div class="market-section">
                    <h3>Fear & Greed Index</h3>
                    <div class="fg-container" style="display: flex; justify-content: center; align-items: center; min-height: 400px;">
                        <img src="/fear_and_greed_gauge.png?v=${timestamp}" alt="Fear and Greed Index Gauge" style="max-width: 100%; height: auto;">
                    </div>
                </div>
            `;
        }

        // Lightweight Charts
        content += `
            <div class="market-grid">
                <div class="market-section">
                    <h3>VIX (4h足)</h3>
                    <div class="chart-container" id="vix-chart-container"></div>
                </div>
                <div class="market-section">
                    <h3>米国10年債金利 (4h足)</h3>
                    <div class="chart-container" id="t-note-chart-container"></div>
                </div>
            </div>
        `;

        // AI Commentary
        if (marketData.ai_commentary) {
            const formattedDate = formatDateForDisplay(lastUpdated);
            const dateHtml = formattedDate ? `<p class="ai-date">${formattedDate}</p>` : '';
            content += `
                <div class="market-section">
                    <div class="ai-header">
                        <h3>AI解説</h3>
                        ${dateHtml}
                    </div>
                    <p>${marketData.ai_commentary.replace(/\n/g, '<br>')}</p>
                </div>
            `;
        }

        card.innerHTML = content;
        container.appendChild(card);

        // Render lightweight charts
        if (marketData.vix && marketData.vix.history) {
            renderLightweightChart('vix-chart-container', marketData.vix.history, 'VIX');
        }
        if (marketData.t_note_future && marketData.t_note_future.history) {
            renderLightweightChart('t-note-chart-container', marketData.t_note_future.history, '10y T-Note');
        }
    }

    function renderNews(container, newsData, lastUpdated) {
        if (!container) return;
        container.innerHTML = '';
        if (!newsData || (!newsData.summary && (!newsData.topics || newsData.topics.length === 0))) {
            container.innerHTML = '<div class="card"><p>ニュースデータがありません。</p></div>';
            return;
        }

        const card = document.createElement('div');
        card.className = 'card news-card'; // Add news-card for specific styling

        // --- Summary Section ---
        if (newsData.summary) {
            const summaryContainer = document.createElement('div');
            summaryContainer.className = 'news-summary';

            // Title and Date
            const summaryHeader = document.createElement('div');
            summaryHeader.className = 'news-summary-header';

            let title = '<h3>今朝のサマリー</h3>';
            let dateString = '';
            if (lastUpdated) {
                const date = new Date(lastUpdated);
                // getDay() returns 0 for Sunday, 1 for Monday, etc.
                if (date.getDay() === 1) { // Monday
                    title = '<h3>先週のサマリー</h3>';
                }
                dateString = `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${date.getHours()}:${String(date.getMinutes()).padStart(2, '0')}`;
            }
            summaryHeader.innerHTML = `${title}<p class="summary-date">${dateString}</p>`;

            // Body and Image
            const summaryBody = document.createElement('div');
            summaryBody.className = 'news-summary-body';
            summaryBody.innerHTML = `<p>${newsData.summary.replace(/\n/g, '<br>')}</p>`;
            summaryBody.innerHTML += `<img src="icons/suit.PNG" alt="suit" class="summary-image">`;

            summaryContainer.appendChild(summaryHeader);
            summaryContainer.appendChild(summaryBody);
            card.appendChild(summaryContainer);
        }

        // --- Topics Section ---
        if (newsData.topics && newsData.topics.length > 0) {
            const topicsOuterContainer = document.createElement('div');
            topicsOuterContainer.className = 'main-topics-outer-container';
            topicsOuterContainer.innerHTML = '<h3>主要トピック</h3>';

            const topicsContainer = document.createElement('div');
            topicsContainer.className = 'main-topics-container';

            newsData.topics.forEach((topic, index) => {
                const topicBox = document.createElement('div');
                topicBox.className = `topic-box topic-${index + 1}`;

                let topicContent = '';
                if (topic.analysis && topic.url) {
                    topicContent = `<p>${topic.analysis.replace(/\n/g, '<br>')}</p>`;
                } else if (topic.body) {
                    topicContent = `<p>${topic.body}</p>`;
                } else {
                    topicContent = `
                        <p><strong>事実:</strong> ${topic.fact || 'N/A'}</p>
                        <p><strong>解釈:</strong> ${topic.interpretation || 'N/A'}</p>
                        <p><strong>市場への影響:</strong> ${topic.impact || 'N/A'}</p>
                    `;
                }

                // --- Icon Logic ---
                const iconUrl = topic.source_icon_url || 'icons/external-link.svg';
                const sourceIcon = `
                    <a href="${topic.url}" target="_blank" class="source-link">
                        <img src="${iconUrl}" alt="Source" class="source-icon" onerror="this.onerror=null;this.src='icons/external-link.svg';">
                    </a>
                `;

                topicBox.innerHTML = `
                    <div class="topic-number-container">
                        <div class="topic-number">${index + 1}</div>
                    </div>
                    <div class="topic-details">
                        <p class="topic-title">${topic.title}</p>
                        <div class="topic-content">
                            ${topicContent}
                            ${sourceIcon}
                        </div>
                    </div>
                `;
                topicsContainer.appendChild(topicBox);
            });
            topicsOuterContainer.appendChild(topicsContainer);
            card.appendChild(topicsOuterContainer);
        }

        container.appendChild(card);
    }

    function getPerformanceColor(performance) {
        // From bright to dark for positive performance
        if (performance >= 3) return '#00c853'; // bright green
        if (performance > 1) return '#66bb6a'; // light green
        if (performance > 0) return '#2e7d32'; // dark green

        if (performance == 0) return '#888888'; // grey

        // From dark to bright for negative performance
        if (performance > -1) return '#e53935'; // dark red
        if (performance > -3) return '#ef5350'; // light red
        return '#c62828'; // bright red
    }

    function renderGridHeatmap(container, title, heatmapData) {
        if (!container) return;
        container.innerHTML = '';

        let items = heatmapData?.items || heatmapData?.stocks || [];
        const isSP500 = title.includes('SP500');
        let etfStartIndex = -1;

        if (isSP500 && items.length > 0) {
            const stocks = items.filter(d => d.market_cap);
            const etfs = items.filter(d => !d.market_cap);
            stocks.sort((a, b) => b.market_cap - a.market_cap);
            const top30Stocks = stocks.slice(0, 30);
            items = [...top30Stocks, ...etfs];
            etfStartIndex = top30Stocks.length;
        } else if (!isSP500 && items.length > 0) { // For Nasdaq
            items.sort((a, b) => b.market_cap - a.market_cap);
            items = items.slice(0, 30); // Get top 30
        }

        if (items.length === 0) {
            return;
        }

        const card = document.createElement('div');
        card.className = 'card';
        const heatmapWrapper = document.createElement('div');
        heatmapWrapper.className = 'heatmap-wrapper';
        heatmapWrapper.innerHTML = `<h2 class="heatmap-main-title">${title}</h2>`;

        const numItems = items.length;
        const itemsPerRow = 6; // Set to 6 for both

        const margin = { top: 10, right: 10, bottom: 10, left: 10 };
        const containerWidth = container.clientWidth || 1000;
        const width = containerWidth - margin.left - margin.right;

        const tilePadding = 5;
        const tileWidth = (width - (itemsPerRow - 1) * tilePadding) / itemsPerRow;
        const tileHeight = tileWidth; // Make it square
        const etfGap = isSP500 ? tileHeight * 0.5 : 0; // Gap for SP500 chart

        // Calculate total height dynamically
        let totalHeight = 0;
        let yPos = 0;
        const yPositions = []; // Store y position for each item

        for (let i = 0; i < numItems; i++) {
            // Force a new row for the first ETF
            if (isSP500 && i === etfStartIndex) {
                // If the first ETF is not at the start of a row, move to next row
                if (i % itemsPerRow !== 0) {
                    yPos += tileHeight + tilePadding;
                }
                yPos += etfGap; // Add the gap before the ETF row
            }
            yPositions.push(yPos);
            // Move to next row
            if ((i + 1) % itemsPerRow === 0 && i + 1 < numItems) {
                yPos += tileHeight + tilePadding;
            }
        }
        totalHeight = yPos + tileHeight; // Add height of the last row


        const svg = d3.create("svg")
            .attr("viewBox", `0 0 ${containerWidth} ${totalHeight + margin.top + margin.bottom}`)
            .attr("width", "100%")
            .attr("height", "auto")
            .style("font-family", "sans-serif");

        const g = svg.append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        const tooltip = d3.select("body").append("div")
            .attr("class", "heatmap-tooltip")
            .style("opacity", 0);

        const nodes = g.selectAll("g")
            .data(items)
            .enter()
            .append("g")
            .attr("transform", (d, i) => {
                const col = i % itemsPerRow;
                const x = col * (tileWidth + tilePadding);
                const y = yPositions[i];
                return `translate(${x},${y})`;
            });

        nodes.append("rect")
            .attr("width", tileWidth)
            .attr("height", tileHeight)
            .attr("fill", d => getPerformanceColor(d.performance))
            .on("mouseover", (event, d) => {
                tooltip.transition().duration(200).style("opacity", .9);
                tooltip.html(`<strong>${d.ticker}</strong><br/>Perf: ${d.performance.toFixed(2)}%`)
                    .style("left", (event.pageX + 5) + "px")
                    .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseout", () => {
                tooltip.transition().duration(500).style("opacity", 0);
            });

        const text = nodes.append("text")
            .attr("class", "stock-label")
            .attr("x", tileWidth / 2)
            .attr("y", tileHeight / 2)
            .attr("text-anchor", "middle")
            .attr("dominant-baseline", "central")
            .style("pointer-events", "none");

        // Dynamically adjust font size based on tile width
        const tickerFontSize = Math.max(10, Math.min(tileWidth / 3, 24)) * 1.5;
        const perfFontSize = Math.max(8, Math.min(tileWidth / 4, 18)) * 1.5;

        text.append("tspan")
            .attr("class", "ticker-label")
            .style("font-size", `${tickerFontSize}px`)
            .text(d => d.ticker);

        text.append("tspan")
            .attr("class", "performance-label")
            .attr("x", tileWidth / 2)
            .attr("dy", "1.2em")
            .style("font-size", `${perfFontSize}px`)
            .text(d => `${d.performance.toFixed(2)}%`);

        heatmapWrapper.appendChild(svg.node());
        card.appendChild(heatmapWrapper);
        container.appendChild(card);
    }

    function renderIndicators(container, indicatorsData, lastUpdated) {
        if (!container) return;
        container.innerHTML = ''; // Clear previous content

        const indicators = indicatorsData || {};
        const economicIndicators = indicators.economic || [];
        const usEarnings = indicators.us_earnings || [];
        const jpEarnings = indicators.jp_earnings || [];

        // --- Time-based filtering logic ---
        // lastUpdated is expected to be a JST string like "2025-09-10T07:59:24.957729+09:00"
        const now = lastUpdated ? new Date(lastUpdated) : new Date();
        const year = now.getFullYear();

        let startTime = new Date(now);
        startTime.setHours(7, 0, 0, 0); // Set to 07:00:00.000 JST today

        // If current time is before 7 AM JST, the window is from yesterday 7 AM to today 7 AM.
        if (now.getHours() < 7) {
            startTime.setDate(startTime.getDate() - 1);
        }

        let endTime = new Date(startTime);
        endTime.setDate(endTime.getDate() + 1); // This correctly sets it to the next day at 07:00 JST

        const parseDateTime = (dateTimeStr) => {
            if (!dateTimeStr || !/^\d{2}\/\d{2} \d{2}:\d{2}$/.test(dateTimeStr)) {
                return null; // Invalid format
            }
            const [datePart, timePart] = dateTimeStr.split(' ');
            const [month, day] = datePart.split('/');
            const [hour, minute] = timePart.split(':');
            // Note: month is 0-indexed in JS Date
            return new Date(year, parseInt(month) - 1, parseInt(day), parseInt(hour), parseInt(minute));
        };

        // --- Part 1: Economic Calendar (High Importance) ---
        const economicCard = document.createElement('div');
        economicCard.className = 'card';
        economicCard.innerHTML = '<h3>経済指標カレンダー (重要度★★以上)</h3>';

        const todaysIndicators = economicIndicators.filter(ind => {
            const importanceOk = typeof ind.importance === 'string' && (ind.importance.match(/★/g) || []).length >= 2;
            if (!importanceOk) return false;

            const eventTime = parseDateTime(ind.datetime);
            if (!eventTime) return false;

            // Handle year change for events in early January when 'now' is in late December
            if (now.getMonth() === 11 && eventTime.getMonth() === 0) {
              eventTime.setFullYear(year + 1);
            }
            // Handle year change for events in late December when 'now' is in early January
            else if (now.getMonth() === 0 && eventTime.getMonth() === 11) {
              eventTime.setFullYear(year - 1);
            }

            return eventTime >= startTime && eventTime < endTime;
        });

        if (todaysIndicators.length > 0) {
            const table = document.createElement('table');
            table.className = 'indicators-table';
            table.innerHTML = `
                <thead>
                    <tr>
                        <th>発表日</th>
                        <th>発表時刻</th>
                        <th>指標名</th>
                        <th>重要度</th>
                        <th>前回</th>
                        <th>予測</th>
                    </tr>
                </thead>
            `;
            const tbody = document.createElement('tbody');
            todaysIndicators.forEach(ind => {
                const row = document.createElement('tr');
                const starCount = (ind.importance.match(/★/g) || []).length;
                const importanceStars = '★'.repeat(starCount);
                const [date, time] = (ind.datetime || ' / ').split(' ');
                row.innerHTML = `
                    <td>${date || '--'}</td>
                    <td>${time || '--'}</td>
                    <td>${ind.name || '--'}</td>
                    <td class="importance-${starCount}">${importanceStars}</td>
                    <td>${ind.previous || '--'}</td>
                    <td>${ind.forecast || '--'}</td>
                `;
                tbody.appendChild(row);
            });
            table.appendChild(tbody);
            economicCard.appendChild(table);
        } else {
            economicCard.innerHTML += '<p>本日予定されている重要経済指標はありません。</p>';
        }

        // --- AI Commentary for Economic Indicators ---
        if (indicators.economic_commentary) {
            const commentaryDiv = document.createElement('div');
            commentaryDiv.className = 'ai-commentary'; // Reuse existing style
            commentaryDiv.innerHTML = `
                <div class="ai-header">
                    <h3>AI解説</h3>
                </div>
                <p>${indicators.economic_commentary.replace(/\n/g, '<br>')}</p>
            `;
            economicCard.appendChild(commentaryDiv);
        }
        container.appendChild(economicCard);

        // --- Part 2: Earnings Announcements ---
        const allEarnings = [...usEarnings, ...jpEarnings];

        const todaysEarnings = allEarnings.filter(earning => {
            const eventTime = parseDateTime(earning.datetime);
             if (!eventTime) return false;

            // Handle year change for events in early January when 'now' is in late December
            if (now.getMonth() === 11 && eventTime.getMonth() === 0) {
              eventTime.setFullYear(year + 1);
            }
            // Handle year change for events in late December when 'now' is in early January
            else if (now.getMonth() === 0 && eventTime.getMonth() === 11) {
              eventTime.setFullYear(year - 1);
            }

            return eventTime && eventTime >= startTime && eventTime < endTime;
        });

        // Sort by datetime.
        todaysEarnings.sort((a, b) => {
            const timeA = parseDateTime(a.datetime);
            const timeB = parseDateTime(b.datetime);
            if (!timeA) return 1;
            if (!timeB) return -1;
            return timeA - timeB;
        });

        const earningsCard = document.createElement('div');
        earningsCard.className = 'card';
        earningsCard.innerHTML = '<h3>注目決算</h3>';

        if (todaysEarnings.length > 0) {
            const earningsTable = document.createElement('table');
            earningsTable.className = 'indicators-table'; // reuse style
            earningsTable.innerHTML = `
                <thead>
                    <tr>
                        <th>発表日時</th>
                        <th>ティッカー</th>
                        <th>企業名</th>
                    </tr>
                </thead>
            `;
            const tbody = document.createElement('tbody');
            todaysEarnings.forEach(earning => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${earning.datetime || '--'}</td>
                    <td>${earning.ticker || '--'}</td>
                    <td>${earning.company || ''}</td>
                `;
                tbody.appendChild(row);
            });
            earningsTable.appendChild(tbody);
            earningsCard.appendChild(earningsTable);
        } else {
            earningsCard.innerHTML += '<p>今日予定されている注目決算はありません。</p>';
        }

        // --- AI Commentary for Earnings ---
        if (indicators.earnings_commentary) {
            const commentaryDiv = document.createElement('div');
            commentaryDiv.className = 'ai-commentary'; // Reuse existing style
            commentaryDiv.innerHTML = `
                <div class="ai-header">
                    <h3>AI解説</h3>
                </div>
                <p>${indicators.earnings_commentary.replace(/\n/g, '<br>')}</p>
            `;
            earningsCard.appendChild(commentaryDiv);
        }
        container.appendChild(earningsCard);
    }

    function renderColumn(container, columnData) {
        if (!container) return;
        container.innerHTML = '';

        // Case 0: columnData itself is an error string.
        if (typeof columnData === 'string') {
            container.innerHTML = `<div class="card"><p>${columnData}</p></div>`;
            return;
        }

        const report = columnData ? (columnData.daily_report || columnData.weekly_report) : null;

        // Case 1: Success - content is available
        if (report && report.content) {
            const card = document.createElement('div');
            card.className = 'card';
            const formattedDate = formatDateForDisplay(report.date);
            const dateHtml = formattedDate ? `<p class="ai-date">${formattedDate}</p>` : '';
            card.innerHTML = `
                <div class="column-container">
                    <div class="ai-header">
                        <h3>${report.title || 'AI解説'}</h3>
                        ${dateHtml}
                    </div>
                    <div class="column-content">
                        ${report.content.replace(/\n/g, '<br>')}
                    </div>
                </div>
            `;
            container.appendChild(card);
        // Case 2: Failure - an error is reported inside the report object
        } else if (report && report.error) {
            container.innerHTML = '<div class="card"><p>生成が失敗しました。</p></div>';
        // Case 3: Not yet generated
        } else {
            container.innerHTML = '<div class="card"><p>AI解説はまだありません。（月曜日に週間分、火〜金曜日に当日分が生成されます）</p></div>';
        }
    }

    function renderHeatmapCommentary(container, commentary, lastUpdated) {
        if (!container || !commentary) return;

        // Create a wrapper card for the commentary
        const card = document.createElement('div');
        card.className = 'card';

        const formattedDate = formatDateForDisplay(lastUpdated);
        const dateHtml = formattedDate ? `<p class="ai-date">${formattedDate}</p>` : '';

        const commentaryDiv = document.createElement('div');
        commentaryDiv.className = 'ai-commentary';
        commentaryDiv.innerHTML = `
            <div class="ai-header">
                <h3>AI解説</h3>
                ${dateHtml}
            </div>
            <p>${commentary.replace(/\n/g, '<br>')}</p>
        `;

        card.appendChild(commentaryDiv);
        container.appendChild(card);
    }

    async function fetchDataAndRender() {
        try {
            const response = await fetch('/api/data');
            if (!response.ok) {
                // If token expires, API will return 401, redirect to auth
                if (response.status === 401) {
                    showAuthScreen();
                    return;
                }
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            const data = await response.json();
            console.log("Data fetched successfully:", data);

            const lastUpdatedEl = document.getElementById('last-updated');
            if (data.last_updated) {
                lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`;
            }

            renderMarketOverview(document.getElementById('market-content'), data.market, data.last_updated);
            renderNews(document.getElementById('news-content'), data.news, data.last_updated);

            // Render NASDAQ Heatmaps
            renderGridHeatmap(document.getElementById('nasdaq-heatmap-1d'), 'Nasdaq (1-Day)', data.nasdaq_heatmap_1d);
            renderGridHeatmap(document.getElementById('nasdaq-heatmap-1w'), 'Nasdaq (1-Week)', data.nasdaq_heatmap_1w);
            renderGridHeatmap(document.getElementById('nasdaq-heatmap-1m'), 'Nasdaq (1-Month)', data.nasdaq_heatmap_1m);
            renderHeatmapCommentary(document.getElementById('nasdaq-commentary'), data.nasdaq_heatmap?.ai_commentary, data.last_updated);

            // Render S&P 500 & Sector ETF Combined Heatmaps
            renderGridHeatmap(document.getElementById('sp500-heatmap-1d'), 'SP500 & Sector ETFs (1-Day)', data.sp500_combined_heatmap_1d);
            renderGridHeatmap(document.getElementById('sp500-heatmap-1w'), 'SP500 & Sector ETFs (1-Week)', data.sp500_combined_heatmap_1w);
            renderGridHeatmap(document.getElementById('sp500-heatmap-1m'), 'SP500 & Sector ETFs (1-Month)', data.sp500_combined_heatmap_1m);
            renderHeatmapCommentary(document.getElementById('sp500-commentary'), data.sp500_heatmap?.ai_commentary, data.last_updated);

            renderIndicators(document.getElementById('indicators-content'), data.indicators, data.last_updated);
            renderColumn(document.getElementById('column-content'), data.column);

        } catch (error) {
            console.error("Failed to fetch data:", error);
            document.getElementById('dashboard-content').innerHTML = `<div class="card"><p>データの読み込みに失敗しました: ${error.message}</p></div>`;
        }
    }

    // --- Swipe Navigation for Tabs ---
    function initSwipeNavigation() {
        const contentArea = document.getElementById('dashboard-content');
        const tabContainer = document.querySelector('.tab-container');
        let touchstartX = 0;
        let touchendX = 0;
        const swipeThreshold = 50; // Minimum horizontal distance for a swipe

        contentArea.addEventListener('touchstart', e => {
            touchstartX = e.changedTouches[0].screenX;
        }, { passive: true });

        contentArea.addEventListener('touchend', e => {
            touchendX = e.changedTouches[0].screenX;
            handleSwipe();
        });

        function handleSwipe() {
            const deltaX = touchendX - touchstartX;
            if (Math.abs(deltaX) < swipeThreshold) {
                return; // Not a significant swipe
            }

            const tabButtons = Array.from(document.querySelectorAll('.tab-button'));
            const currentActiveIndex = tabButtons.findIndex(btn => btn.classList.contains('active'));
            if (currentActiveIndex === -1) return; // Should not happen

            let nextIndex;
            if (deltaX > 0) { // Right swipe (move to tab on the left)
                nextIndex = currentActiveIndex - 1;
            } else { // Left swipe (move to tab on the right)
                nextIndex = currentActiveIndex + 1;
            }

            // Loop around if at the beginning or end
            if (nextIndex < 0) {
                nextIndex = tabButtons.length - 1;
            } else if (nextIndex >= tabButtons.length) {
                nextIndex = 0;
            }

            // Get the target button and simulate a click to switch tabs
            const nextTabButton = tabButtons[nextIndex];
            if (nextTabButton) {
                nextTabButton.click();
            }
        }
    }

    // --- App Initialization ---
    initializeApp();
});

// Add this to the existing app.js file

class NotificationManager {
    constructor() {
        this.isSupported = 'Notification' in window && 'serviceWorker' in navigator && 'PushManager' in window;
        this.vapidPublicKey = null;
    }

    async init() {
        if (!this.isSupported) {
            console.log('Push notifications are not supported');
            return;
        }

        // Get VAPID public key from server
        try {
            const response = await fetch('/api/vapid-public-key');
            const data = await response.json();
            this.vapidPublicKey = data.public_key;
        } catch (error) {
            console.error('Failed to get VAPID public key:', error);
            return;
        }

        // Check and request permission
        await this.requestPermission();

        // Subscribe to push notifications
        await this.subscribeUser();

        // Listen for messages from Service Worker
        navigator.serviceWorker.addEventListener('message', event => {
            if (event.data.type === 'data-updated') {
                console.log('Data updated via background sync at', event.data.timestamp || 'now');
                // Refresh the dashboard
                if (typeof fetchDataAndRender === 'function') {
                    fetchDataAndRender();
                }
                // Show a subtle notification in the UI
                const time = event.data.timestamp === '6:30' ? '朝6:30の' : '';
                this.showInAppNotification(`${time}データが更新されました`);
            }
        });
    }

    async requestPermission() {
        const permission = await Notification.requestPermission();
        console.log('Notification permission:', permission);
        return permission === 'granted';
    }

    async subscribeUser() {
        try {
            const registration = await navigator.serviceWorker.ready;

            // Check if already subscribed
            let subscription = await registration.pushManager.getSubscription();

            if (!subscription) {
                // Convert VAPID key
                const convertedVapidKey = this.urlBase64ToUint8Array(this.vapidPublicKey);

                // Subscribe
                subscription = await registration.pushManager.subscribe({
                    userVisibleOnly: true,
                    applicationServerKey: convertedVapidKey
                });

                // Send subscription to server
                await this.sendSubscriptionToServer(subscription);
                console.log('User is subscribed to push notifications for 6:30 AM updates');
            }

            // Register background sync
            if ('sync' in registration) {
                await registration.sync.register('data-sync');
                console.log('Background sync registered for 6:30 AM updates');
            }

            // Register periodic background sync if available
            if ('periodicSync' in registration) {
                const status = await navigator.permissions.query({
                    name: 'periodic-background-sync',
                });
                if (status.state === 'granted') {
                    await registration.periodicSync.register('data-update', {
                        minInterval: 60 * 60 * 1000 // 1 hour
                    });
                    console.log('Periodic background sync registered');
                }
            }
        } catch (error) {
            console.error('Failed to subscribe user:', error);
        }
    }

    async sendSubscriptionToServer(subscription) {
        try {
            const response = await fetch('/api/subscribe', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(subscription)
            });

            if (!response.ok) {
                throw new Error('Failed to send subscription to server');
            }
        } catch (error) {
            console.error('Error sending subscription to server:', error);
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
        // Create a toast notification in the UI
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

// Add CSS for toast animation
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }

    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

