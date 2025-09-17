document.addEventListener('DOMContentLoaded', () => {
    console.log("HanaView Dashboard Initialized");

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
        });
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

    function renderMarketOverview(container, marketData) {
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
            content += `
                <div class="market-section">
                    <h3>AI解説</h3>
                    <p>${marketData.ai_commentary}</p>
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
            summaryHeader.innerHTML = '<h3>今朝のサマリー</h3>';
            if (lastUpdated) {
                const date = new Date(lastUpdated);
                const dateString = `${date.getFullYear()}年${date.getMonth() + 1}月${date.getDate()}日 ${date.getHours()}:${String(date.getMinutes()).padStart(2, '0')}`;
                summaryHeader.innerHTML += `<p class="summary-date">${dateString}</p>`;
            }

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
        container.appendChild(earningsCard);
    }

    function renderColumn(container, columnData) {
        if (!container) return;
        container.innerHTML = '';
        const report = columnData ? columnData.weekly_report : null;

        // Case 1: Success - content is available
        if (report && report.content) {
            const card = document.createElement('div');
            card.className = 'card';
            card.innerHTML = `
                <div class="column-container">
                    <h3>${report.title || '週次AIコラム'}</h3>
                    <p class="column-date">Date: ${report.date || ''}</p>
                    <div class="column-content">
                        ${report.content.replace(/\n/g, '<br>')}
                    </div>
                </div>
            `;
            container.appendChild(card);
        // Case 2: Failure - an error is reported
        } else if (report && report.error) {
            container.innerHTML = '<div class="card"><p>生成が失敗しました。</p></div>';
        // Case 3: Not yet generated
        } else {
            container.innerHTML = '<div class="card"><p>AIコラムはまだありません。（月曜日に週間分、火〜金曜日に当日分が生成されます）</p></div>';
        }
    }

    function renderHeatmapCommentary(container, commentary) {
        if (!container || !commentary) return;

        // Create a wrapper card for the commentary
        const card = document.createElement('div');
        card.className = 'card';

        const commentaryDiv = document.createElement('div');
        commentaryDiv.className = 'ai-commentary';
        commentaryDiv.innerHTML = `<h3>AI解説</h3><p>${commentary.replace(/\n/g, '<br>')}</p>`;

        card.appendChild(commentaryDiv);
        container.appendChild(card);
    }

    async function fetchDataAndRender() {
        try {
            const response = await fetch('/api/data');
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            console.log("Data fetched successfully:", data);

            const lastUpdatedEl = document.getElementById('last-updated');
            if (data.last_updated) {
                lastUpdatedEl.textContent = `Last updated: ${new Date(data.last_updated).toLocaleString('ja-JP')}`;
            }

            renderMarketOverview(document.getElementById('market-content'), data.market);
            renderNews(document.getElementById('news-content'), data.news, data.last_updated);

            // Render NASDAQ Heatmaps
            renderGridHeatmap(document.getElementById('nasdaq-heatmap-1d'), 'Nasdaq (1-Day)', data.nasdaq_heatmap_1d);
            renderGridHeatmap(document.getElementById('nasdaq-heatmap-1w'), 'Nasdaq (1-Week)', data.nasdaq_heatmap_1w);
            renderGridHeatmap(document.getElementById('nasdaq-heatmap-1m'), 'Nasdaq (1-Month)', data.nasdaq_heatmap_1m);
            renderHeatmapCommentary(document.getElementById('nasdaq-commentary'), data.nasdaq_heatmap?.ai_commentary);

            // Render S&P 500 & Sector ETF Combined Heatmaps
            renderGridHeatmap(document.getElementById('sp500-heatmap-1d'), 'SP500 & Sector ETFs (1-Day)', data.sp500_combined_heatmap_1d);
            renderGridHeatmap(document.getElementById('sp500-heatmap-1w'), 'SP500 & Sector ETFs (1-Week)', data.sp500_combined_heatmap_1w);
            renderGridHeatmap(document.getElementById('sp500-heatmap-1m'), 'SP500 & Sector ETFs (1-Month)', data.sp500_combined_heatmap_1m);
            renderHeatmapCommentary(document.getElementById('sp500-commentary'), data.sp500_heatmap?.ai_commentary);

            renderIndicators(document.getElementById('indicators-content'), data.indicators, data.last_updated);
            renderColumn(document.getElementById('column-content'), data.column);

        } catch (error) {
            console.error("Failed to fetch data:", error);
            document.getElementById('dashboard-content').innerHTML = `<div class="card"><p>データの読み込みに失敗しました: ${error.message}</p></div>`;
        }
    }

    initTabs();
    fetchDataAndRender();
});
