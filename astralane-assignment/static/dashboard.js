// ---------------------------------------------------------------------
// Pure, DOM-free layout calculation for the candlestick chart. Kept
// separate from the actual canvas drawing so it's testable in plain
// Node (see dashboard.test.js) without needing a browser/DOM.
// ---------------------------------------------------------------------
function computeCandleLayout(candles, width, height, paddingLeft = 50, paddingBottom = 20) {
  if (!candles || candles.length === 0) return [];

  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const maxPrice = Math.max(...highs);
  const minPrice = Math.min(...lows);
  const priceRange = maxPrice - minPrice || 1; // avoid div-by-zero on flat data

  const plotWidth = width - paddingLeft;
  const plotHeight = height - paddingBottom;
  const slotWidth = plotWidth / candles.length;
  const bodyWidth = Math.max(2, slotWidth * 0.6);

  const priceToY = (price) => {
    const frac = (price - minPrice) / priceRange;
    return plotHeight - frac * plotHeight;
  };

  return candles.map((c, i) => {
    const xCenter = paddingLeft + i * slotWidth + slotWidth / 2;
    const isUp = c.close >= c.open;
    return {
      x: xCenter,
      bodyWidth,
      yHigh: priceToY(c.high),
      yLow: priceToY(c.low),
      yOpen: priceToY(c.open),
      yClose: priceToY(c.close),
      color: isUp ? "#2e8b57" : "#b03a2e",
      bucketStart: c.bucket_start,
    };
  });
}

// ---------------------------------------------------------------------
// Canvas drawing (uses the layout above). Not unit-tested headlessly —
// straightforward enough to eyeball once served for real.
// ---------------------------------------------------------------------
function drawCandleChart(canvas, candles) {
  const ctx = canvas.getContext("2d");
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  const layout = computeCandleLayout(candles, canvas.width, canvas.height);
  if (layout.length === 0) {
    ctx.fillStyle = "#5b6472";
    ctx.fillText("No data for this range", 20, 20);
    return;
  }

  for (const bar of layout) {
    ctx.strokeStyle = bar.color;
    ctx.fillStyle = bar.color;

    // wick
    ctx.beginPath();
    ctx.moveTo(bar.x, bar.yHigh);
    ctx.lineTo(bar.x, bar.yLow);
    ctx.stroke();

    // body
    const bodyTop = Math.min(bar.yOpen, bar.yClose);
    const bodyHeight = Math.max(1, Math.abs(bar.yClose - bar.yOpen));
    ctx.fillRect(bar.x - bar.bodyWidth / 2, bodyTop, bar.bodyWidth, bodyHeight);
  }
}

// ---------------------------------------------------------------------
// Data fetching + wiring. Runs only in the browser (guarded so this file
// can still be `require()`'d from a Node test without executing DOM code).
// ---------------------------------------------------------------------
async function loadContention() {
  const from = document.getElementById("from-slot").value;
  const to = document.getElementById("to-slot").value;
  const resp = await fetch(`/api/contention?from=${from}&to=${to}`);
  const data = await resp.json();

  document.getElementById("contention-summary").textContent =
    `Schedule depth: ${data.depth} steps`;

  const tbody = document.querySelector("#conflict-table tbody");
  tbody.innerHTML = "";
  for (const [account, count] of data.top_conflicting_accounts) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${account}</td><td>${count}</td>`;
    tbody.appendChild(tr);
  }
}

let tokenList = [];

function syncIntervalOptions() {
  const select = document.getElementById("interval-select");
  const mint = document.getElementById("token-select").value;
  const token = tokenList.find((t) => t.mint === mint);
  select.innerHTML = "";
  if (!token) return;
  if (token.candles_1m > 0) {
    const opt = document.createElement("option");
    opt.value = "1m";
    opt.textContent = "1 minute";
    select.appendChild(opt);
  }
  if (token.candles_5m > 0) {
    const opt = document.createElement("option");
    opt.value = "5m";
    opt.textContent = "5 minute";
    select.appendChild(opt);
  }
}

async function loadTokens() {
  const resp = await fetch("/api/tokens");
  tokenList = await resp.json();
  const select = document.getElementById("token-select");
  select.innerHTML = "";
  for (const token of tokenList) {
    const opt = document.createElement("option");
    opt.value = token.mint;
    const n = token.candles_1m + token.candles_5m;
    opt.textContent = `${token.mint.slice(0, 8)}... (${n} candles)`;
    select.appendChild(opt);
  }
  syncIntervalOptions();
}

async function loadOhlcv() {
  const mint = document.getElementById("token-select").value;
  const interval = document.getElementById("interval-select").value;
  if (!mint || !interval) return;
  const resp = await fetch(`/api/ohlcv?mint=${encodeURIComponent(mint)}&interval=${interval}`);
  const candles = await resp.json();
  const canvas = document.getElementById("candle-chart");
  drawCandleChart(canvas, candles);
}

if (typeof window !== "undefined") {
  document.getElementById("load-contention").addEventListener("click", loadContention);
  document.getElementById("load-ohlcv").addEventListener("click", loadOhlcv);
  document.getElementById("token-select").addEventListener("change", () => {
    syncIntervalOptions();
    loadOhlcv();
  });
  document.getElementById("interval-select").addEventListener("change", loadOhlcv);
  loadTokens().then(loadOhlcv);
}

// Export for Node-based testing (no-op in the browser).
if (typeof module !== "undefined") {
  module.exports = { computeCandleLayout };
}
