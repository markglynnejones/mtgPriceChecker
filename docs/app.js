/* MTG Price Dashboard
   Features:
   - Card dropdown shows base card names only
   - Printing selector for specific printings
   - View mode: Combined (default) or Specific printing
   - Combined aggregation: MAX price per day across printings
   - Dynamic Top Movers (24h / 7d)
   - Movers show printing + set counts and are clickable
*/

const els = {
  cardSearch: document.getElementById("cardSearch"),
  cardSelect: document.getElementById("cardSelect"),
  viewMode: document.getElementById("viewMode"),
  printingSelect: document.getElementById("printingSelect"),
  moversWindow: document.getElementById("moversWindow"),

  cardTitle: document.getElementById("cardTitle"),
  cardBadges: document.getElementById("cardBadges"),
  statCurrent: document.getElementById("statCurrent"),
  stat7d: document.getElementById("stat7d"),
  statAth: document.getElementById("statAth"),
  statCount: document.getElementById("statCount"),
  footerNote: document.getElementById("footerNote"),
  movers: document.getElementById("movers"),
};

let cards = [];
let pricesById = {};
let chart = null;

let baseNames = [];
let printingsByBase = new Map();
let selectedBase = null;
let selectedPrintingId = null;

/* ---------------- Helpers ---------------- */

function formatGBP(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" }).format(n);
}

function sortSeries(series) {
  return [...series].sort((a, b) => a.date.localeCompare(b.date));
}

function parseDateISO(s) {
  const [y, m, d] = s.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

function getClosestOnOrBefore(series, targetISO) {
  for (let i = series.length - 1; i >= 0; i--) {
    if (series[i].date <= targetISO) return series[i];
  }
  return null;
}

function isoMinusDays(isoDate, days) {
  const d = parseDateISO(isoDate);
  const t = new Date(d.getTime() - days * 86400000);
  const y = t.getUTCFullYear();
  const m = String(t.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(t.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function pctChange(from, to) {
  if (!from || from === 0) return null;
  return ((to - from) / from) * 100;
}

/* ---------------- Parsing ---------------- */

function parseLabel(label) {
  const out = {
    id: label,
    baseName: label,
    set: null,
    collector: null,
    lang: null,
    finish: null,
    printable: label,
  };

  const m = /^(.+?)\s*\((.+)\)\s*$/.exec(label);
  if (!m) return out;

  out.baseName = m[1].trim();
  const tokens = m[2].trim().split(/\s+/);

  if (tokens[0]) out.set = tokens[0].toUpperCase();
  const cn = tokens.find(t => t.startsWith("#"));
  if (cn) out.collector = cn.slice(1);

  const lang = tokens.find(t => /^[a-z]{2,3}$/.test(t));
  if (lang) out.lang = lang;

  const finish = tokens.find(t => ["foil", "nonfoil", "non-foil", "etched"].includes(t.toLowerCase()));
  if (finish) out.finish = finish.replace("-", "");

  const bits = [];
  if (out.set) bits.push(out.set);
  if (out.collector) bits.push(`#${out.collector}`);
  if (out.lang) bits.push(out.lang);
  if (out.finish) bits.push(out.finish);

  out.printable = bits.join(" ");
  return out;
}

/* ---------------- Aggregation ---------------- */

function buildCombinedSeries(baseName) {
  const printings = printingsByBase.get(baseName) || [];
  const perDay = new Map();

  for (const p of printings) {
    const series = pricesById[p.id] || [];
    for (const pt of series) {
      const curr = perDay.get(pt.date);
      if (curr === undefined || pt.price > curr) {
        perDay.set(pt.date, pt.price);
      }
    }
  }

  return Array.from(perDay.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([date, price]) => ({ date, price }));
}

/* ---------------- Stats ---------------- */

function computeStats(series) {
  if (!series.length) {
    return { current: null, change7d: null, change7dPct: null, ath: null, count: 0 };
  }

  const sorted = sortSeries(series);
  const last = sorted[sorted.length - 1];
  const startISO = isoMinusDays(last.date, 7);
  const startPt = getClosestOnOrBefore(sorted, startISO);

  const change7d = startPt ? last.price - startPt.price : null;
  const change7dPct = startPt ? pctChange(startPt.price, last.price) : null;
  const ath = Math.max(...sorted.map(p => p.price));

  return {
    current: last.price,
    change7d,
    change7dPct,
    ath,
    count: sorted.length,
  };
}

/* ---------------- Movers ---------------- */

function getMetaCounts(baseName) {
  const printings = printingsByBase.get(baseName) || [];
  const sets = new Set(printings.map(p => p.set).filter(Boolean));
  return { printings: printings.length, sets: sets.size };
}

function computeMovers(days, limit = 8) {
  const movers = [];

  for (const baseName of baseNames) {
    const combined = buildCombinedSeries(baseName);
    if (!combined.length) continue;

    const last = combined[combined.length - 1];
    const startISO = isoMinusDays(last.date, days);
    const startPt = getClosestOnOrBefore(combined, startISO);
    if (!startPt) continue;

    const delta = last.price - startPt.price;
    movers.push({
      baseName,
      delta,
      pct: pctChange(startPt.price, last.price),
      meta: getMetaCounts(baseName),
    });
  }

  return {
    gainers: movers.filter(m => m.delta > 0).sort((a, b) => b.delta - a.delta).slice(0, limit),
    losers: movers.filter(m => m.delta < 0).sort((a, b) => a.delta - b.delta).slice(0, limit),
  };
}

function renderMovers() {
  const days = Number(els.moversWindow?.value || 7);
  const label = days === 1 ? "24h" : `${days}d`;
  const { gainers, losers } = computeMovers(days);

  els.movers.innerHTML = "";

  const header = (t) => {
    const h = document.createElement("div");
    h.style.fontWeight = "600";
    h.style.margin = "8px 0 4px";
    h.textContent = t;
    return h;
  };

  const row = (m) => {
    const li = document.createElement("li");
    li.style.cursor = "pointer";
    const delta = `${m.delta >= 0 ? "+" : ""}${formatGBP(m.delta)}`;
    const pct = m.pct == null ? "—" : `${m.pct >= 0 ? "+" : ""}${m.pct.toFixed(1)}%`;
    li.textContent = `${m.baseName} (${m.meta.printings}p · ${m.meta.sets}s): ${delta} (${pct})`;
    li.onclick = () => setSelectedBase(m.baseName);
    return li;
  };

  els.movers.appendChild(header(`📈 Top Gainers (${label})`));
  gainers.forEach(m => els.movers.appendChild(row(m)));

  els.movers.appendChild(header(`📉 Top Losers (${label})`));
  losers.forEach(m => els.movers.appendChild(row(m)));
}

/* ---------------- Rendering ---------------- */

function renderChart(label, series) {
  const ctx = document.getElementById("priceChart").getContext("2d");
  const labels = series.map(p => p.date);
  const data = series.map(p => p.price);

  if (chart) {
    chart.data.labels = labels;
    chart.data.datasets[0].data = data;
    chart.data.datasets[0].label = label;
    chart.update();
    return;
  }

  chart = new Chart(ctx, {
    type: "line",
    data: { labels, datasets: [{ label, data, tension: 0.25 }] },
    options: {
      plugins: { legend: { display: false } },
      scales: { y: { ticks: { callback: v => formatGBP(v) } } },
    },
  });
}

function applyViewState() {
  const printings = printingsByBase.get(selectedBase) || [];
  const mode = els.viewMode.value;

  if (mode === "combined" || printings.length <= 1) {
    const series = buildCombinedSeries(selectedBase);
    els.cardTitle.textContent = `${selectedBase} — Combined`;
    renderChart(selectedBase, series);
    return;
  }

  const p = printings.find(x => x.id === selectedPrintingId) || printings[0];
  const series = pricesById[p.id] || [];
  els.cardTitle.textContent = `${selectedBase} — ${p.printable}`;
  renderChart(`${selectedBase} (${p.printable})`, series);
}

function setSelectedBase(name) {
  selectedBase = name;
  const printings = printingsByBase.get(name) || [];
  selectedPrintingId = printings[0]?.id || null;
  applyViewState();
}

/* ---------------- Init ---------------- */

async function loadJson(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to load ${path}`);
  return res.json();
}

async function init() {
  [cards, pricesById] = await Promise.all([
    loadJson("./data/cards.json"),
    loadJson("./data/prices.json"),
  ]);

  const printings = cards.map(c => parseLabel(c.name));
  for (const p of printings) {
    if (!printingsByBase.has(p.baseName)) printingsByBase.set(p.baseName, []);
    printingsByBase.get(p.baseName).push(p);
  }

  baseNames = Array.from(printingsByBase.keys()).sort();
  els.cardSelect.innerHTML = baseNames.map(n => `<option>${n}</option>`).join("");

  setSelectedBase(baseNames[0]);
  renderMovers();

  els.cardSelect.onchange = e => setSelectedBase(e.target.value);
  els.viewMode.onchange = applyViewState;
  els.moversWindow.onchange = renderMovers;
}

init().catch(console.error);
