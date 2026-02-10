/* Dashboard v2:
   - Card dropdown shows base names only
   - Printing selector shows printings for selected card
   - View mode: Combined (default) or Specific printing
   - Combined aggregation: MAX price per day across printings
*/

const els = {
  cardSearch: document.getElementById("cardSearch"),
  cardSelect: document.getElementById("cardSelect"),
  viewMode: document.getElementById("viewMode"),
  printingSelect: document.getElementById("printingSelect"),

  cardTitle: document.getElementById("cardTitle"),
  cardBadges: document.getElementById("cardBadges"),
  statCurrent: document.getElementById("statCurrent"),
  stat7d: document.getElementById("stat7d"),
  statAth: document.getElementById("statAth"),
  statCount: document.getElementById("statCount"),
  footerNote: document.getElementById("footerNote"),
  movers: document.getElementById("movers"),
};

let cards = [];        // from cards.json (each is a printing id label)
let pricesById = {};   // from prices.json keyed by printing id label
let chart = null;

// index
let baseNames = [];                           // unique card names
let printingsByBase = new Map();              // baseName -> array of printing objects
let selectedBase = null;
let selectedPrintingId = null;

/** ---------- helpers ---------- **/

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

function getClosestOnOrBefore(series, targetDateISO) {
  for (let i = series.length - 1; i >= 0; i--) {
    if (series[i].date <= targetDateISO) return series[i];
  }
  return null;
}

function computeStats(series) {
  if (!series || series.length === 0) {
    return { current: null, change7d: null, change7dPct: null, ath: null, count: 0 };
  }

  const sorted = sortSeries(series);
  const last = sorted[sorted.length - 1];
  const current = last.price;

  const lastDate = parseDateISO(last.date);
  const compareDate = new Date(lastDate.getTime() - 7 * 24 * 60 * 60 * 1000);
  const compareISO = [
    compareDate.getUTCFullYear(),
    String(compareDate.getUTCMonth() + 1).padStart(2, "0"),
    String(compareDate.getUTCDate()).padStart(2, "0"),
  ].join("-");

  const comparePoint = getClosestOnOrBefore(sorted, compareISO);

  let change7d = null;
  let change7dPct = null;
  if (comparePoint && typeof comparePoint.price === "number") {
    change7d = current - comparePoint.price;
    change7dPct = comparePoint.price === 0 ? null : (change7d / comparePoint.price) * 100;
  }

  const ath = sorted.reduce((max, p) => (p.price > max ? p.price : max), -Infinity);
  return { current, change7d, change7dPct, ath: ath === -Infinity ? null : ath, count: sorted.length };
}

/**
 * Our exporter uses a label like:
 *   "Aim High (INR #185 en nonfoil)"
 * We parse that so the UI can group by base name and show printing details.
 */
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
  const inside = m[2].trim();
  const tokens = inside.split(/\s+/).filter(Boolean);

  if (tokens[0]) out.set = tokens[0].toUpperCase();

  const cn = tokens.find(t => t.startsWith("#"));
  if (cn) out.collector = cn.replace("#", "");

  const lang = tokens.find(t => /^[a-z]{2,3}$/.test(t) || /^zhs$/.test(t) || /^zht$/.test(t));
  if (lang) out.lang = lang;

  const finish = tokens.find(t => ["foil", "nonfoil", "non-foil", "etched"].includes(t.toLowerCase()));
  if (finish) out.finish = finish.toLowerCase() === "non-foil" ? "nonfoil" : finish.toLowerCase();

  const bits = [];
  if (out.set) bits.push(out.set);
  if (out.collector) bits.push(`#${out.collector}`);
  if (out.lang) bits.push(out.lang);
  if (out.finish) bits.push(out.finish);

  out.printable = bits.join(" ");
  return out;
}

/** Combined series = max price per day across printings */
function buildCombinedSeries(baseName) {
  const printings = printingsByBase.get(baseName) || [];
  const perDay = new Map(); // date -> maxPrice

  for (const p of printings) {
    const series = pricesById[p.id] || [];
    for (const pt of series) {
      const curr = perDay.get(pt.date);
      if (curr === undefined || pt.price > curr) perDay.set(pt.date, pt.price);
    }
  }

  const dates = Array.from(perDay.keys()).sort();
  return dates.map(d => ({ date: d, price: perDay.get(d) }));
}

/** ---------- rendering ---------- **/

function renderBadgesForCombined(baseName) {
  els.cardBadges.innerHTML = "";

  const printings = printingsByBase.get(baseName) || [];
  const sets = new Set();
  const langs = new Set();
  const finishes = new Set();

  for (const p of printings) {
    if (p.set) sets.add(p.set);
    if (p.lang) langs.add(p.lang);
    if (p.finish) finishes.add(p.finish);
  }

  const parts = [];
  parts.push(`Printings: ${printings.length}`);
  if (sets.size) parts.push(`Sets: ${sets.size}`);
  if (langs.size) parts.push(`Langs: ${langs.size}`);
  if (finishes.size) parts.push(`Finishes: ${Array.from(finishes).join(", ")}`);

  for (const txt of parts) {
    const span = document.createElement("span");
    span.className = "badge";
    span.textContent = txt;
    els.cardBadges.appendChild(span);
  }
}

function renderBadgesForPrinting(printing) {
  els.cardBadges.innerHTML = "";
  const parts = [];

  if (printing.set) parts.push(`Set: ${printing.set}`);
  if (printing.collector) parts.push(`No: ${printing.collector}`);
  if (printing.lang) parts.push(`Lang: ${printing.lang}`);
  if (printing.finish) parts.push(`Finish: ${printing.finish}`);

  for (const txt of parts) {
    const span = document.createElement("span");
    span.className = "badge";
    span.textContent = txt;
    els.cardBadges.appendChild(span);
  }
}

function renderChart(label, series) {
  const sorted = sortSeries(series || []);
  const labels = sorted.map(p => p.date);
  const data = sorted.map(p => p.price);
  const ctx = document.getElementById("priceChart").getContext("2d");

  if (chart) {
    chart.data.labels = labels;
    chart.data.datasets[0].data = data;
    chart.data.datasets[0].label = label;
    chart.update();
    return;
  }

  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label,
          data,
          tension: 0.25,
          pointRadius: 2,
          pointHoverRadius: 5,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${formatGBP(ctx.parsed.y)}`,
          },
        },
      },
      scales: {
        y: {
          ticks: {
            callback: (v) => formatGBP(v),
          },
        },
      },
    },
  });
}

function renderStats(series) {
  const s = computeStats(series);
  els.statCurrent.textContent = formatGBP(s.current);

  if (s.change7d === null || s.change7dPct === null) {
    els.stat7d.textContent = "—";
  } else {
    const sign = s.change7d >= 0 ? "+" : "";
    const pctSign = s.change7dPct >= 0 ? "+" : "";
    els.stat7d.textContent = `${sign}${formatGBP(s.change7d)} (${pctSign}${s.change7dPct.toFixed(2)}%)`;
  }

  els.statAth.textContent = formatGBP(s.ath);
  els.statCount.textContent = String(s.count);

  if (series && series.length) {
    const sorted = sortSeries(series);
    els.footerNote.textContent = `Data range: ${sorted[0].date} → ${sorted[sorted.length - 1].date}`;
  } else {
    els.footerNote.textContent = "No data for this card yet.";
  }
}

function populateCardDropdown(listOfBaseNames) {
  els.cardSelect.innerHTML = "";
  for (const name of listOfBaseNames) {
    const opt = document.createElement("option");
    opt.value = name;
    opt.textContent = name;
    els.cardSelect.appendChild(opt);
  }
}

function populatePrintingDropdown(baseName) {
  const printings = (printingsByBase.get(baseName) || []).slice();
  // stable sort: set then collector then lang then finish
  printings.sort((a, b) => {
    const ak = `${a.set || ""}|${a.collector || ""}|${a.lang || ""}|${a.finish || ""}`;
    const bk = `${b.set || ""}|${b.collector || ""}|${b.lang || ""}|${b.finish || ""}`;
    return ak.localeCompare(bk);
  });

  els.printingSelect.innerHTML = "";
  for (const p of printings) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = p.printable || p.id;
    els.printingSelect.appendChild(opt);
  }

  // choose first by default if current selection isn’t valid
  if (!printings.find(p => p.id === selectedPrintingId)) {
    selectedPrintingId = printings[0]?.id || null;
  }

  els.printingSelect.value = selectedPrintingId || "";
}

function applyViewState() {
  const mode = els.viewMode.value; // combined | printing
  const printings = printingsByBase.get(selectedBase) || [];

  const hasMultiplePrintings = printings.length > 1;

  // Only enable printing selector if user chooses printing view AND there is > 1 printing
  const printingMode = (mode === "printing") && hasMultiplePrintings;
  els.printingSelect.disabled = !printingMode;

  if (mode === "combined" || !hasMultiplePrintings) {
    const combined = buildCombinedSeries(selectedBase);
    els.cardTitle.textContent = `${selectedBase} — Combined`;
    renderBadgesForCombined(selectedBase);
    renderStats(combined);
    renderChart(`${selectedBase} (Combined)`, combined);
    return;
  }

  // printing mode
  populatePrintingDropdown(selectedBase);
  const printing = printings.find(p => p.id === selectedPrintingId) || printings[0];
  const series = pricesById[printing.id] || [];

  const titleBits = printing.printable ? printing.printable : printing.id;
  els.cardTitle.textContent = `${selectedBase} — ${titleBits}`;
  renderBadgesForPrinting(printing);
  renderStats(series);
  renderChart(`${selectedBase} (${titleBits})`, series);
}

function setSelectedBase(name) {
  selectedBase = name;
  els.cardSelect.value = name;

  // default view mode stays as chosen; but if only one printing, force printing selector disabled
  const printings = printingsByBase.get(selectedBase) || [];
  if (printings.length === 1) {
    selectedPrintingId = printings[0].id;
    // optional: keep combined default anyway; we’ll still show combined unless user switches to printing
  }

  // Ensure printing dropdown is populated if needed
  populatePrintingDropdown(selectedBase);
  applyViewState();
}

function pctChange(from, to) {
  if (from === null || from === undefined || from === 0) return null;
  return ((to - from) / from) * 100;
}

function seriesLast(series) {
  if (!series || !series.length) return null;
  const s = sortSeries(series);
  return s[s.length - 1];
}

function seriesValueAtOrBefore(series, targetISODate) {
  const s = sortSeries(series);
  const p = getClosestOnOrBefore(s, targetISODate);
  return p ? p.price : null;
}

function isoMinusDays(isoDate, days) {
  // isoDate is YYYY-MM-DD
  const d = parseDateISO(isoDate);
  const t = new Date(d.getTime() - days * 24 * 60 * 60 * 1000);
  const y = t.getUTCFullYear();
  const m = String(t.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(t.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${dd}`;
}

function computeMovers7d(limit = 8) {
  const movers = [];
  for (const baseName of baseNames) {
    const combined = buildCombinedSeries(baseName);
    const lastPt = seriesLast(combined);
    if (!lastPt) continue;

    const endDate = lastPt.date;
    const startDate = isoMinusDays(endDate, 7);

    const startVal = seriesValueAtOrBefore(combined, startDate);
    const endVal = lastPt.price;

    if (startVal === null || startVal === undefined) continue;

    const delta = endVal - startVal;
    const pct = pctChange(startVal, endVal);

    movers.push({
      baseName,
      startDate,
      endDate,
      startVal,
      endVal,
      delta,
      pct,
    });
  }

  const gainers = movers
    .filter(m => m.delta > 0)
    .sort((a, b) => b.delta - a.delta)
    .slice(0, limit);

  const losers = movers
    .filter(m => m.delta < 0)
    .sort((a, b) => a.delta - b.delta) // most negative first
    .slice(0, limit);

  return { gainers, losers };
}

function renderMovers() {
  const { gainers, losers } = computeMovers7d(8);

  els.movers.innerHTML = "";

  const makeHeader = (text) => {
    const h = document.createElement("div");
    h.style.marginTop = "10px";
    h.style.marginBottom = "6px";
    h.style.fontWeight = "600";
    h.textContent = text;
    return h;
  };

  const makeRow = (m) => {
    const li = document.createElement("li");
    li.style.cursor = "pointer";
    li.style.userSelect = "none";

    const pctStr = (m.pct === null ? "—" : `${m.pct >= 0 ? "+" : ""}${m.pct.toFixed(1)}%`);
    const deltaStr = `${m.delta >= 0 ? "+" : ""}${formatGBP(m.delta)}`;

    li.textContent = `${m.baseName}: ${deltaStr} (${pctStr})`;

    li.addEventListener("click", () => {
      // jump to that card and keep current view mode
      setSelectedBase(m.baseName);
      // optional: scroll to chart
      document.getElementById("priceChart")?.scrollIntoView({ behavior: "smooth", block: "center" });
    });

    return li;
  };

  // If no data yet, show a friendly message
  if (!gainers.length && !losers.length) {
    const li = document.createElement("li");
    li.textContent = "Not enough history yet to compute 7-day movers.";
    els.movers.appendChild(li);
    return;
  }

  els.movers.appendChild(makeHeader("📈 Top Gainers (7d)"));
  if (gainers.length) {
    gainers.forEach(m => els.movers.appendChild(makeRow(m)));
  } else {
    const li = document.createElement("li");
    li.textContent = "No gainers in the last 7 days.";
    els.movers.appendChild(li);
  }

  els.movers.appendChild(makeHeader("📉 Top Losers (7d)"));
  if (losers.length) {
    losers.forEach(m => els.movers.appendChild(makeRow(m)));
  } else {
    const li = document.createElement("li");
    li.textContent = "No losers in the last 7 days.";
    els.movers.appendChild(li);
  }
}

async function loadJson(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to load ${path}: ${res.status}`);
  return res.json();
}

async function init() {
  renderMovers();

  [cards, pricesById] = await Promise.all([
    loadJson("./data/cards.json"),
    loadJson("./data/prices.json"),
  ]);

  // Parse printings
  const printings = cards.map(c => parseLabel(c.name));

  // Group by base name
  printingsByBase = new Map();
  for (const p of printings) {
    if (!printingsByBase.has(p.baseName)) printingsByBase.set(p.baseName, []);
    printingsByBase.get(p.baseName).push(p);
  }

  baseNames = Array.from(printingsByBase.keys()).sort((a, b) => a.localeCompare(b));
  populateCardDropdown(baseNames);

  // Default selection
  const first = baseNames[0];
  setSelectedBase(first);

  // Events
  els.cardSelect.addEventListener("change", (e) => setSelectedBase(e.target.value));

  els.viewMode.addEventListener("change", () => applyViewState());

  els.printingSelect.addEventListener("change", (e) => {
    selectedPrintingId = e.target.value;
    applyViewState();
  });

  els.cardSearch.addEventListener("input", (e) => {
    const q = (e.target.value || "").trim().toLowerCase();
    const filtered = !q
      ? baseNames
      : baseNames.filter(n => n.toLowerCase().includes(q));

    populateCardDropdown(filtered);

    if (filtered.length) setSelectedBase(filtered[0]);
  });
}

init().catch((err) => {
  els.cardTitle.textContent = "Failed to load dashboard data";
  els.footerNote.textContent = String(err?.message || err);
  console.error(err);
});
