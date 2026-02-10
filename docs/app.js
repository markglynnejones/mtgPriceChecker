/* Dashboard UI (improved display)
   - Uses series key as an internal ID (unique per printing)
   - Displays "card name" only in the dropdown/search
   - Shows set / collector / lang / finish as badges
*/

const els = {
  cardSearch: document.getElementById("cardSearch"),
  cardSelect: document.getElementById("cardSelect"),
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

/** -------- helpers -------- **/

function formatGBP(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return new Intl.NumberFormat("en-GB", { style: "currency", currency: "GBP" }).format(n);
}

function parseDateISO(s) {
  const [y, m, d] = s.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

function sortSeries(series) {
  return [...series].sort((a, b) => a.date.localeCompare(b.date));
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

/** -------- NEW: parse label into a nice display model -------- **/
function parseLabel(label) {
  // Expected: "Name (SET #123 en nonfoil)"
  // But we’ll be tolerant if format varies.
  const out = {
    id: label,
    displayName: label,
    set: null,
    collector: null,
    lang: null,
    finish: null,
  };

  const m = /^(.+?)\s*\((.+)\)\s*$/.exec(label);
  if (!m) return out;

  out.displayName = m[1].trim();
  const inside = m[2].trim();

  // Common inside tokens: "CMM #123 en nonfoil"
  const tokens = inside.split(/\s+/).filter(Boolean);

  if (tokens.length > 0) out.set = tokens[0].toUpperCase();

  // find collector like "#123"
  const cn = tokens.find(t => t.startsWith("#"));
  if (cn) out.collector = cn.replace("#", "");

  // language likely a 2-3 char code in tokens (en, ja, zhs, zht, etc.)
  const lang = tokens.find(t => /^[a-z]{2,3}$/.test(t) || /^zh[st]$/.test(t) || /^zhs$/.test(t) || /^zht$/.test(t));
  if (lang) out.lang = lang;

  // finish likely one of: foil/nonfoil/etched
  const finish = tokens.find(t => ["foil", "nonfoil", "non-foil", "etched"].includes(t.toLowerCase()));
  if (finish) out.finish = finish.toLowerCase() === "non-foil" ? "nonfoil" : finish.toLowerCase();

  return out;
}

/** -------- NEW: group printings by base card name -------- **/
function groupByName(cards) {
  const map = new Map(); // displayName -> [cardObj]
  for (const c of cards) {
    const parsed = parseLabel(c.name);
    const displayName = parsed.displayName;
    const item = { ...c, _parsed: parsed };
    if (!map.has(displayName)) map.set(displayName, []);
    map.get(displayName).push(item);
  }
  return map;
}

function makeOptionText(item) {
  // Dropdown shows only base name; if multiple printings exist, add a suffix.
  const p = item._parsed || parseLabel(item.name);
  const base = p.displayName;

  // If we have set/collector/finish, show a short disambiguator
  const bits = [];
  if (p.set) bits.push(p.set);
  if (p.collector) bits.push(`#${p.collector}`);
  if (p.lang) bits.push(p.lang);
  if (p.finish) bits.push(p.finish);

  return bits.length ? `${base} — ${bits.join(" ")}` : base;
}

/** -------- UI render -------- **/

function renderBadges(card) {
  els.cardBadges.innerHTML = "";
  if (!card) return;

  const p = card._parsed || parseLabel(card.name);

  const parts = [];
  if (p.set) parts.push(`Set: ${p.set}`);
  if (p.collector) parts.push(`No: ${p.collector}`);
  if (p.lang) parts.push(`Lang: ${p.lang}`);
  if (p.finish) parts.push(`Finish: ${p.finish}`);

  // fallback to supplied metadata if label parsing didn’t find it
  if (!p.set && card.set) parts.push(`Set: ${card.set}`);
  if (!card.rarity && card.rarity) parts.push(`Rarity: ${card.rarity}`);
  if (!p.finish && card.finish) parts.push(`Finish: ${card.finish}`);

  for (const txt of parts) {
    const span = document.createElement("span");
    span.className = "badge";
    span.textContent = txt;
    els.cardBadges.appendChild(span);
  }
}

function renderChart(cardLabelId, series) {
  const sorted = sortSeries(series || []);
  const labels = sorted.map(p => p.date);
  const data = sorted.map(p => p.price);
  const ctx = document.getElementById("priceChart").getContext("2d");

  if (chart) {
    chart.data.labels = labels;
    chart.data.datasets[0].data = data;
    chart.data.datasets[0].label = `${cardLabelId} (GBP)`;
    chart.update();
    return;
  }

  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: `${cardLabelId} (GBP)`,
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
        legend: { display: false }, // cleaner
        tooltip: { callbacks: { label: (ctx) => ` ${formatGBP(ctx.parsed.y)}` } },
      },
      scales: {
        y: { ticks: { callback: (v) => formatGBP(v) } },
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

function setSelectedCardById(cardId) {
  const card = cards.find(c => c.name === cardId) || { name: cardId };
  const p = card._parsed || parseLabel(cardId);

  const series = pricesById[cardId] || [];
  const bits = [];
  if (p.set) bits.push(p.set);
  if (p.collector) bits.push(`#${p.collector}`);
  if (p.lang) bits.push(p.lang);
  if (p.finish) bits.push(p.finish);

  els.cardTitle.textContent = bits.length
    ? `${p.displayName} — ${bits.join(" ")}`
    : p.displayName;

  renderBadges(card);
  renderStats(series);
  renderChart(cardId, series);

  if (els.cardSelect.value !== cardId) els.cardSelect.value = cardId;
}

function populateSelectFromList(list) {
  els.cardSelect.innerHTML = "";
  for (const card of list) {
    const opt = document.createElement("option");
    opt.value = card.name; // internal id
    opt.textContent = makeOptionText(card);
    els.cardSelect.appendChild(opt);
  }
}

/** -------- Search behaviour: search by base name -------- **/
function filterCardsBySearch(query) {
  const q = (query || "").trim().toLowerCase();
  if (!q) return cards;

  // match against base display name and also the full label
  return cards.filter(c => {
    const p = c._parsed || parseLabel(c.name);
    return p.displayName.toLowerCase().includes(q) || String(c.name).toLowerCase().includes(q);
  });
}

function renderMoversDemo() {
  const demo = [
    "Smothering Tithe: +£2.70 (demo)",
    "The One Ring: +£4.10 (demo)",
    "Orcish Bowmasters: -£1.20 (demo)",
  ];
  els.movers.innerHTML = "";
  demo.forEach(t => {
    const li = document.createElement("li");
    li.textContent = t;
    els.movers.appendChild(li);
  });
}

async function loadJson(path) {
  const res = await fetch(path, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to load ${path}: ${res.status}`);
  return res.json();
}

async function init() {
  renderMoversDemo();

  [cards, pricesById] = await Promise.all([
    loadJson("./data/cards.json"),
    loadJson("./data/prices.json"),
  ]);

  // attach parsed label info for display grouping
  cards = cards.map(c => ({ ...c, _parsed: parseLabel(c.name) }));

  // sort by base name then by full label
  cards.sort((a, b) => {
    const an = (a._parsed?.displayName || a.name).toLowerCase();
    const bn = (b._parsed?.displayName || b.name).toLowerCase();
    if (an !== bn) return an.localeCompare(bn);
    return String(a.name).localeCompare(String(b.name));
  });

  populateSelectFromList(cards);

  const firstId = cards[0]?.name || Object.keys(pricesById)[0];
  if (firstId) setSelectedCardById(firstId);

  els.cardSelect.addEventListener("change", (e) => setSelectedCardById(e.target.value));

  els.cardSearch.addEventListener("input", (e) => {
    const filtered = filterCardsBySearch(e.target.value);
    populateSelectFromList(filtered);
    if (filtered.length) setSelectedCardById(filtered[0].name);
  });
}

init().catch((err) => {
  els.cardTitle.textContent = "Failed to load dashboard data";
  els.footerNote.textContent = String(err?.message || err);
  console.error(err);
});
