/* ============================================
   PQP Charts Theme — layout-safe brand layer
   Works with Chart.js v3 and v4
   ============================================ */

(function () {
  if (typeof window === "undefined") return;
  if (!window.Chart) {
    console.warn("[PQP] Chart.js not found. charts-theme.js loaded before Chart.js?");
    return;
  }

  // --- Brand palette ------------------------------------
  const COLORS = {
    blue: "#1E73BE",      // Primary
    blueDark: "#165A91",  // Hover/Dark
    green: "#28A745",     // Success
    red: "#DC3545",       // Danger
    gray600: "#6E6E6E",   // Medium gray (labels)
    gray200: "#E6E6E6",   // Grid/border
    gray050: "#F4F4F4",   // Light gray fill
    text: "#2B2B2B",      // Primary text
    white: "#FFFFFF"
  };

  // Public-ish helpers (attach to window for reuse)
  const PQP = (window.PQP = window.PQP || {});
  PQP.COLORS = COLORS;

  // Series pickers: get N distinct brand colors
  PQP.series = function (n = 2) {
    const ordered = [
      COLORS.blue,
      COLORS.green,
      COLORS.red,
      COLORS.blueDark,
      COLORS.gray600
    ];
    return ordered.slice(0, Math.max(1, n));
  };

  // Number formatters
  const locale = "en-ZA"; // adjust if needed
  PQP.format = {
    number: (v, maxFrac = 0) =>
      new Intl.NumberFormat(locale, { maximumFractionDigits: maxFrac }).format(v),
    currencyR: (v, maxFrac = 0) =>
      new Intl.NumberFormat(locale, {
        style: "currency",
        currency: "ZAR",
        maximumFractionDigits: maxFrac
      }).format(v),
    percent: (v, maxFrac = 0) =>
      new Intl.NumberFormat(locale, {
        style: "percent",
        maximumFractionDigits: maxFrac
      }).format(v)
  };

  // Gradients (optional subtle fills)
  PQP.makeLinearGradient = function (ctx, area, hex, alphaStart = 0.25, alphaEnd = 0) {
    const gradient = ctx.createLinearGradient(0, area.top, 0, area.bottom);
    const rgba = (hex, a) => {
      const c = hex.replace("#", "");
      const r = parseInt(c.substring(0, 2), 16);
      const g = parseInt(c.substring(2, 4), 16);
      const b = parseInt(c.substring(4, 6), 16);
      return `rgba(${r}, ${g}, ${b}, ${a})`;
    };
    gradient.addColorStop(0, rgba(hex, alphaStart));
    gradient.addColorStop(1, rgba(hex, alphaEnd));
    return gradient;
  };

  // --- Global defaults (non-breaking) --------------------
  const { Chart } = window;

  // Text & grid
  Chart.defaults.color = COLORS.text;
  Chart.defaults.borderColor = COLORS.gray200;

  // Responsive behavior preserved
  Chart.defaults.maintainAspectRatio = true;

  // Elements
  if (Chart.defaults.elements) {
    Chart.defaults.elements.line = Object.assign({}, Chart.defaults.elements.line, {
      tension: 0.25,
      borderWidth: 2,
      borderColor: COLORS.blue,
      backgroundColor: COLORS.blue + "33" // fallback alpha
    });

    Chart.defaults.elements.bar = Object.assign({}, Chart.defaults.elements.bar, {
      borderWidth: 0,
      backgroundColor: COLORS.blue
    });

    Chart.defaults.elements.point = Object.assign({}, Chart.defaults.elements.point, {
      radius: 3,
      hoverRadius: 4,
      backgroundColor: COLORS.white,
      borderColor: COLORS.blue,
      borderWidth: 2
    });
  }

  // Scales (category/time/linear)
  const scaleDefaults = {
    ticks: {
      color: COLORS.text
    },
    grid: {
      color: COLORS.gray200
    },
    title: {
      color: COLORS.text,
      font: { weight: 600 }
    }
  };
  Chart.defaults.scales = Chart.defaults.scales || {};
  ["category", "time", "linear", "logarithmic"].forEach((k) => {
    Chart.defaults.scales[k] = Object.assign({}, Chart.defaults.scales[k], scaleDefaults);
  });

  // Plugins
  Chart.defaults.plugins = Chart.defaults.plugins || {};
  Chart.defaults.plugins.legend = Object.assign({}, Chart.defaults.plugins.legend, {
    labels: {
      color: COLORS.text,
      usePointStyle: true,
      pointStyle: "circle",
      boxWidth: 8
    }
  });
  Chart.defaults.plugins.title = Object.assign({}, Chart.defaults.plugins.title, {
    color: COLORS.text,
    font: { weight: 600 }
  });

  Chart.defaults.plugins.tooltip = Object.assign({}, Chart.defaults.plugins.tooltip, {
    backgroundColor: COLORS.text,
    titleColor: COLORS.white,
    bodyColor: COLORS.white,
    borderColor: COLORS.gray200,
    borderWidth: 1,
    displayColors: true,
    usePointStyle: true,
    callbacks: {
      // Safe defaults; you can override per-chart
      label: function (ctx) {
        const ds = ctx.dataset || {};
        const label = ds.label ? ds.label + ": " : "";
        const v = typeof ctx.formattedValue !== "undefined" ? ctx.formattedValue : ctx.parsed;
        return label + v;
      }
    }
  });

  // --- Helpers to quickly brand charts -------------------
  /**
   * Apply PQP colors to datasets:
   * - First dataset => blue
   * - Second => green
   * - Third => red
   * etc.
   * Preserves any explicit colors you already set.
   */
  PQP.applyPalette = function (config, useGradients = false) {
    if (!config || !config.data || !Array.isArray(config.data.datasets)) return config;

    const pick = PQP.series(config.data.datasets.length);

    config.data.datasets.forEach((ds, i) => {
      const base = pick[i] || COLORS.blue;

      // Respect pre-set colors if provided
      if (!ds.backgroundColor) ds.backgroundColor = base;
      if (!ds.borderColor) ds.borderColor = base;

      // Optional subtle gradient fills for line/area charts
      if (useGradients && ds.type !== "bar") {
        ds.backgroundColor = function (ctx) {
          const { chart, chartArea, ctx: canvasCtx } = ctx;
          if (!chartArea) return base + "33"; // before first render
          return PQP.makeLinearGradient(canvasCtx, chartArea, base, 0.25, 0.0);
        };
      }
    });

    return config;
  };

  /**
   * Shortcut: create a chart with PQP branding automatically applied.
   * @param {HTMLCanvasElement | CanvasRenderingContext2D} ctx
   * @param {object} config
   * @param {boolean} gradientFills
   * @returns {Chart}
   */
  PQP.chart = function (ctx, config, gradientFills = false) {
    const cfg = JSON.parse(JSON.stringify(config)); // shallow clone
    PQP.applyPalette(cfg, gradientFills);
    return new Chart(ctx, cfg);
  };

  // --- Example tooltip formatters you can reuse ----------
  PQP.tooltip = {
    currencyR: function (label = "") {
      return function (ctx) {
        const v = ctx.parsed.y ?? ctx.parsed;
        return (label ? label + ": " : "") + PQP.format.currencyR(v, 0);
      };
    },
    percent: function (label = "") {
      return function (ctx) {
        const v = (ctx.parsed.y ?? ctx.parsed) / 100;
        return (label ? label + ": " : "") + PQP.format.percent(v, 1);
      };
    },
    number: function (label = "") {
      return function (ctx) {
        const v = ctx.parsed.y ?? ctx.parsed;
        return (label ? label + ": " : "") + PQP.format.number(v, 0);
      };
    }
  };

  // Expose simple presets for quick use
  PQP.presets = {
    barBasic: function (labels, seriesLabel, data) {
      return {
        type: "bar",
        data: {
          labels,
          datasets: [{ label: seriesLabel, data }]
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: { label: PQP.tooltip.number(seriesLabel) }
            }
          }
        }
      };
    },
    lineBasic: function (labels, seriesLabel, data) {
      return {
        type: "line",
        data: {
          labels,
          datasets: [{ label: seriesLabel, data, fill: true }]
        },
        options: {
          plugins: {
            tooltip: {
              callbacks: { label: PQP.tooltip.number(seriesLabel) }
            }
          }
        }
      };
    }
  };

  console.info("[PQP] charts-theme.js applied.");
})();
