<script>
  // Primary PQP palette
  const PQP_COLORS = [
    "#1E73BE", // primary blue
    "#28A745", // green
    "#DC3545", // red
    "#6E6E6E", // medium gray
    "#165A91", // dark blue for contrast series
    "#F4F4F4"  // light gray (fills, backgrounds)
  ];

  // Global defaults (keeps layout, spacing untouched)
  if (window.Chart) {
    Chart.defaults.color = "#2B2B2B";                    // axis/labels text
    Chart.defaults.borderColor = "#E6E6E6";              // grid/border
    // Example helpers you can reuse:
    window.pqpSeries = (n=2) => PQP_COLORS.slice(0, n);  // pick top n colors
    window.pqpPrimary = "#1E73BE";
    window.pqpAccent  = { green:"#28A745", red:"#DC3545" };
  }
</script>
