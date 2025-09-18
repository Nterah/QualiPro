
# QualiPro Logo — SVG & Font Specs

**Fonts**
- Name: Montserrat **Bold 700**
- Tagline: Montserrat **Medium 500**
- Fallback: `Montserrat, system-ui, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif`

**Letter‑spacing**
- Name: **-0.5px**
- Tagline: **+0.25px**

**Colors**
- Charcoal (name): #2B2B2B
- Medium Gray (tagline): #6B6B6B
- Gradient (symbol): #18A999 → #F4C542 → #F68B1E

**Usage**
- **Symbol-only**: Favicons, app icons, navbars, buttons, small headers.
- **Full logo (name)**: Section headers, large cards.
- **Full logo (name + tagline)**: Splash/login, landing, exports.

**Web integration**
Include Montserrat (500, 700) in your CSS:
```css
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@500;700&display=swap');
.qp-name { font-family: 'Montserrat', ...; font-weight: 700; letter-spacing:-0.5px; }
.qp-tag  { font-family: 'Montserrat', ...; font-weight: 500; letter-spacing:0.25px; }
```

**Print/Offline**
Open the SVG in a vector editor (Figma/Illustrator/Inkscape) and **outline text** before print to avoid font substitution.

**Scaling**
- Minimum width for tagline legibility: **≥ 650 px** total logo width.
- Symbol height ≈ name cap‑height × **1.4** for balanced proportions.
