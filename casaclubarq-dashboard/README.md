# Casa Club ARQ — Dashboard de Marketing

Dashboard en tiempo real que jala datos de **Google Ads** (campaña `ARQ_2025_B`)
y **Meta Ads** (campaña `2024, ARQ, QRO`) hacia un Google Sheet,
que el dashboard HTML lee para mostrar métricas reales.

---

## Arquitectura

```
GitHub Actions (cron diario 7am CDMX)
    └── pull_ads_arq.py
        ├── Google Ads API → métricas de ARQ_2025_B
        └── Meta Marketing API → métricas de 2024, ARQ, QRO
              └── Google Apps Script (Code.gs)
                      └── Google Sheet "CasaClubARQ - Marketing Data"
                              └── index.html (dashboard) lo lee via fetch()
                                      └── Netlify lo sirve (accesible desde celular)
```

---

## PASO 1 — Google Apps Script

1. Ve a [script.google.com](https://script.google.com)
2. **Nuevo proyecto** → nombra: `CasaClubARQ - Dashboard API`
3. Borra el código default y pega el contenido de `Code.gs`
4. Guarda (Ctrl+S)
5. **Implementar → Nueva implementación**
   - Tipo: `Aplicación web`
   - Ejecutar como: `Yo (tu cuenta de Google)`
   - Quien tiene acceso: `Cualquier persona`
6. Autoriza los permisos que te pida
7. **Copia la URL** que termina en `/exec` — la necesitas para el Paso 2

---

## PASO 2 — Secrets de GitHub en este repo

Ve a **Settings → Secrets and variables → Actions** en este repo y agrega:

| Secret | Valor |
|--------|-------|
| `GOOGLE_ADS_YAML` | El mismo que ya tienes en `marketing-guru` |
| `META_ACCESS_TOKEN` | El mismo que ya tienes en `marketing-guru` |
| `SHEET_API_URL_ARQ` | La URL del Apps Script del Paso 1 |

Para copiar los secrets del repo `marketing-guru`:
- No puedes copiar el valor (GitHub los oculta), pero puedes crear nuevos con el mismo valor
- El `google-ads.yaml` lo tienes guardado en tu máquina o en el repo original

---

## PASO 3 — Netlify

1. Ve a [netlify.com](https://netlify.com) → **Add new site → Import an existing project**
2. Conecta con GitHub y selecciona `casaclubarq-dashboard`
3. Configuración del build:
   - Branch: `main`
   - Build command: (dejar vacío)
   - Publish directory: `.`
4. **Deploy site**
5. Netlify te da una URL tipo `https://casaclubarq-XXXXX.netlify.app`
   — esa URL funciona desde cualquier celular

---

## PASO 4 — Conectar el dashboard al Sheet

Una vez desplegado en Netlify, abre el dashboard y en la consola del navegador ejecuta:

```javascript
setSheetUrl("URL_DEL_APPS_SCRIPT_AQUI")
```

O directamente en el código: edita `index.html`, busca:
```javascript
const SHEET_API_URL = localStorage.getItem('arq-sheet-url') || '';
```
y reemplaza `''` con tu URL entre comillas.

---

## PASO 5 — Prueba manual

En GitHub → **Actions → Pull ARQ Ads Data → Run workflow**

Esto ejecuta el script ahora mismo y escribe los datos al Sheet.
Luego recarga el dashboard y deberías ver datos reales.

---

## Ejecución automática

El workflow corre todos los días a las **7:00 AM hora México**.
Puedes cambiarlo editando la línea `cron` en `.github/workflows/pull_ads_arq.yml`.

---

## Archivos del repo

| Archivo | Descripción |
|---------|-------------|
| `index.html` | Dashboard completo (HTML/CSS/JS) |
| `pull_ads_arq.py` | Script Python que jala los datos de las APIs |
| `.github/workflows/pull_ads_arq.yml` | Workflow de GitHub Actions |
| `Code.gs` | Google Apps Script (copiar a script.google.com) |
| `netlify.toml` | Configuración de Netlify |
