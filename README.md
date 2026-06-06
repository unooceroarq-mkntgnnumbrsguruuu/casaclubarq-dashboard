# e.ARGAMA — Marketing Intelligence Dashboard v2

Dashboard en tiempo real del funnel de marketing de e.ARGAMA (despacho de arquitectura, Querétaro).

**Funnel:** Google Ads `ARQ_QRO_2026` → landing page → Meta Ads `2026_ARQ_QRO` → WhatsApp → leads calificados

---

## Arquitectura

```
GitHub Actions (cron 07:00 AM CDMX)
    └── pull_ads_arq.py
        ├── Google Ads API  → 4 periodos: HOY / 7D / 30D / 90D
        │   └── métricas + keywords + search terms + impression share + saldo cuenta
        └── Meta Ads API    → 4 periodos: HOY / 7D / 30D / 90D
            └── métricas + frecuencia + ad sets + saldo cuenta
                └── alertas automáticas (CPC, frecuencia, conversiones)
                    └── Google Apps Script (Code.gs)
                            └── Google Sheet "CasaClubARQ - Marketing Data"
                                    └── index.html lo lee via fetch()
                                            └── Netlify lo sirve (accesible desde celular)
```

---

## IDs de cuenta

| Plataforma | ID                        | Campaña        |
|------------|---------------------------|----------------|
| Google Ads | `3248545148`              | `ARQ_QRO_2026` |
| Meta Ads   | `act_286249402372299`     | `2026_ARQ_QRO` |

---

## Umbrales de alerta

| Alerta              | Umbral             | Nivel    |
|---------------------|--------------------|----------|
| CPC Google          | > $18 MXN          | warning  |
| Frecuencia Meta     | > 5.0              | critical |
| 0 conversiones      | 7 días con gasto   | urgent   |
| IS perdida (presp.) | > 30%              | warning  |
| 0 WA conversations  | 7 días con gasto   | warning  |

---

## Secrets de GitHub requeridos

Ve a **Settings → Secrets and variables → Actions** y verifica que existen:

| Secret              | Descripción                                      |
|---------------------|--------------------------------------------------|
| `GOOGLE_ADS_YAML`   | Contenido del archivo `google-ads.yaml`          |
| `META_ACCESS_TOKEN` | Token de acceso de la Meta Marketing API         |
| `SHEET_API_URL_ARQ` | URL del Google Apps Script (`/exec`)             |

---

## Setup inicial

### Paso 1 — Google Apps Script

1. Ve a [script.google.com](https://script.google.com) → **Nuevo proyecto**
2. Nombra el proyecto: `CasaClubARQ - Dashboard API`
3. Borra el código default y pega el contenido de `Code.gs`
4. Guarda (Ctrl+S)
5. **Implementar → Nueva implementación**
   - Tipo: `Aplicación web`
   - Ejecutar como: `Yo (tu cuenta de Google)`
   - Quién tiene acceso: `Cualquier persona`
6. Autoriza los permisos
7. Copia la URL que termina en `/exec`

### Paso 2 — Agregar secret en GitHub

1. Ve a **Settings → Secrets → Actions** en este repo
2. Agrega `SHEET_API_URL_ARQ` con la URL del Apps Script del Paso 1

### Paso 3 — Netlify

1. [netlify.com](https://netlify.com) → **Add new site → Import an existing project**
2. Conecta GitHub → selecciona `casaclubarq-dashboard`
3. Build command: *(vacío)* | Publish directory: `.`
4. Deploy

### Paso 4 — Conectar dashboard al Sheet

La primera vez que abras el dashboard te pedirá la URL del Apps Script.
Pégala y guarda. Se almacena en `localStorage` del navegador.

Si necesitas cambiarla: botón **⚙ config** en el header.

---

## Ejecutar manualmente

**GitHub → Actions → Pull ARQ Ads Data → Run workflow**

Esto ejecuta el script inmediatamente. Los datos aparecen en el dashboard al recargar.

---

## Ejecución automática

Todos los días a las **07:00 AM CDMX** vía cron en GitHub Actions.

---

## Archivos del repo

| Archivo                                  | Descripción                                      |
|------------------------------------------|--------------------------------------------------|
| `pull_ads_arq.py`                        | Script Python: pull de Google Ads + Meta Ads     |
| `index.html`                             | Dashboard (dark terminal, sin gráficas)          |
| `Code.gs`                                | Google Apps Script: guarda y sirve los datos     |
| `.github/workflows/pull_ads_arq.yml`     | GitHub Actions: cron diario 07:00 AM CDMX        |
| `netlify.toml`                           | Configuración de Netlify                         |
