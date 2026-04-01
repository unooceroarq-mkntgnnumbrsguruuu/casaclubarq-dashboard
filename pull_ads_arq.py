"""
CASA CLUB ARQ — Pull de datos publicitarios
Jala metricas de Google Ads (ARQ_2025_B) + Meta Ads (2024, ARQ, QRO)
y escribe los resultados a Google Sheets para que el dashboard los lea.

USO:
    python3 pull_ads_arq.py           # Mes actual
    python3 pull_ads_arq.py 2025-03   # Mes especifico

SECRETS REQUERIDOS en GitHub (mismos del repo marketing-guru):
    GOOGLE_ADS_YAML   — contenido completo del google-ads.yaml
    META_ACCESS_TOKEN — token de acceso Meta Marketing API
    SHEET_API_URL     — URL del Google Apps Script de ARQ
"""

import json, requests, sys, os, calendar, time, tempfile
from datetime import datetime

# ── CAMPAAS ────────────────────────────────────────────────
GOOGLE_CAMPAIGN_FILTER = "ARQ_2025_B"
META_CAMPAIGN_FILTER   = "2024, ARQ, QRO"
META_ACCOUNT           = "act_286249402372299"

# ── SECRETS ────────────────────────────────────────────────
META_TOKEN    = os.environ.get("META_ACCESS_TOKEN", "")
SHEET_API_URL = os.environ.get("SHEET_API_URL_ARQ", 
RETRY_MAX   = 3
RETRY_DELAY = 5


# ── FECHAS ────────────────────────────────────────────────
def get_date_range(ym=None):
    if ym:
        y, m = map(int, ym.split("-"))
    else:
        now = datetime.now()
        y, m = now.year, now.month
    last = calendar.monthrange(y, m)[1]
    return y, m, f"{y}-{m:02d}-01", f"{y}-{m:02d}-{last:02d}"


# ── GOOGLE ADS ────────────────────────────────────────────
def setup_google_client():
    from google.ads.googleads.client import GoogleAdsClient
    yaml_content = os.environ.get("GOOGLE_ADS_YAML", "")
    if not yaml_content:
        raise ValueError("Secret GOOGLE_ADS_YAML no encontrado")
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    tmp.write(yaml_content)
    tmp.close()
    client = GoogleAdsClient.load_from_storage(tmp.name, version="v18")
    os.unlink(tmp.name)
    return client


def pull_google(date_from, date_to):
    print(f"[GOOGLE] Jalando {GOOGLE_CAMPAIGN_FILTER} | {date_from} -> {date_to}")
    result = dict(campaign=GOOGLE_CAMPAIGN_FILTER, spend=0.0, clicks=0,
                  impressions=0, ctr=0.0, cpc=0.0, conversions=0, cpa=0.0,
                  status="ok", date_from=date_from, date_to=date_to)
    try:
        import yaml
        client = setup_google_client()
        yaml_content = os.environ.get("GOOGLE_ADS_YAML", "")
        cfg = yaml.safe_load(yaml_content)
        cid = str(cfg.get("login_customer_id",
                  cfg.get("client_customer_id", ""))).replace("-", "")

        ga = client.get_service("GoogleAdsService")
        query = f"""
            SELECT campaign.name, metrics.clicks, metrics.impressions,
                   metrics.cost_micros, metrics.ctr, metrics.average_cpc,
                   metrics.conversions
            FROM campaign
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN_FILTER}%'
              AND campaign.status != 'REMOVED'
        """
        for attempt in range(RETRY_MAX):
            try:
                for batch in ga.search_stream(customer_id=cid, query=query):
                    for row in batch.results:
                        m = row.metrics
                        result["spend"]       += round(m.cost_micros / 1_000_000, 2)
                        result["clicks"]      += m.clicks
                        result["impressions"] += m.impressions
                        result["conversions"] += int(m.conversions)
                break
            except Exception as e:
                if attempt < RETRY_MAX - 1:
                    time.sleep(RETRY_DELAY)
                else:
                    raise

        if result["clicks"] > 0:
            result["ctr"] = round(result["clicks"] / max(result["impressions"], 1) * 100, 2)
            result["cpc"] = round(result["spend"] / result["clicks"], 2)
        if result["conversions"] > 0:
            result["cpa"] = round(result["spend"] / result["conversions"], 2)

        print(f"[GOOGLE] ${result['spend']:,.2f} | {result['clicks']} clics | {result['conversions']} conv.")
    except Exception as e:
        result["status"] = f"error: {e}"
        print(f"[GOOGLE] ERROR: {e}")
    return result


# ── META ADS ──────────────────────────────────────────────
def pull_meta(date_from, date_to):
    print(f"[META] Jalando '{META_CAMPAIGN_FILTER}' | {date_from} -> {date_to}")
    result = dict(campaign=META_CAMPAIGN_FILTER, spend=0.0, impressions=0,
                  clicks=0, cpm=0.0, cpc=0.0, ctr=0.0, reach=0,
                  wa_conversations=0, cost_per_wa=0.0,
                  status="ok", date_from=date_from, date_to=date_to)
    if not META_TOKEN:
        result["status"] = "error: no token"
        return result

    base = "https://graph.facebook.com/v18.0"
    try:
        # 1. Buscar campaña por nombre
        r = requests.get(
            f"{base}/{META_ACCOUNT}/campaigns"
            f"?fields=id,name,status&access_token={META_TOKEN}",
            timeout=30
        )
        r.raise_for_status()
        campaigns = [c for c in r.json().get("data", [])
                     if META_CAMPAIGN_FILTER.lower() in c["name"].lower()]

        if not campaigns:
            result["status"] = "campaign not found"
            print(f"[META] No se encontro '{META_CAMPAIGN_FILTER}'")
            return result

        # 2. Insights por campaña
        for camp in campaigns:
            for attempt in range(RETRY_MAX):
                try:
                    r2 = requests.get(
                        f"{base}/{camp['id']}/insights"
                        f"?fields=spend,impressions,clicks,cpm,cpc,ctr,reach,actions,frequency"
                        f"&time_range={{\"since\":\"{date_from}\",\"until\":\"{date_to}\"}}"
                        f"&access_token={META_TOKEN}",
                        timeout=30
                    )
                    r2.raise_for_status()
                    data = r2.json().get("data", [])
                    if data:
                        d = data[0]
                        result["spend"]       += float(d.get("spend", 0))
                        result["impressions"] += int(d.get("impressions", 0))
                        result["clicks"]      += int(d.get("clicks", 0))
                        result["reach"]       += int(d.get("reach", 0))
                        result["cpm"]          = float(d.get("cpm", 0))
                        result["ctr"]          = float(d.get("ctr", 0))
                        for action in d.get("actions", []):
                            if "messaging" in action.get("action_type", ""):
                                result["wa_conversations"] += int(float(action.get("value", 0)))
                    break
                except Exception as e:
                    if attempt < RETRY_MAX - 1:
                        time.sleep(RETRY_DELAY)
                    else:
                        raise

        if result["clicks"] > 0:
            result["cpc"] = round(result["spend"] / result["clicks"], 2)
        if result["wa_conversations"] > 0:
            result["cost_per_wa"] = round(result["spend"] / result["wa_conversations"], 2)

        print(f"[META] ${result['spend']:,.2f} | {result['wa_conversations']} WA convs.")
    except Exception as e:
        result["status"] = f"error: {e}"
        print(f"[META] ERROR: {e}")
    return result


# ── GOOGLE SHEET ──────────────────────────────────────────
def update_sheet(year, month, g, m):
    if not SHEET_API_URL:
        print("[SHEET] SHEET_API_URL no configurada, saltando.")
        return
    meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
             "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
    payload = {
        "action":             "actualizar_arq",
        "mes":                meses[month - 1],
        "anio":               year,
        "google_spend":       g["spend"],
        "google_clicks":      g["clicks"],
        "google_impressions": g["impressions"],
        "google_ctr":         g["ctr"],
        "google_cpc":         g["cpc"],
        "google_conversions": g["conversions"],
        "google_cpa":         g["cpa"],
        "meta_spend":         m["spend"],
        "meta_impressions":   m["impressions"],
        "meta_clicks":        m["clicks"],
        "meta_cpm":           m["cpm"],
        "meta_ctr":           m["ctr"],
        "meta_reach":         m["reach"],
        "meta_wa_convs":      m["wa_conversations"],
        "meta_cost_per_wa":   m["cost_per_wa"],
        "total_spend":        round(g["spend"] + m["spend"], 2),
        "google_status":      g["status"],
        "meta_status":        m["status"],
        "timestamp":          datetime.now().isoformat()
    }
    for attempt in range(RETRY_MAX):
        try:
            r = requests.post(SHEET_API_URL, json=payload, timeout=30)
            r.raise_for_status()
            print(f"[SHEET] OK: {r.text[:100]}")
            return
        except Exception as e:
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"[SHEET] ERROR: {e}")


# ── MAIN ──────────────────────────────────────────────────
if __name__ == "__main__":
    ym = sys.argv[1] if len(sys.argv) > 1 else None
    year, month, date_from, date_to = get_date_range(ym)

    print(f"\n{'='*50}")
    print(f"CASA CLUB ARQ — Pull de Ads | {date_from} -> {date_to}")
    print(f"{'='*50}\n")

    g = pull_google(date_from, date_to)
    m = pull_meta(date_from, date_to)
    update_sheet(year, month, g, m)

    print(f"\n{'='*50}")
    print(f"RESUMEN FINAL:")
    print(f"  Google (ARQ_2025_B)        : ${g['spend']:,.2f} MXN | {g['clicks']} clics | {g['conversions']} conv.")
    print(f"  Meta (2024, ARQ, QRO)      : ${m['spend']:,.2f} MXN | {m['wa_conversations']} WA convs.")
    print(f"  TOTAL                      : ${g['spend'] + m['spend']:,.2f} MXN")
    print(f"{'='*50}\n")
