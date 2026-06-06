"""
e.ARGAMA Marketing Intelligence — pull_ads_arq.py v2
Funnel: Google Ads (ARQ_QRO_2026) → landing → Meta Ads (2026_ARQ_QRO) → WhatsApp → leads

Jala 4 ventanas de tiempo (hoy, 7d, 30d, 90d) para Google y Meta,
genera alertas automáticas con umbrales definidos y envía todo al Sheet.
"""

import json
import os
import requests
import sys
import tempfile
import time
from datetime import date, datetime, timedelta

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
GOOGLE_CAMPAIGN = "ARQ_QRO_2026"
META_CAMPAIGN   = "2026_ARQ_QRO"
META_ACCOUNT    = "act_286249402372299"
GOOGLE_CUSTOMER = "3248545148"
META_API_VER    = "v20.0"

# Umbrales de alerta
CPC_MAX_MXN   = 18.0   # CPC Google > $18 MXN → alerta
FREQ_MAX      = 5.0    # Frecuencia Meta > 5   → alerta roja
IS_LOST_MAX   = 30.0   # Impression share perdida por presupuesto > 30% → alerta

META_TOKEN    = os.environ.get("META_ACCESS_TOKEN", "")
SHEET_API_URL = os.environ.get("SHEET_API_URL_ARQ", "")

RETRY_MAX   = 3
RETRY_DELAY = 5


# ── FECHAS ────────────────────────────────────────────────────────────────────

def build_date_ranges():
    today = date.today()
    return {
        "today": (today.isoformat(), today.isoformat()),
        "7d":    ((today - timedelta(days=6)).isoformat(),  today.isoformat()),
        "30d":   ((today - timedelta(days=29)).isoformat(), today.isoformat()),
        "90d":   ((today - timedelta(days=89)).isoformat(), today.isoformat()),
    }


# ── GOOGLE ADS ────────────────────────────────────────────────────────────────

def setup_google_client():
    from google.ads.googleads.client import GoogleAdsClient
    yaml_content = os.environ.get("GOOGLE_ADS_YAML", "")
    if not yaml_content:
        raise ValueError("Secret GOOGLE_ADS_YAML no encontrado")
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    tmp.write(yaml_content)
    tmp.close()
    client = GoogleAdsClient.load_from_storage(tmp.name)
    os.unlink(tmp.name)
    return client


def pull_google(ga, cid, date_from, date_to):
    result = {
        "campaign": GOOGLE_CAMPAIGN,
        "spend": 0.0, "clicks": 0, "impressions": 0,
        "ctr": 0.0, "cpc": 0.0, "conversions": 0, "cpa": 0.0,
        "impression_share_lost_budget": 0.0,
        "status": "ok", "date_from": date_from, "date_to": date_to,
        "keywords": [], "search_terms": [],
    }
    try:
        # Métricas de campaña + impression share perdida por presupuesto
        q = f"""
            SELECT campaign.name, campaign.status,
                   metrics.clicks, metrics.impressions,
                   metrics.cost_micros, metrics.ctr,
                   metrics.average_cpc, metrics.conversions,
                   metrics.cost_per_conversion,
                   metrics.search_budget_lost_impression_share
            FROM campaign
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN}%'
              AND campaign.status != 'REMOVED'
        """
        for batch in ga.search_stream(customer_id=cid, query=q):
            for row in batch.results:
                m = row.metrics
                result["spend"]       += round(m.cost_micros / 1_000_000, 2)
                result["clicks"]      += m.clicks
                result["impressions"] += m.impressions
                result["conversions"] += int(m.conversions)
                if m.search_budget_lost_impression_share:
                    result["impression_share_lost_budget"] = round(
                        m.search_budget_lost_impression_share * 100, 1
                    )

        if result["clicks"] > 0:
            result["ctr"] = round(result["clicks"] / max(result["impressions"], 1) * 100, 2)
            result["cpc"] = round(result["spend"] / result["clicks"], 2)
        if result["conversions"] > 0:
            result["cpa"] = round(result["spend"] / result["conversions"], 2)

        # Keywords top
        try:
            q_kw = f"""
                SELECT ad_group_criterion.keyword.text,
                       ad_group_criterion.keyword.match_type,
                       ad_group_criterion.quality_info.quality_score,
                       metrics.clicks, metrics.impressions,
                       metrics.cost_micros, metrics.ctr, metrics.conversions
                FROM keyword_view
                WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
                  AND campaign.name LIKE '%{GOOGLE_CAMPAIGN}%'
                  AND ad_group_criterion.status != 'REMOVED'
                ORDER BY metrics.clicks DESC
                LIMIT 15
            """
            kw = []
            for batch in ga.search_stream(customer_id=cid, query=q_kw):
                for row in batch.results:
                    kw.append({
                        "keyword": row.ad_group_criterion.keyword.text,
                        "match":   str(row.ad_group_criterion.keyword.match_type).split(".")[-1],
                        "qs":      row.ad_group_criterion.quality_info.quality_score,
                        "clicks":  row.metrics.clicks,
                        "conv":    int(row.metrics.conversions),
                        "spend":   round(row.metrics.cost_micros / 1_000_000, 2),
                    })
            result["keywords"] = kw
        except Exception as e:
            print(f"[GOOGLE] Keywords (no critico): {e}")

        # Terminos de busqueda
        try:
            q_st = f"""
                SELECT search_term_view.search_term,
                       metrics.clicks, metrics.impressions,
                       metrics.cost_micros, metrics.conversions
                FROM search_term_view
                WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
                  AND campaign.name LIKE '%{GOOGLE_CAMPAIGN}%'
                ORDER BY metrics.clicks DESC
                LIMIT 20
            """
            st = []
            for batch in ga.search_stream(customer_id=cid, query=q_st):
                for row in batch.results:
                    st.append({
                        "term":   row.search_term_view.search_term,
                        "clicks": row.metrics.clicks,
                        "spend":  round(row.metrics.cost_micros / 1_000_000, 2),
                        "conv":   int(row.metrics.conversions),
                    })
            result["search_terms"] = st
        except Exception as e:
            print(f"[GOOGLE] Search terms (no critico): {e}")

        print(
            f"[GOOGLE] {date_from}/{date_to}: "
            f"${result['spend']:,.2f} | {result['clicks']} clics | "
            f"{result['conversions']} conv | IS lost: {result['impression_share_lost_budget']}%"
        )

    except Exception as e:
        result["status"] = f"error: {e}"
        print(f"[GOOGLE] ERROR {date_from}/{date_to}: {e}")
    return result


def pull_google_account(ga, cid):
    """Saldo/presupuesto de la cuenta de Google Ads."""
    info = {"status": "ok", "approved_amount": 0.0, "balance": 0.0, "currency": "MXN"}
    try:
        q = """
            SELECT account_budget.status,
                   account_budget.name,
                   account_budget.approved_spending_limit_micros,
                   account_budget.adjusted_spending_limit_micros
            FROM account_budget
            WHERE account_budget.status = 'APPROVED'
        """
        for batch in ga.search_stream(customer_id=cid, query=q):
            for row in batch.results:
                ab = row.account_budget
                approved = ab.approved_spending_limit_micros or 0
                adjusted = ab.adjusted_spending_limit_micros or 0
                # adjusted = approved - adjustments; aproxima saldo disponible
                info["approved_amount"] = round(approved / 1_000_000, 2) if approved else 0
                info["balance"]         = round(adjusted / 1_000_000, 2) if adjusted else 0
        print(f"[GOOGLE] Presupuesto aprobado: ${info['approved_amount']:,.2f} | Disponible: ${info['balance']:,.2f}")
    except Exception as e:
        info["status"] = f"error: {e}"
        print(f"[GOOGLE] Account balance (no critico): {e}")
    return info


# ── META ADS ──────────────────────────────────────────────────────────────────

def pull_meta(date_from, date_to):
    result = {
        "campaign": META_CAMPAIGN,
        "spend": 0.0, "impressions": 0, "clicks": 0,
        "cpm": 0.0, "cpc": 0.0, "ctr": 0.0,
        "reach": 0, "frequency": 0.0,
        "wa_conversations": 0, "cost_per_wa": 0.0,
        "status": "ok", "date_from": date_from, "date_to": date_to,
        "ad_sets": [],
    }
    if not META_TOKEN:
        result["status"] = "error: no token"
        return result

    base = f"https://graph.facebook.com/{META_API_VER}"
    time_range = json.dumps({"since": date_from, "until": date_to})

    try:
        # Buscar campana por nombre
        r = requests.get(
            f"{base}/{META_ACCOUNT}/campaigns",
            params={"fields": "id,name,status", "access_token": META_TOKEN},
            timeout=30,
        )
        r.raise_for_status()
        campaigns = [
            c for c in r.json().get("data", [])
            if META_CAMPAIGN.lower() in c["name"].lower()
        ]
        if not campaigns:
            result["status"] = "campaign not found"
            print(f"[META] No se encontro '{META_CAMPAIGN}'")
            return result

        for camp in campaigns:
            # Insights de campana
            r2 = requests.get(
                f"{base}/{camp['id']}/insights",
                params={
                    "fields": "spend,impressions,clicks,cpm,cpc,ctr,reach,frequency,actions",
                    "time_range": time_range,
                    "access_token": META_TOKEN,
                },
                timeout=30,
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
                result["frequency"]    = float(d.get("frequency", 0))
                for action in d.get("actions", []):
                    if "messaging" in action.get("action_type", ""):
                        result["wa_conversations"] += int(float(action.get("value", 0)))

            # Ad sets
            try:
                r3 = requests.get(
                    f"{base}/{camp['id']}/adsets",
                    params={"fields": "id,name,status", "access_token": META_TOKEN},
                    timeout=30,
                )
                r3.raise_for_status()
                for adset in r3.json().get("data", []):
                    r4 = requests.get(
                        f"{base}/{adset['id']}/insights",
                        params={
                            "fields": "spend,impressions,clicks,cpm,ctr,reach,frequency,actions",
                            "time_range": time_range,
                            "access_token": META_TOKEN,
                        },
                        timeout=30,
                    )
                    r4.raise_for_status()
                    as_data = r4.json().get("data", [])
                    if as_data:
                        ad = as_data[0]
                        wa_convs = sum(
                            int(float(a.get("value", 0)))
                            for a in ad.get("actions", [])
                            if "messaging" in a.get("action_type", "")
                        )
                        spend_as = float(ad.get("spend", 0))
                        freq_as  = float(ad.get("frequency", 0))
                        result["ad_sets"].append({
                            "name":        adset["name"],
                            "status":      adset["status"],
                            "spend":       spend_as,
                            "impressions": int(ad.get("impressions", 0)),
                            "clicks":      int(ad.get("clicks", 0)),
                            "reach":       int(ad.get("reach", 0)),
                            "frequency":   freq_as,
                            "cpm":         float(ad.get("cpm", 0)),
                            "ctr":         float(ad.get("ctr", 0)),
                            "wa_convs":    wa_convs,
                            "cost_per_wa": round(spend_as / wa_convs, 2) if wa_convs > 0 else 0,
                        })
            except Exception as e:
                print(f"[META] Ad sets (no critico): {e}")

        if result["clicks"] > 0:
            result["cpc"] = round(result["spend"] / result["clicks"], 2)
        if result["wa_conversations"] > 0:
            result["cost_per_wa"] = round(result["spend"] / result["wa_conversations"], 2)

        print(
            f"[META] {date_from}/{date_to}: "
            f"${result['spend']:,.2f} | freq {result['frequency']:.1f} | "
            f"{result['wa_conversations']} WA | {len(result['ad_sets'])} ad sets"
        )

    except Exception as e:
        result["status"] = f"error: {e}"
        print(f"[META] ERROR {date_from}/{date_to}: {e}")
    return result


def pull_meta_account():
    """Saldo de la cuenta de Meta Ads."""
    info = {"status": "ok", "balance": 0.0, "currency": "MXN", "amount_spent": 0.0}
    if not META_TOKEN:
        return info
    base = f"https://graph.facebook.com/{META_API_VER}"
    try:
        r = requests.get(
            f"{base}/{META_ACCOUNT}",
            params={
                "fields": "balance,currency,amount_spent,spend_cap",
                "access_token": META_TOKEN,
            },
            timeout=30,
        )
        r.raise_for_status()
        d = r.json()
        # Meta devuelve balance en centavos de la moneda de la cuenta
        info["balance"]      = float(d.get("balance", 0)) / 100
        info["currency"]     = d.get("currency", "MXN")
        info["amount_spent"] = float(d.get("amount_spent", 0)) / 100
        info["spend_cap"]    = float(d.get("spend_cap", 0)) / 100
        print(f"[META] Saldo cuenta: ${info['balance']:,.2f} {info['currency']}")
    except Exception as e:
        info["status"] = f"error: {e}"
        print(f"[META] Account balance (no critico): {e}")
    return info


# ── ALERTAS ───────────────────────────────────────────────────────────────────

def generate_alerts(periods):
    alerts = []
    g7 = periods.get("7d", {}).get("google", {})
    m7 = periods.get("7d", {}).get("meta",   {})

    # CPC > $18 MXN
    cpc = g7.get("cpc", 0)
    if cpc > CPC_MAX_MXN and cpc > 0:
        alerts.append({
            "level":  "warning",
            "code":   "CPC_ALTO",
            "msg":    f"CPC Google ${cpc:.2f} MXN supera umbral ${CPC_MAX_MXN:.0f} MXN",
            "detail": f"Periodo 7 dias | {GOOGLE_CAMPAIGN} | Revisar pujas y quality score",
        })

    # Frecuencia Meta > 5 (nivel campana)
    freq = m7.get("frequency", 0)
    if freq > FREQ_MAX and freq > 0:
        alerts.append({
            "level":  "critical",
            "code":   "FRECUENCIA_ALTA",
            "msg":    f"Frecuencia Meta {freq:.1f} supera umbral {FREQ_MAX:.0f} — fatiga de audiencia",
            "detail": f"Periodo 7 dias | {META_CAMPAIGN} | Renovar creativos o ampliar publico",
        })

    # Frecuencia Meta > 5 por ad set
    for ads in m7.get("ad_sets", []):
        fa = ads.get("frequency", 0)
        if fa > FREQ_MAX:
            alerts.append({
                "level":  "critical",
                "code":   "FRECUENCIA_ADSET",
                "msg":    f"Ad Set '{ads['name']}' frecuencia {fa:.1f} > {FREQ_MAX:.0f}",
                "detail": "Renovar creativos o ampliar segmentacion de publico",
            })

    # 0 conversiones Google en 7 dias con gasto activo
    conv_7d = g7.get("conversions", -1)
    if conv_7d == 0 and g7.get("spend", 0) > 0:
        alerts.append({
            "level":  "urgent",
            "code":   "SIN_CONVERSIONES_GOOGLE",
            "msg":    f"0 conversiones Google en 7 dias con ${g7.get('spend', 0):,.0f} MXN gastado",
            "detail": "Revisar landing page, tracking de conversiones y configuracion de campana",
        })

    # Impression share perdida por presupuesto > 30%
    is_lost = g7.get("impression_share_lost_budget", 0)
    if is_lost > IS_LOST_MAX:
        alerts.append({
            "level":  "warning",
            "code":   "IS_PERDIDA_PRESUPUESTO",
            "msg":    f"Impresiones perdidas por presupuesto: {is_lost:.0f}%",
            "detail": "Aumentar presupuesto diario o reducir pujas para maximizar alcance",
        })

    # 0 conversaciones WhatsApp Meta en 7 dias con gasto activo
    wa_7d = m7.get("wa_conversations", -1)
    if wa_7d == 0 and m7.get("spend", 0) > 0:
        alerts.append({
            "level":  "warning",
            "code":   "SIN_WA_CONVERSACIONES",
            "msg":    f"0 conversaciones WhatsApp en 7 dias con ${m7.get('spend', 0):,.0f} MXN gastado",
            "detail": "Verificar boton de WhatsApp, objetivo de campana y tracking de Meta",
        })

    return alerts


# ── SHEET ─────────────────────────────────────────────────────────────────────

def update_sheet(payload):
    if not SHEET_API_URL:
        print("[SHEET] SHEET_API_URL_ARQ no configurada, saltando.")
        return
    for attempt in range(RETRY_MAX):
        try:
            r = requests.post(SHEET_API_URL, json=payload, timeout=30)
            r.raise_for_status()
            print(f"[SHEET] OK: {r.text[:120]}")
            return
        except Exception as e:
            if attempt < RETRY_MAX - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"[SHEET] ERROR final: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"e.ARGAMA MARKETING INTELLIGENCE v2")
    print(f"Google: {GOOGLE_CAMPAIGN}  |  Meta: {META_CAMPAIGN}")
    print(f"Fecha:  {date.today().isoformat()}")
    print(f"{'='*60}\n")

    ranges = build_date_ranges()
    all_periods = {}

    # Inicializar cliente Google Ads una sola vez
    google_ok = False
    ga = None
    try:
        google_client = setup_google_client()
        ga = google_client.get_service("GoogleAdsService")
        google_ok = True
        print("[GOOGLE] Cliente inicializado OK\n")
    except Exception as e:
        print(f"[GOOGLE] No se pudo inicializar cliente: {e}\n")

    # Pull de los 4 periodos
    for period_key, (d_from, d_to) in ranges.items():
        g = (
            pull_google(ga, GOOGLE_CUSTOMER, d_from, d_to)
            if google_ok
            else {"status": "error: client init failed", "campaign": GOOGLE_CAMPAIGN}
        )
        m = pull_meta(d_from, d_to)
        all_periods[period_key] = {"google": g, "meta": m}
        print()

    # Saldos de cuenta
    google_account = pull_google_account(ga, GOOGLE_CUSTOMER) if google_ok else {"status": "skipped"}
    meta_account   = pull_meta_account()
    print()

    # Alertas
    alerts = generate_alerts(all_periods)

    # Payload final
    payload = {
        "action":         "actualizar_arq_v2",
        "timestamp":      datetime.now().isoformat(),
        "periods":        all_periods,
        "alerts":         alerts,
        "google_account": google_account,
        "meta_account":   meta_account,
    }

    update_sheet(payload)

    # Resumen consola
    g7  = all_periods["7d"]["google"]
    m7  = all_periods["7d"]["meta"]
    g30 = all_periods["30d"]["google"]
    m30 = all_periods["30d"]["meta"]

    print(f"\n{'='*60}")
    print(f"RESUMEN 7D:")
    print(f"  Google  : ${g7['spend']:>10,.2f} MXN | {g7['clicks']:>5} clics | {g7['conversions']:>3} conv | CPC ${g7['cpc']:.2f}")
    print(f"  Meta    : ${m7['spend']:>10,.2f} MXN | freq {m7['frequency']:.1f} | {m7['wa_conversations']} WA")
    print(f"RESUMEN 30D:")
    print(f"  Google  : ${g30['spend']:>10,.2f} MXN")
    print(f"  Meta    : ${m30['spend']:>10,.2f} MXN")
    print(f"  TOTAL   : ${g30['spend'] + m30['spend']:>10,.2f} MXN")
    if alerts:
        print(f"\nALERTAS ({len(alerts)}):")
        for a in alerts:
            prefix = "● URGENTE" if a["level"] == "urgent" else "● CRITICO" if a["level"] == "critical" else "▲ ALERTA"
            print(f"  [{prefix}] {a['msg']}")
    else:
        print("\nSin alertas activas.")
    print(f"{'='*60}\n")
