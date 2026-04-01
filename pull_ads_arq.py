import json, requests, sys, os, calendar, time, tempfile
from datetime import datetime

GOOGLE_CAMPAIGN_FILTER = "ARQ_2025_B"
META_CAMPAIGN_FILTER   = "2024, ARQ, QRO"
META_ACCOUNT           = "act_286249402372299"
GOOGLE_CLIENT_ID       = "3248545148"

META_TOKEN    = os.environ.get("META_ACCESS_TOKEN", "")
SHEET_API_URL = os.environ.get("SHEET_API_URL_ARQ", "https://script.google.com/macros/s/AKfycbxyrmc3JXpUFnjrCYVEF23oZgX5KbVYeKRKc0i9933HZx_flPh6mSDhe5bO4ruOc8Fk_Q/exec")

RETRY_MAX   = 3
RETRY_DELAY = 5


def get_date_range(ym=None):
    if ym:
        y, m = map(int, ym.split("-"))
    else:
        now = datetime.now()
        y, m = now.year, now.month
    last = calendar.monthrange(y, m)[1]
    return y, m, f"{y}-{m:02d}-01", f"{y}-{m:02d}-{last:02d}"


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


def pull_google(date_from, date_to):
    print(f"[GOOGLE] Jalando {GOOGLE_CAMPAIGN_FILTER} | {date_from} -> {date_to}")
    result = dict(
        campaign=GOOGLE_CAMPAIGN_FILTER, spend=0.0, clicks=0,
        impressions=0, ctr=0.0, cpc=0.0, conversions=0, cpa=0.0,
        status="ok", date_from=date_from, date_to=date_to,
        geo=[], keywords=[], search_terms=[], ads=[]
    )
    try:
        client = setup_google_client()
        ga = client.get_service("GoogleAdsService")
        cid = GOOGLE_CLIENT_ID

        # 1. METRICAS GENERALES
        q_camp = f"""
            SELECT campaign.name, campaign.status,
                   metrics.clicks, metrics.impressions,
                   metrics.cost_micros, metrics.ctr,
                   metrics.average_cpc, metrics.conversions,
                   metrics.cost_per_conversion
            FROM campaign
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN_FILTER}%'
              AND campaign.status != 'REMOVED'
        """
        for batch in ga.search_stream(customer_id=cid, query=q_camp):
            for row in batch.results:
                m = row.metrics
                result["spend"]       += round(m.cost_micros / 1_000_000, 2)
                result["clicks"]      += m.clicks
                result["impressions"] += m.impressions
                result["conversions"] += int(m.conversions)
        print(f"[GOOGLE] Campaña: ${result['spend']:,.2f} | {result['clicks']} clics")

        # 2. DESGLOSE GEOGRAFICO
        q_geo = f"""
            SELECT campaign.name,
                   geographic_view.country_criterion_id,
                   segments.geo_target_city,
                   metrics.clicks, metrics.impressions,
                   metrics.cost_micros, metrics.conversions
            FROM geographic_view
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN_FILTER}%'
            ORDER BY metrics.clicks DESC
            LIMIT 10
        """
        try:
            geo_data = []
            for batch in ga.search_stream(customer_id=cid, query=q_geo):
                for row in batch.results:
                    geo_data.append({
                        "city":        row.segments.geo_target_city,
                        "clicks":      row.metrics.clicks,
                        "impressions": row.metrics.impressions,
                        "spend":       round(row.metrics.cost_micros / 1_000_000, 2),
                        "conversions": int(row.metrics.conversions)
                    })
            result["geo"] = geo_data
            print(f"[GOOGLE] Geo: {len(geo_data)} ciudades")
        except Exception as e:
            print(f"[GOOGLE] Geo error (no critico): {e}")

        # 3. KEYWORDS
        q_kw = f"""
            SELECT ad_group_criterion.keyword.text,
                   ad_group_criterion.keyword.match_type,
                   ad_group_criterion.quality_info.quality_score,
                   metrics.clicks, metrics.impressions,
                   metrics.cost_micros, metrics.ctr,
                   metrics.conversions, metrics.cost_per_conversion
            FROM keyword_view
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN_FILTER}%'
              AND ad_group_criterion.status != 'REMOVED'
            ORDER BY metrics.clicks DESC
            LIMIT 20
        """
        try:
            kw_data = []
            for batch in ga.search_stream(customer_id=cid, query=q_kw):
                for row in batch.results:
                    kw_data.append({
                        "keyword":       row.ad_group_criterion.keyword.text,
                        "match_type":    str(row.ad_group_criterion.keyword.match_type),
                        "quality_score": row.ad_group_criterion.quality_info.quality_score,
                        "clicks":        row.metrics.clicks,
                        "impressions":   row.metrics.impressions,
                        "spend":         round(row.metrics.cost_micros / 1_000_000, 2),
                        "ctr":           round(row.metrics.ctr * 100, 2),
                        "conversions":   int(row.metrics.conversions)
                    })
            result["keywords"] = kw_data
            print(f"[GOOGLE] Keywords: {len(kw_data)} palabras")
        except Exception as e:
            print(f"[GOOGLE] Keywords error (no critico): {e}")

        # 4. TERMINOS DE BUSQUEDA
        q_st = f"""
            SELECT campaign.name,
                   search_term_view.search_term,
                   search_term_view.status,
                   metrics.clicks, metrics.impressions,
                   metrics.cost_micros, metrics.conversions
            FROM search_term_view
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN_FILTER}%'
            ORDER BY metrics.clicks DESC
            LIMIT 25
        """
        try:
            st_data = []
            for batch in ga.search_stream(customer_id=cid, query=q_st):
                for row in batch.results:
                    st_data.append({
                        "term":        row.search_term_view.search_term,
                        "status":      str(row.search_term_view.status),
                        "clicks":      row.metrics.clicks,
                        "impressions": row.metrics.impressions,
                        "spend":       round(row.metrics.cost_micros / 1_000_000, 2),
                        "conversions": int(row.metrics.conversions)
                    })
            result["search_terms"] = st_data
            print(f"[GOOGLE] Terminos: {len(st_data)} busquedas")
        except Exception as e:
            print(f"[GOOGLE] Search terms error (no critico): {e}")

        # 5. ANUNCIOS RSA
        q_ads = f"""
            SELECT campaign.name,
                   ad_group_ad.ad.name,
                   ad_group_ad.status,
                   metrics.clicks, metrics.impressions,
                   metrics.ctr, metrics.cost_micros,
                   metrics.conversions
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{date_from}' AND '{date_to}'
              AND campaign.name LIKE '%{GOOGLE_CAMPAIGN_FILTER}%'
              AND ad_group_ad.status != 'REMOVED'
            ORDER BY metrics.clicks DESC
            LIMIT 10
        """
        try:
            ads_data = []
            for batch in ga.search_stream(customer_id=cid, query=q_ads):
                for row in batch.results:
                    ads_data.append({
                        "name":        row.ad_group_ad.ad.name,
                        "status":      str(row.ad_group_ad.status),
                        "clicks":      row.metrics.clicks,
                        "impressions": row.metrics.impressions,
                        "ctr":         round(row.metrics.ctr * 100, 2),
                        "spend":       round(row.metrics.cost_micros / 1_000_000, 2),
                        "conversions": int(row.metrics.conversions)
                    })
            result["ads"] = ads_data
            print(f"[GOOGLE] Anuncios: {len(ads_data)} activos")
        except Exception as e:
            print(f"[GOOGLE] Ads error (no critico): {e}")

        if result["clicks"] > 0:
            result["ctr"] = round(result["clicks"] / max(result["impressions"], 1) * 100, 2)
            result["cpc"] = round(result["spend"] / result["clicks"], 2)
        if result["conversions"] > 0:
            result["cpa"] = round(result["spend"] / result["conversions"], 2)

        print(f"[GOOGLE] TOTAL: ${result['spend']:,.2f} | {result['clicks']} clics | {result['conversions']} conv.")

    except Exception as e:
        result["status"] = f"error: {e}"
        print(f"[GOOGLE] ERROR: {e}")
    return result


def pull_meta(date_from, date_to):
    print(f"[META] Jalando '{META_CAMPAIGN_FILTER}' | {date_from} -> {date_to}")
    result = dict(
        campaign=META_CAMPAIGN_FILTER, spend=0.0, impressions=0,
        clicks=0, cpm=0.0, cpc=0.0, ctr=0.0, reach=0,
        wa_conversations=0, cost_per_wa=0.0,
        status="ok", date_from=date_from, date_to=date_to,
        ad_sets=[]
    )
    if not META_TOKEN:
        result["status"] = "error: no token"
        return result

    base = "https://graph.facebook.com/v18.0"
    try:
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

        for camp in campaigns:
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

            try:
                r3 = requests.get(
                    f"{base}/{camp['id']}/adsets"
                    f"?fields=id,name,status&access_token={META_TOKEN}",
                    timeout=30
                )
                r3.raise_for_status()
                for adset in r3.json().get("data", []):
                    r4 = requests.get(
                        f"{base}/{adset['id']}/insights"
                        f"?fields=spend,impressions,clicks,cpm,ctr,reach,actions"
                        f"&time_range={{\"since\":\"{date_from}\",\"until\":\"{date_to}\"}}"
                        f"&access_token={META_TOKEN}",
                        timeout=30
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
                        result["ad_sets"].append({
                            "name":        adset["name"],
                            "status":      adset["status"],
                            "spend":       spend_as,
                            "impressions": int(ad.get("impressions", 0)),
                            "clicks":      int(ad.get("clicks", 0)),
                            "reach":       int(ad.get("reach", 0)),
                            "cpm":         float(ad.get("cpm", 0)),
                            "ctr":         float(ad.get("ctr", 0)),
                            "wa_convs":    wa_convs,
                            "cost_per_wa": round(spend_as / wa_convs, 2) if wa_convs > 0 else 0
                        })
            except Exception as e:
                print(f"[META] Ad sets error (no critico): {e}")

        if result["clicks"] > 0:
            result["cpc"] = round(result["spend"] / result["clicks"], 2)
        if result["wa_conversations"] > 0:
            result["cost_per_wa"] = round(result["spend"] / result["wa_conversations"], 2)

        print(f"[META] ${result['spend']:,.2f} | {result['wa_conversations']} WA convs. | {len(result['ad_sets'])} ad sets")

    except Exception as e:
        result["status"] = f"error: {e}"
        print(f"[META] ERROR: {e}")
    return result


def update_sheet(year, month, g, m):
    if not SHEET_API_URL:
        print("[SHEET] SHEET_API_URL no configurada, saltando.")
        return
    meses = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
             "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
    payload = {
        "action":              "actualizar_arq",
        "mes":                 meses[month - 1],
        "anio":                year,
        "google_spend":        g["spend"],
        "google_clicks":       g["clicks"],
        "google_impressions":  g["impressions"],
        "google_ctr":          g["ctr"],
        "google_cpc":          g["cpc"],
        "google_conversions":  g["conversions"],
        "google_cpa":          g["cpa"],
        "google_geo":          json.dumps(g.get("geo", [])),
        "google_keywords":     json.dumps(g.get("keywords", [])),
        "google_search_terms": json.dumps(g.get("search_terms", [])),
        "google_ads":          json.dumps(g.get("ads", [])),
        "meta_spend":          m["spend"],
        "meta_impressions":    m["impressions"],
        "meta_clicks":         m["clicks"],
        "meta_cpm":            m["cpm"],
        "meta_ctr":            m["ctr"],
        "meta_reach":          m["reach"],
        "meta_wa_convs":       m["wa_conversations"],
        "meta_cost_per_wa":    m["cost_per_wa"],
        "meta_ad_sets":        json.dumps(m.get("ad_sets", [])),
        "total_spend":         round(g["spend"] + m["spend"], 2),
        "google_status":       g["status"],
        "meta_status":         m["status"],
        "timestamp":           datetime.now().isoformat()
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
    print(f"  Google (ARQ_2025_B) : ${g['spend']:,.2f} MXN | {g['clicks']} clics | {g['conversions']} conv.")
    print(f"  Keywords            : {len(g.get('keywords',[]))} | Search terms: {len(g.get('search_terms',[]))}")
    print(f"  Geo                 : {len(g.get('geo',[]))} ciudades")
    print(f"  Meta (2024,ARQ,QRO) : ${m['spend']:,.2f} MXN | {m['wa_conversations']} WA | {len(m.get('ad_sets',[]))} ad sets")
    print(f"  TOTAL               : ${g['spend'] + m['spend']:,.2f} MXN")
    print(f"{'='*50}\n")
