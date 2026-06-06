/**
 * e.ARGAMA Marketing Intelligence — Google Apps Script v2
 * Guarda el payload completo (periodos, alertas, saldos) y lo sirve al dashboard.
 *
 * Tras actualizar este codigo:
 *   Implementar → Administrar implementaciones → lapiz → Nueva version → Implementar
 */

var SHEET_NAME_V2 = "ARQ_Dashboard_v2";

// ── POST ─────────────────────────────────────────────────────────────────────

function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    if (data.action === "actualizar_arq_v2") return guardarV2(data);
    if (data.action === "actualizar_arq")    return guardarV1Legacy(data);
    return resp({ error: "Accion no reconocida: " + data.action });
  } catch (err) {
    return resp({ error: err.toString() });
  }
}

// ── GET ───────────────────────────────────────────────────────────────────────

function doGet(e) {
  try {
    return ContentService
      .createTextOutput(JSON.stringify(leerUltimo()))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ error: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ── GUARDAR V2 ────────────────────────────────────────────────────────────────

function guardarV2(data) {
  var ss    = getSheet();
  var sheet = ss.getSheetByName(SHEET_NAME_V2) || ss.insertSheet(SHEET_NAME_V2);

  var COLS = [
    "Timestamp", "JSON_Data", "Alert_Count",
    "G7_Spend", "G7_CPC", "G7_Conv",
    "M7_Spend", "M7_Freq", "M7_WA",
    "G30_Spend", "M30_Spend", "Total30_Spend"
  ];

  if (sheet.getLastRow() === 0) {
    sheet.appendRow(COLS);
    sheet.setFrozenRows(1);
    sheet.setColumnWidth(2, 60); // JSON_Data column narrow — data is large
  }

  var periods = data.periods || {};
  var g7  = ((periods["7d"]  || {}).google) || {};
  var m7  = ((periods["7d"]  || {}).meta)   || {};
  var g30 = ((periods["30d"] || {}).google) || {};
  var m30 = ((periods["30d"] || {}).meta)   || {};

  var fila = [
    data.timestamp          || new Date().toISOString(),
    JSON.stringify(data),
    (data.alerts || []).length,
    g7.spend               || 0,
    g7.cpc                 || 0,
    g7.conversions         || 0,
    m7.spend               || 0,
    m7.frequency           || 0,
    m7.wa_conversations    || 0,
    g30.spend              || 0,
    m30.spend              || 0,
    (g30.spend || 0) + (m30.spend || 0)
  ];

  sheet.appendRow(fila);

  // Mantener solo las ultimas 90 filas de datos (+ 1 header)
  var maxRows = 91;
  if (sheet.getLastRow() > maxRows) {
    sheet.deleteRow(2);
  }

  return resp({ ok: true, fila: sheet.getLastRow(), alerts: (data.alerts || []).length });
}

// ── LEER ULTIMO ───────────────────────────────────────────────────────────────

function leerUltimo() {
  var ss    = getSheet();
  var sheet = ss.getSheetByName(SHEET_NAME_V2);
  if (!sheet || sheet.getLastRow() < 2) {
    return { error: "no data", status: "empty" };
  }

  var lastRow = sheet.getLastRow();
  var jsonStr = sheet.getRange(lastRow, 2).getValue(); // columna JSON_Data

  try {
    var parsed = JSON.parse(jsonStr);
    parsed._sheet_row   = lastRow;
    parsed._sheet_total = lastRow - 1;
    return parsed;
  } catch (e) {
    return { error: "parse error: " + e.toString() };
  }
}

// ── LEGACY V1 (soporte backward compat) ──────────────────────────────────────

function guardarV1Legacy(data) {
  // Guarda en hoja separada para no mezclar formatos
  var ss    = getSheet();
  var sheet = ss.getSheetByName("ARQ_Dashboard") || ss.insertSheet("ARQ_Dashboard");
  var COLS  = [
    "Timestamp","Anio","Mes","Total Spend","Google Spend","Google Clicks",
    "Google Impresiones","Google CTR","Google CPC","Google Conversiones","Google CPA",
    "Meta Spend","Meta Impresiones","Meta Clicks","Meta CPM","Meta CTR","Meta Reach",
    "Meta WA Convs","Meta Costo WA","Google Status","Meta Status","Google Geo",
    "Google Keywords","Google Search Terms","Google Ads","Meta Ad Sets"
  ];
  if (sheet.getLastRow() === 0) { sheet.appendRow(COLS); sheet.setFrozenRows(1); }

  var fila = [
    data.timestamp || new Date().toISOString(),
    data.anio || "", data.mes || "",
    data.total_spend || 0, data.google_spend || 0, data.google_clicks || 0,
    data.google_impressions || 0, data.google_ctr || 0, data.google_cpc || 0,
    data.google_conversions || 0, data.google_cpa || 0,
    data.meta_spend || 0, data.meta_impressions || 0, data.meta_clicks || 0,
    data.meta_cpm || 0, data.meta_ctr || 0, data.meta_reach || 0,
    data.meta_wa_convs || 0, data.meta_cost_per_wa || 0,
    data.google_status || "ok", data.meta_status || "ok",
    data.google_geo || "[]", data.google_keywords || "[]",
    data.google_search_terms || "[]", data.google_ads || "[]",
    data.meta_ad_sets || "[]"
  ];
  sheet.appendRow(fila);
  return resp({ ok: true, version: "v1_legacy", fila: sheet.getLastRow() });
}

// ── UTILS ─────────────────────────────────────────────────────────────────────

function getSheet() {
  var files = DriveApp.getFilesByName("CasaClubARQ - Marketing Data");
  if (files.hasNext()) return SpreadsheetApp.openById(files.next().getId());
  return SpreadsheetApp.create("CasaClubARQ - Marketing Data");
}

function resp(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
