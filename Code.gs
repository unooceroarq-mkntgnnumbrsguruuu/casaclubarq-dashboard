/**
 * CASA CLUB ARQ — Google Apps Script v2
 * Guarda y sirve TODOS los datos: metricas, geo, keywords,
 * search terms, anuncios y ad sets.
 *
 * Tras actualizar este codigo:
 *   Implementar → Administrar implementaciones → lapiz → Nueva version → Implementar
 */

var SHEET_NAME = "ARQ_Dashboard";

var COLS = [
  "Timestamp","Anio","Mes",
  "Total Spend","Google Spend","Google Clicks","Google Impresiones",
  "Google CTR","Google CPC","Google Conversiones","Google CPA",
  "Meta Spend","Meta Impresiones","Meta Clicks","Meta CPM",
  "Meta CTR","Meta Reach","Meta WA Convs","Meta Costo WA",
  "Google Status","Meta Status",
  "Google Geo","Google Keywords","Google Search Terms","Google Ads",
  "Meta Ad Sets"
];

function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    if (data.action === "actualizar_arq") return guardarDatos(data);
    return resp({ error: "Accion no reconocida: " + data.action });
  } catch (err) {
    return resp({ error: err.toString() });
  }
}

function doGet(e) {
  try {
    return ContentService
      .createTextOutput(JSON.stringify(leerDatos()))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ error: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function guardarDatos(data) {
  var ss    = getSheet();
  var sheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);

  if (sheet.getLastRow() === 0) {
    sheet.appendRow(COLS);
    sheet.setFrozenRows(1);
  }

  var mes  = data.mes  || "";
  var anio = data.anio || "";
  var filaExistente = -1;
  var lastRow = sheet.getLastRow();

  if (lastRow > 1) {
    var anios = sheet.getRange(2, COLS.indexOf("Anio") + 1, lastRow - 1, 1).getValues();
    var meses = sheet.getRange(2, COLS.indexOf("Mes") + 1,  lastRow - 1, 1).getValues();
    for (var i = 0; i < anios.length; i++) {
      if (String(anios[i][0]) == String(anio) && meses[i][0] == mes) {
        filaExistente = i + 2;
        break;
      }
    }
  }

  var fila = [
    data.timestamp           || new Date().toISOString(),
    anio, mes,
    data.total_spend         || 0,
    data.google_spend        || 0,
    data.google_clicks       || 0,
    data.google_impressions  || 0,
    data.google_ctr          || 0,
    data.google_cpc          || 0,
    data.google_conversions  || 0,
    data.google_cpa          || 0,
    data.meta_spend          || 0,
    data.meta_impressions    || 0,
    data.meta_clicks         || 0,
    data.meta_cpm            || 0,
    data.meta_ctr            || 0,
    data.meta_reach          || 0,
    data.meta_wa_convs       || 0,
    data.meta_cost_per_wa    || 0,
    data.google_status       || "ok",
    data.meta_status         || "ok",
    data.google_geo          || "[]",
    data.google_keywords     || "[]",
    data.google_search_terms || "[]",
    data.google_ads          || "[]",
    data.meta_ad_sets        || "[]"
  ];

  if (filaExistente > 0) {
    sheet.getRange(filaExistente, 1, 1, fila.length).setValues([fila]);
    return resp({ ok: true, accion: "actualizado", fila: filaExistente });
  } else {
    sheet.appendRow(fila);
    return resp({ ok: true, accion: "creado", fila: sheet.getLastRow() });
  }
}

function leerDatos() {
  var ss    = getSheet();
  var sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet || sheet.getLastRow() < 2) return { data: [], latest: {}, status: "empty" };

  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var rows    = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
  var jsonCols = ["Google Geo","Google Keywords","Google Search Terms","Google Ads","Meta Ad Sets"];

  var result = rows.map(function(row) {
    var obj = {};
    headers.forEach(function(h, i) { obj[h] = row[i]; });
    jsonCols.forEach(function(k) {
      if (obj[k] && typeof obj[k] === "string" && obj[k].length > 2) {
        try { obj[k] = JSON.parse(obj[k]); } catch(e) { obj[k] = []; }
      } else { obj[k] = []; }
    });
    return obj;
  });

  return {
    data:    result,
    latest:  result[result.length - 1] || {},
    count:   result.length,
    updated: new Date().toISOString()
  };
}

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
