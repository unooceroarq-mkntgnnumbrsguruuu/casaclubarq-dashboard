/**
 * CASA CLUB ARQ — Google Apps Script
 * 
 * COMO USARLO:
 * 1. Ve a script.google.com -> Nuevo proyecto
 * 2. Pega este codigo completo
 * 3. Guarda el proyecto con nombre "CasaClubARQ - Dashboard API"
 * 4. Ve a Implementar -> Nueva implementacion -> Aplicacion web
 *    - Ejecutar como: Yo (tu cuenta)
 *    - Quien tiene acceso: Cualquier persona
 * 5. Copia la URL que te da -> esa es tu SHEET_API_URL_ARQ para el secret de GitHub
 * 6. Abre el Google Sheet que se creara automaticamente con el primer POST
 *    (o crea uno manualamente y reemplaza SHEET_NAME)
 *
 * ENDPOINTS:
 *   POST -> escribe datos del pull de ads
 *   GET  -> devuelve JSON con todos los datos (para el dashboard)
 */

var SHEET_NAME = "ARQ_Dashboard";

function doPost(e) {
  try {
    var data = JSON.parse(e.postData.contents);
    var action = data.action;

    if (action === "actualizar_arq") {
      return guardarDatos(data);
    }

    return respuesta({ error: "Accion no reconocida: " + action });
  } catch (err) {
    return respuesta({ error: err.toString() });
  }
}

function doGet(e) {
  // El dashboard llama este endpoint para leer los datos
  try {
    var todos = leerTodosLosDatos();
    return ContentService
      .createTextOutput(JSON.stringify(todos))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService
      .createTextOutput(JSON.stringify({ error: err.toString() }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function guardarDatos(data) {
  var ss = obtenerSheet();
  var sheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);

  // Encabezados si la hoja esta vacia
  if (sheet.getLastRow() === 0) {
    sheet.appendRow([
      "Timestamp", "Año", "Mes",
      "Google Spend", "Google Clicks", "Google Impresiones",
      "Google CTR", "Google CPC", "Google Conversiones", "Google CPA",
      "Meta Spend", "Meta Impresiones", "Meta Clicks",
      "Meta CPM", "Meta CTR", "Meta Reach",
      "Meta WA Convs", "Meta Costo/WA",
      "Total Spend", "Google Status", "Meta Status"
    ]);
    sheet.setFrozenRows(1);
  }

  // Buscar si ya existe una fila para este mes/año (actualizar en vez de duplicar)
  var lastRow = sheet.getLastRow();
  var filaExistente = -1;
  if (lastRow > 1) {
    var anios = sheet.getRange(2, 2, lastRow - 1, 1).getValues();
    var meses = sheet.getRange(2, 3, lastRow - 1, 1).getValues();
    for (var i = 0; i < anios.length; i++) {
      if (anios[i][0] == data.anio && meses[i][0] == data.mes) {
        filaExistente = i + 2;
        break;
      }
    }
  }

  var fila = [
    data.timestamp || new Date().toISOString(),
    data.anio,
    data.mes,
    data.google_spend       || 0,
    data.google_clicks      || 0,
    data.google_impressions || 0,
    data.google_ctr         || 0,
    data.google_cpc         || 0,
    data.google_conversions || 0,
    data.google_cpa         || 0,
    data.meta_spend         || 0,
    data.meta_impressions   || 0,
    data.meta_clicks        || 0,
    data.meta_cpm           || 0,
    data.meta_ctr           || 0,
    data.meta_reach         || 0,
    data.meta_wa_convs      || 0,
    data.meta_cost_per_wa   || 0,
    data.total_spend        || 0,
    data.google_status      || "ok",
    data.meta_status        || "ok"
  ];

  if (filaExistente > 0) {
    sheet.getRange(filaExistente, 1, 1, fila.length).setValues([fila]);
    return respuesta({ ok: true, accion: "actualizado", fila: filaExistente });
  } else {
    sheet.appendRow(fila);
    return respuesta({ ok: true, accion: "creado", fila: sheet.getLastRow() });
  }
}

function leerTodosLosDatos() {
  var ss = obtenerSheet();
  var sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet || sheet.getLastRow() < 2) {
    return { data: [], status: "empty" };
  }

  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  var rows    = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();

  var result = rows.map(function(row) {
    var obj = {};
    headers.forEach(function(h, i) { obj[h] = row[i]; });
    return obj;
  });

  // Ultimo registro (mes mas reciente) para el dashboard
  var ultimo = result[result.length - 1] || {};

  return {
    data:    result,
    latest:  ultimo,
    count:   result.length,
    updated: new Date().toISOString()
  };
}

function obtenerSheet() {
  // Intenta abrir el spreadsheet del script, o crea uno nuevo
  var files = DriveApp.getFilesByName("CasaClubARQ - Marketing Data");
  if (files.hasNext()) {
    var file = files.next();
    return SpreadsheetApp.openById(file.getId());
  }
  // Crear nuevo spreadsheet
  var ss = SpreadsheetApp.create("CasaClubARQ - Marketing Data");
  return ss;
}

function respuesta(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
