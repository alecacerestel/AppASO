/**
 * Servir le fichier HTML
 */
function doGet() {
  return HtmlService.createTemplateFromFile('index')
    .evaluate()
    .setTitle('Dashboard ASO & ROI')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/**
 * Fonction principale
 */
function getDashboardData(startDateStr, endDateStr) {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  
  var start = new Date(startDateStr);
  var end = new Date(endDateStr);
  
  var startPrev = new Date(start);
  startPrev.setFullYear(start.getFullYear() - 1);
  var endPrev = new Date(end);
  endPrev.setFullYear(end.getFullYear() - 1);

  var keywordsData = getSheetData(ss, 'KEYWORDS');
  var installsData = getSheetData(ss, 'INSTALLS');
  var usersData = getSheetData(ss, 'USERS');
  var forecastData = getSheetData(ss, 'FORECAST'); 

  var kpis = calculateKPIs(keywordsData, installsData, usersData, start, end, startPrev, endPrev);
  var charts = prepareChartData(keywordsData, installsData, usersData, start, end);
  var forecast = processForecastData(forecastData);

  return {
    kpis: kpis,
    charts: charts,
    forecast: forecast
  };
}

/**
 * Helper: Lire les données
 */
function getSheetData(ss, sheetName) {
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) return [];
  var data = sheet.getDataRange().getValues();
  var headers = data.shift();
  
  return data.map(function(row) {
    var obj = {};
    headers.forEach(function(header, i) {
      obj[header] = row[i];
    });
    if (obj['Date'] && typeof obj['Date'] === 'string') {
      var parts = obj['Date'].split('/');
      obj['DateObj'] = new Date(parts[2], parts[1] - 1, parts[0]);
    } else if (obj['Date'] instanceof Date) {
      obj['DateObj'] = obj['Date'];
    }
    return obj;
  });
}

/**
 * Calcul des KPIs
 */
function calculateKPIs(keywords, installs, users, start, end, startPrev, endPrev) {
  function filterByDate(data, s, e) {
    return data.filter(r => r.DateObj >= s && r.DateObj <= e);
  }

  var kwCurr = filterByDate(keywords, start, end);
  var kwPrev = filterByDate(keywords, startPrev, endPrev);

  function getAvgTop30(data) {
    if (data.length === 0) return 0;
    var sum = data.reduce((acc, r) => {
      var val = (Number(r['Rank_1']) || 0) + (Number(r['Rank_2_3']) || 0) + (Number(r['Rank_4_10']) || 0) + (Number(r['Rank_11_30']) || 0);
      return acc + val;
    }, 0);
    return Math.round(sum / data.length);
  }
  
  var instCurr = filterByDate(installs, start, end);
  var instPrev = filterByDate(installs, startPrev, endPrev);
  
  function getSumInstalls(data) {
    return data.reduce((acc, r) => acc + (Number(r['Installs']) || 0), 0);
  }

  var userCurr = filterByDate(users, start, end);
  var userPrev = filterByDate(users, startPrev, endPrev);

  function getAvgUsers(data) {
    if (data.length === 0) return 0;
    var sum = data.reduce((acc, r) => acc + (Number(r['Active_Users']) || 0), 0);
    return Math.round(sum / data.length);
  }

  return {
    keywords: { current: getAvgTop30(kwCurr), prev: getAvgTop30(kwPrev) },
    installs: { current: getSumInstalls(instCurr), prev: getSumInstalls(instPrev) },
    users:    { current: getAvgUsers(userCurr), prev: getAvgUsers(userPrev) }
  };
}

/**
 * Préparation des données pour Chart.js
 */
function prepareChartData(keywords, installs, users, start, end) {
  var formatDate = d => Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");

  function getKeywordSeries(platform) {
    var filtered = keywords.filter(r => r.DateObj >= start && r.DateObj <= end && r.Platform === platform);
    filtered.sort((a, b) => a.DateObj - b.DateObj);
    return filtered.map(r => {
      var top30 = (Number(r['Rank_1']) || 0) + (Number(r['Rank_2_3']) || 0) + (Number(r['Rank_4_10']) || 0) + (Number(r['Rank_11_30']) || 0);
      return { x: formatDate(r.DateObj), y: top30 };
    });
  }

  function getComparisonData(sourceData, platform, valueField) {
    var dataCurrent = sourceData.filter(r => r.DateObj.getFullYear() === end.getFullYear() && r.Platform === platform);
    var dataPrev = sourceData.filter(r => r.DateObj.getFullYear() === (end.getFullYear() - 1) && r.Platform === platform);

    var prevSeries = dataPrev.map(r => {
      var d = new Date(r.DateObj);
      d.setFullYear(end.getFullYear()); 
      return { x: formatDate(d), y: Number(r[valueField]) };
    });
    
    var currSeries = dataCurrent.map(r => {
      return { x: formatDate(r.DateObj), y: Number(r[valueField]) };
    });
    
    prevSeries.sort((a, b) => new Date(a.x) - new Date(b.x));
    currSeries.sort((a, b) => new Date(a.x) - new Date(b.x));

    return { current: currSeries, prev: prevSeries };
  }

  return {
    keywordsApple: getKeywordSeries('Apple'),
    keywordsGoogle: getKeywordSeries('Google'),
    installsGoogle: getComparisonData(installs, 'Google', 'Installs'),
    installsApple: getComparisonData(installs, 'Apple', 'Installs'),
    usersGoogle: getComparisonData(users, 'Google', 'Active_Users'), 
    usersApple: getComparisonData(users, 'Apple', 'Active_Users')
  };
}

/**
 * FORECAST
 */
function processForecastData(forecastData) {
  var targetData = forecastData.filter(r => {
    return r.DateObj.getFullYear() === 2026 && r.DateObj.getMonth() === 0;
  });
  
  targetData.sort((a, b) => a.DateObj - b.DateObj);

  var total = 0;
  var googleSum = 0;
  var appleSum = 0;
  
  var dailyMap = {}; 

  targetData.forEach(r => {
    var day = r.DateObj.getDate(); 
    var val = Number(r['Installs']) || 0;
    var plat = r['Platform'];

    if (!dailyMap[day]) dailyMap[day] = { google: 0, apple: 0 };
    
    if (plat === 'Google') {
      googleSum += val;
      dailyMap[day].google += val;
    } else if (plat === 'Apple') {
      appleSum += val;
      dailyMap[day].apple += val;
    }
    total += val;
  });

  var days = [];
  var googleDaily = [];
  var appleDaily = [];

  Object.keys(dailyMap).sort((a,b) => a-b).forEach(d => {
    days.push(d);
    googleDaily.push(dailyMap[d].google);
    appleDaily.push(dailyMap[d].apple);
  });

  return { 
    total: total, 
    google: googleSum, 
    apple: appleSum,
    chart: {
      labels: days,
      google: googleDaily,
      apple: appleDaily
    }
  };
}