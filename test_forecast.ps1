# Test ML Forecasting Pipeline Locally
# This script runs the forecasting pipeline and uploads to Google Sheets

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "ML FORECASTING PIPELINE - LOCAL TEST" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Load Google credentials
$env:GCP_JSON = (Get-Content "Secret\datapipeline-stage-089124019a24.json" -Raw)

# Run the forecasting pipeline
C:/Users/Alejandro/AppData/Local/Microsoft/WindowsApps/python3.11.exe ML/forecast_pipeline.py

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "TEST COMPLETED" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Check results:" -ForegroundColor Green
Write-Host "  - Local backup: ML/MASTER_DATA_FORECAST.csv" -ForegroundColor Yellow
Write-Host "  - Google Sheets: MASTER_DATA_CLEAN > FORECAST worksheet" -ForegroundColor Yellow
