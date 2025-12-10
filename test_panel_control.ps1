# Test Panel de Control flags locally
# Simulates GitHub Actions environment variables

# Load credentials
$env:GCP_JSON = Get-Content "Secret\datapipeline-stage-089124019a24.json" -Raw

# Panel de Control flags (change these to test different scenarios)
$env:RUN_BACKUP = "true"      # B4 - Save to Data Lake Historic
$env:RUN_ML = "false"         # B5 - Force ML Retrain
$env:SEND_ALERTS = "false"    # B6 - Send Email Alerts (false for local testing)

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "Testing Panel de Control Configuration" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "RUN_BACKUP (B4):   $env:RUN_BACKUP" -ForegroundColor Yellow
Write-Host "RUN_ML (B5):       $env:RUN_ML" -ForegroundColor Yellow
Write-Host "SEND_ALERTS (B6):  $env:SEND_ALERTS" -ForegroundColor Yellow
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# Run pipeline
python etl_pipeline.py
