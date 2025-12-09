# Local testing script for AppASO ETL Pipeline
# This script sets up environment variables and runs the pipeline locally

# Load environment variables from .env file
if (Test-Path ".env") {
    Write-Host "Loading environment variables from .env file..." -ForegroundColor Green
    Get-Content .env | ForEach-Object {
        if ($_ -match '^([^=]+)=(.*)$') {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
            Write-Host "  Set $name" -ForegroundColor Gray
        }
    }
} else {
    Write-Host "ERROR: .env file not found!" -ForegroundColor Red
    Write-Host "Please copy .env.example to .env and configure it first" -ForegroundColor Yellow
    exit 1
}

Write-Host "`nRunning ETL pipeline locally..." -ForegroundColor Cyan
Write-Host "=" * 70

# Run the pipeline
python etl_pipeline.py

Write-Host "`n" + "=" * 70
if ($LASTEXITCODE -eq 0) {
    Write-Host "Pipeline completed successfully!" -ForegroundColor Green
} else {
    Write-Host "Pipeline failed with exit code: $LASTEXITCODE" -ForegroundColor Red
}
