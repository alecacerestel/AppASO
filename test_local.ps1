# Load credentials and run pipeline
$env:GCP_JSON = (Get-Content "Secret\datapipeline-stage-089124019a24.json" -Raw)
C:/Users/Alejandro/AppData/Local/Microsoft/WindowsApps/python3.11.exe etl_pipeline.py
