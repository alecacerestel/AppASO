# AppASO - ETL Pipeline for ASO Data

ETL pipeline that processes App Store Optimization data from Apple and Google Play to compare performance before and after agency engagement.

## What It Does

Processes 6 Excel files from Google Drive containing:
- Keyword rankings (Apple + Google)
- App installs (Apple + Google)
- Active users (Apple + Google)

Outputs unified data with business logic classification:
- **Pre-Agencia**: Data before July 15, 2025
- **Con-Agencia**: Data from July 15, 2025 onwards

## Built For

Compare app performance metrics before and after the agency started working on ASO strategy.

## How It Works

```
Control Panel Check (B3 = ON?)
         ↓
   Extract (Download 6 Excel files)
         ↓
   Transform (Unify Apple + Google, add Stage column)
         ↓
   Load (Write to 3 Google Sheets + Historical backup)
```

**Execution:**
- Automatic: Daily at 9:00 AM Paris time
- Manual: GitHub Actions > Run workflow

## Setup Requirements

### 1. Google Drive Structure

Share these with service account email (found in GCP_JSON):

- `AppASO/` root folder
- `RAW/` folder (ID: 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL)
  - APPLE motcles.xlsx
  - GOOGLE motcles.xlsx
  - Installs Apple.xlsx
  - Installs Google.xlsx
  - Utilisateurs connectés Apple.xlsx
  - Utilisateurs connectés Google.xlsx
- `00_Control_Panel` sheet with "Config" worksheet (B3 = ON/OFF)
- `MASTER_DATA_CLEAN` sheet (worksheets auto-created)
- `02_Data_Lake_Historic/YYYY/MM_Month/` folders

### 2. GitHub Secrets

Repository Settings > Secrets and variables > Actions

| Secret | Description |
|--------|-------------|
| `GCP_JSON` | Service account JSON credentials |
| `EMAIL_USER` | Gmail address for alerts |
| `EMAIL_PASSWORD` | Gmail App Password |
| `EMAIL_RECIPIENT` | Email to receive error notifications |

## Implementation Status

### Completed
- ✅ Authentication with Google Cloud
- ✅ Control Panel check (cell B3)
- ✅ Error handling and email notifications
- ✅ GitHub Actions workflow setup

### In Progress
- 🔄 Data extraction from Google Drive

### Pending
- ⏳ Data transformation (column mapping, Stage logic)
- ⏳ Data loading (3 worksheets + Data Lake backup)

## Project Structure

```
src/
├── config/settings.py       # Configuration constants
├── services/
│   ├── auth.py             # Google Cloud authentication
│   └── drive.py            # Drive/Sheets operations
├── etl/
│   ├── extract.py          # Download Excel files
│   ├── transform.py        # Clean and unify data
│   ├── load.py             # Write to Sheets and backup
│   ├── pipeline.py         # Orchestration
│   └── column_mapping.py   # Apple/Google standardization
└── utils/
    ├── notifications.py    # Email alerts
    └── error_handler.py    # Error logging

etl_pipeline.py             # Main execution script
```

## Local Testing

### Setup

1. **Install dependencies:**
```powershell
pip install -r requirements.txt
```

2. **Add credentials:**
   - Create `Secret/` folder
   - Download service account JSON from Google Cloud Console
   - Save as `Secret/datapipeline-stage-089124019a24.json`

3. **Run pipeline:**
```powershell
# Set credentials and run
$env:GCP_JSON = (Get-Content "Secret\datapipeline-stage-089124019a24.json" -Raw)
python etl_pipeline.py
```

Or use the test script:
```powershell
.\test_local.ps1
```

### Manual Execution from GitHub

1. Go to repository **Actions** tab
2. Select **Daily ETL Pipeline**
3. Click **Run workflow** button
4. Click green **Run workflow** button

### Automatic Execution

Pipeline runs **automatically every day at 9:00 AM Paris time** via GitHub Actions schedule.

## Control Pipeline Execution

**Enable:** Set cell B3 in `00_Control_Panel` to `ON` or `TRUE`

**Disable:** Set cell B3 to `OFF` or `FALSE`

## Troubleshooting

| Error | Solution |
|-------|----------|
| Pipeline is DISABLED | Set B3 to ON in 00_Control_Panel |
| Root folder not found | Share AppASO folder with service account |
| Config worksheet not found | Rename first sheet to "Config" |
| RAW folder not found | Share folder ID 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL |

## License

Internal project
