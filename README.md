# AppASO - ETL Pipeline for ASO Data Processing

Production-ready serverless ETL pipeline that runs on GitHub Actions and processes ASO (App Store Optimization) data from Google Play and Apple Store.

## Project Overview

This pipeline processes ASO data from 6 Excel files (3 data types x 2 platforms) and consolidates them into a clean, unified dataset for analysis in Looker Studio.

**Business Goal**: Compare app performance "Pre-Agency" (before July 15, 2025) vs "Con-Agency" (after agency started working).

## Project Structure

```
AppASO/
├── .github/workflows/
│   └── daily_etl.yml              # GitHub Actions (runs daily at 9 AM Paris time)
├── src/
│   ├── config/
│   │   └── settings.py            # Configuration and constants
│   ├── services/
│   │   ├── auth.py                # Google Cloud authentication
│   │   └── drive.py               # Google Drive/Sheets operations
│   ├── etl/
│   │   ├── extract.py             # Read Excel files from Drive RAW folder
│   │   ├── transform.py           # Clean, standardize, add business logic
│   │   ├── load.py                # Write to 3 Google Sheets + Data Lake
│   │   ├── pipeline.py            # ETL orchestration
│   │   └── column_mapping.py      # Apple/Google column standardization
│   └── utils/
│       ├── notifications.py       # Email alerts
│       └── error_handler.py       # Error logging
├── data/
│   ├── raw/                       # Local copies (for reference only)
│   └── processed/
├── etl_pipeline.py                # Main execution script
├── requirements.txt               # Python dependencies
└── README.md
```

## Data Sources

### RAW Folder in Google Drive
**Folder ID**: `1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL`

Contains 6 Excel files:

1. **APPLE motcles.xlsx** - Apple keyword rankings
2. **GOOGLE motcles.xlsx** - Google keyword rankings
3. **Installs Apple.xlsx** - Apple app installs
4. **Installs Google.xlsx** - Google Play installs
5. **Utilisateurs connectés Apple.xlsx** - Apple active users
6. **Utilisateurs connectés Google.xlsx** - Google active users

### Output Structure

**MASTER_DATA_CLEAN** Google Sheet with 3 worksheets:

1. **KEYWORDS** - Unified keyword rankings (Apple + Google)
   - Columns: Date, Rank_1, Rank_2_3, Rank_4_10, Rank_11_30, Rank_31_100, Rank_100_Plus, Platform, Stage

2. **INSTALLS** - Unified install data (Apple + Google)
   - Columns: Date, Installs, Platform, Stage

3. **USERS** - Unified active users (Apple + Google)
   - Columns: Date, Active_Users, Platform, Stage

**Stage Column Logic**:
- `Pre-Agencia` if Date < 2025-07-15
- `Con-Agencia` if Date >= 2025-07-15

### Historical Backup

Data Lake folder structure:
```
02_Data_Lake_Historic/
├── 2024/
│   ├── 01_January/
│   │   ├── keywords_20241015.csv
│   │   ├── installs_20241015.csv
│   │   └── users_20241015.csv
│   └── ...
└── 2025/
    └── ...
```

## Setup Instructions

### 1. Google Cloud Setup

1. Service Account already created with credentials in `Secret/datapipeline-stage-089124019a24.json`
2. Google Drive API and Google Sheets API enabled
3. Share the following with the service account email (Editor permissions):
   - `AppASO` root folder
   - RAW data folder (ID: 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL)

### 2. Control Panel

In Google Sheet `00_Control_Panel`:
- Sheet name: `Config`
- Cell `B3`: Set to `TRUE` or `ON` to enable pipeline
- Set to `FALSE` or `OFF` to disable pipeline

### 3. GitHub Secrets

Configure in repository Settings > Secrets and variables > Actions:

| Secret Name | Description |
|-------------|-------------|
| `GCP_JSON` | Complete JSON content from service account file |
| `EMAIL_USER` | Gmail address for sending alerts |
| `EMAIL_PASSWORD` | Gmail App Password (not regular password) |
| `EMAIL_RECIPIENT` | Email address to receive error alerts |

See `SECRETS_SETUP.md` for detailed instructions.

### 4. Local Testing (Optional)

```powershell
# Install dependencies
pip install -r requirements.txt

# Set environment variables (or create .env file)
$env:GCP_JSON = Get-Content "Secret\datapipeline-stage-089124019a24.json" | ConvertTo-Json
$env:EMAIL_USER = "your.email@gmail.com"
$env:EMAIL_PASSWORD = "your_app_password"
$env:EMAIL_RECIPIENT = "recipient@example.com"

# Run pipeline
python etl_pipeline.py
```

## How It Works

### Pipeline Execution Flow

1. **Authentication**
   - Authenticates with Google Cloud using service account credentials

2. **Control Check**
   - Reads `00_Control_Panel` sheet cell B3
   - If not "ON" or "TRUE", pipeline stops gracefully

3. **Extraction**
   - Lists all files in RAW folder (ID: 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL)
   - Downloads and reads 6 Excel files
   - Groups by data type: keywords, installs, users

4. **Transformation**
   - **Standardization**: Maps Apple/Google column names to unified schema
   - **Cleaning**: Handles nulls, formats dates (YYYY-MM-DD)
   - **Business Logic**: Adds "Stage" column (Pre-Agencia / Con-Agencia)
   - **Unification**: Combines Apple + Google for each data type

5. **Loading**
   - Writes to 3 worksheets in MASTER_DATA_CLEAN
   - Saves historical backup to Data Lake as CSV files

6. **Error Handling**
   - On failure: sends email alert with error details
   - Logs printed to GitHub Actions console

### Scheduling

- Runs daily at 9:00 AM Paris time via GitHub Actions
- Manual trigger available: Actions tab > "Daily ETL Pipeline" > "Run workflow"

## Column Mapping Reference

### Keywords
**Apple & Google** (identical structure):
- `DateTime` → `Date`
- `Rank 1` → `Rank_1`
- `Rank 2 - 3` → `Rank_2_3`
- etc.

### Installs
**Apple**:
- `Date` → `Date`
- `Installs Apple` → `Installs`

**Google**:
- `Date` → `Date`
- `Installs Google Play` → `Installs`

### Active Users
**Apple**:
- `Nom` → `Date` (skips first 2 metadata rows)
- `Courses U : Magasin en ligne` → `Active_Users`

**Google**:
- `Date` → `Date` (converts French date format)
- Long column name → `Active_Users`

## Troubleshooting

### Common Issues

1. **"Pipeline is DISABLED"**
   - Check cell B3 in `00_Control_Panel` sheet
   - Set to "ON" or "TRUE"

2. **"Root folder not found"**
   - Ensure `AppASO` folder is shared with service account
   - Check service account email in GCP_JSON

3. **"RAW folder not found"**
   - Verify folder ID: 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL
   - Ensure folder is shared with service account

4. **"File not found" errors**
   - Check file names match exactly in RAW folder
   - Files: "APPLE motcles", "GOOGLE motcles", etc.

5. **Email sending fails**
   - Use Gmail App Password, not regular password
   - Enable 2-Step Verification on Google account

## Data Quality Notes

- **French Date Parsing**: Google users data has French dates ("1 janv. 2024")
- **Apple Users Metadata**: First 2 rows contain start/end dates, script skips them
- **Null Handling**: Nulls in numeric columns are preserved for downstream analysis
- **Date Format**: All dates standardized to YYYY-MM-DD

## Extending the Pipeline

### Adding New Data Sources

1. Add file pattern to `column_mapping.py`
2. Create extraction method in `extract.py`
3. Create transformation method in `transform.py`
4. Add worksheet update in `load.py`

### Modifying Business Logic

Edit `transform.py`:
- `_add_agency_stage()` method for Stage classification
- Modify `AGENCY_START_DATE` in `settings.py`

## License

Internal project - AppASO Stage Environment
