# AppASO - ETL Pipeline (Stage Environment)

Production-ready serverless ETL pipeline that runs on GitHub Actions and integrates with Google Drive and Google Sheets.

## Project Structure

```
AppASO/
├── .github/
│   └── workflows/
│       └── daily_etl.yml          # GitHub Actions workflow
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py            # Configuration and environment variables
│   ├── services/
│   │   ├── __init__.py
│   │   ├── auth.py                # Google Cloud authentication
│   │   └── drive.py               # Google Drive/Sheets operations
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── extract.py             # Data extraction (mock data generation)
│   │   ├── transform.py           # Data transformation and cleaning
│   │   ├── load.py                # Data loading to Drive/Sheets
│   │   └── pipeline.py            # ETL orchestration
│   └── utils/
│       ├── __init__.py
│       ├── notifications.py       # Email notification service
│       └── error_handler.py       # Error handling and logging
├── etl_pipeline.py                # Main execution script
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## Google Drive Structure

The pipeline expects the following folder structure in Google Drive:

```
AppASO/                                    # Root folder
├── 00_Control_Panel                      # Google Sheet with Config sheet (B1: ON/OFF)
├── 01_Data_Warehouse/
│   ├── MASTER_DATA_CLEAN                 # Google Sheet (updated daily)
│   └── PREDICTIONS_ML                    # (Not used yet)
├── 02_Data_Lake_Historic/
│   ├── 2024/
│   │   ├── 01_January/
│   │   ├── 02_February/
│   │   └── ...
│   └── 2025/
│       ├── 01_January/
│       └── ...
└── 03_Logs_And_Errors/                   # Error logs stored here
```

## Setup Instructions

### 1. Google Cloud Setup

1. Create a Service Account in Google Cloud Console
2. Enable Google Drive API and Google Sheets API
3. Download the JSON credentials file
4. Share the AppASO folder in Google Drive with the service account email (found in the JSON file)
   - Format: `xxx@xxx.iam.gserviceaccount.com`
   - Give it "Editor" permissions

### 2. Gmail App Password Setup

1. Go to your Google Account settings
2. Navigate to Security > 2-Step Verification
3. Scroll down to "App passwords"
4. Generate a new app password for "Mail"
5. Save this password for GitHub Secrets

### 3. GitHub Secrets Configuration

Add the following secrets to your GitHub repository (Settings > Secrets and variables > Actions):

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `GCP_JSON` | Complete content of the Google Cloud service account JSON file | `{"type": "service_account", "project_id": "...", ...}` |
| `EMAIL_USER` | Gmail address for sending notifications | `your.email@gmail.com` |
| `EMAIL_PASSWORD` | Gmail App Password (NOT your regular password) | `abcd efgh ijkl mnop` |
| `EMAIL_RECIPIENT` | Email address to receive error notifications | `recipient@example.com` |

### 4. Local Testing (Optional)

Create a `.env` file in the project root (DO NOT commit this):

```bash
GCP_JSON={"type": "service_account", ...}
EMAIL_USER=your.email@gmail.com
EMAIL_PASSWORD=your_app_password
EMAIL_RECIPIENT=recipient@example.com
```

Install dependencies and run:

```bash
pip install -r requirements.txt
python etl_pipeline.py
```

## How It Works

### Pipeline Flow

1. **Authentication**: Authenticates with Google Cloud using service account credentials
2. **Control Check**: Reads `00_Control_Panel` sheet. If cell B1 is not "ON", execution stops
3. **Extract**: Generates mock data (replace this with real data sources later)
4. **Transform**: Cleans data, handles nulls, formats dates
5. **Load**:
   - Saves CSV to appropriate folder in `02_Data_Lake_Historic/YYYY/MM_Month/`
   - Updates `MASTER_DATA_CLEAN` sheet in `01_Data_Warehouse`
6. **Error Handling**: On failure, sends email and uploads log to `03_Logs_And_Errors`

### Scheduling

The pipeline runs automatically every day at 8:00 AM UTC via GitHub Actions.

You can also trigger it manually:
- Go to Actions tab in GitHub
- Select "Daily ETL Pipeline"
- Click "Run workflow"

## Features

- **Modular Architecture**: Separated concerns (config, services, ETL, utils)
- **Robust Error Handling**: Comprehensive try-catch with logging and notifications
- **Smart Folder Navigation**: Finds folders by name, not hardcoded IDs
- **Control Panel**: Easy ON/OFF switch without touching code
- **Scalable**: Easy to replace mock data with real APIs/databases
- **Production-Ready**: Proper logging, error handling, and notifications

## Next Steps

### Replace Mock Data

Edit `src/etl/extract.py` to connect to real data sources:

```python
def extract_data(self):
    # Replace this with actual API calls, database queries, etc.
    # Example: return pd.DataFrame(api_client.get_data())
    pass
```

### Add Machine Learning

Once ML models are ready, integrate them in a new module:
- Create `src/ml/predictor.py`
- Update `ETLPipeline` to include prediction step
- Load predictions to `PREDICTIONS_ML` sheet

## Troubleshooting

### Common Issues

1. **"Root folder not found"**
   - Ensure the AppASO folder is shared with the service account email
   - Check that the folder name is exactly "AppASO"

2. **"Control panel not found"**
   - Verify the Google Sheet is named exactly "00_Control_Panel"
   - Ensure it has a sheet named "Config" with cell B1

3. **"Email sending failed"**
   - Verify you're using an App Password, not your regular Gmail password
   - Check that 2-Step Verification is enabled on your Google account

4. **"Month folder not found"**
   - Ensure folders are named exactly as: `01_January`, `02_February`, etc.
   - Create folders for current year and month if they don't exist

## License

Internal project - AppASO Stage Environment
