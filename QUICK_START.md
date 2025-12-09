# Quick Start Guide - AppASO ETL Pipeline

## Production Deployment Checklist

### Step 1: Google Drive Setup
- [ ] Upload 6 Excel files to RAW folder (ID: 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL)
  - APPLE motcles.xlsx
  - GOOGLE motcles.xlsx
  - Installs Apple.xlsx
  - Installs Google.xlsx
  - Utilisateurs connectés Apple.xlsx
  - Utilisateurs connectés Google.xlsx

- [ ] Create folder structure in AppASO folder:
  ```
  AppASO/
  ├── 00_Control_Panel (Google Sheet)
  ├── MASTER_DATA_CLEAN (Google Sheet)
  └── 02_Data_Lake_Historic/
      ├── 2024/
      │   ├── 01_January/
      │   ├── 02_February/
      │   └── ... (up to 12_December)
      └── 2025/
          ├── 01_January/
          └── ... (up to 12_December)
  ```

- [ ] In 00_Control_Panel:
  - Create sheet named "Config"
  - Set cell B3 to "ON" or "TRUE"

- [ ] In MASTER_DATA_CLEAN:
  - Will auto-create 3 worksheets on first run:
    - KEYWORDS
    - INSTALLS
    - USERS

- [ ] Share with service account (found in datapipeline-stage-089124019a24.json):
  - AppASO folder (Editor)
  - RAW folder (Editor)

### Step 2: GitHub Repository Setup
```powershell
# Navigate to project directory
cd "c:\Users\Alejandro\Documents\Cosas_Universidad\Doble_Titulo\Dasci_Brest\Programas Python\AppASO"

# Initialize git if not already done
git init
git add .
git commit -m "Add refactored ETL pipeline for ASO data"

# Push to GitHub
git remote add origin https://github.com/alecacerestel/AppASO.git
git branch -M main
git push -u origin main
```

### Step 3: Configure GitHub Secrets

Go to: Repository > Settings > Secrets and variables > Actions

Add these 4 secrets:

1. **GCP_JSON**
   ```powershell
   # Get content from file
   Get-Content "Secret\datapipeline-stage-089124019a24.json" -Raw
   # Copy the output and paste in GitHub Secret
   ```

2. **EMAIL_USER**
   ```
   your.email@gmail.com
   ```

3. **EMAIL_PASSWORD**
   ```
   # Generate App Password:
   # 1. Go to https://myaccount.google.com/apppasswords
   # 2. Create password for "AppASO Pipeline"
   # 3. Copy the 16-character password
   abcd efgh ijkl mnop
   ```

4. **EMAIL_RECIPIENT**
   ```
   admin@example.com
   ```

### Step 4: Local Testing (Optional)

```powershell
# Install dependencies
pip install -r requirements.txt

# Set environment variables
$env:GCP_JSON = Get-Content "Secret\datapipeline-stage-089124019a24.json" -Raw
$env:EMAIL_USER = "your.email@gmail.com"
$env:EMAIL_PASSWORD = "your_app_password"
$env:EMAIL_RECIPIENT = "recipient@example.com"

# Run pipeline
python etl_pipeline.py
```

Expected output:
```
======================================================================
AppASO ETL Pipeline - ASO Data Processing
======================================================================
Execution started: 2025-12-09 10:30:00

[AUTHENTICATION] Authenticating with Google Cloud...
[AUTHENTICATION] Successfully authenticated

[CONTROL CHECK] Verifying control panel status...
[CONTROL CHECK] Pipeline is ENABLED (control panel B3 is ON)

[STEP 1/3] EXTRACTION
----------------------------------------------------------------------
[EXTRACTION] Found 6 files in RAW folder
[EXTRACTION] Keywords - Apple: 391 rows, Google: 395 rows
[EXTRACTION] Installs - Apple: 639 rows, Google: 639 rows
[EXTRACTION] Users - Apple: 667 rows, Google: 663 rows

[STEP 2/3] TRANSFORMATION
----------------------------------------------------------------------
[TRANSFORMATION] Keywords: 786 total rows
[TRANSFORMATION] Installs: 1278 total rows
[TRANSFORMATION] Users: 1330 total rows

[STEP 3/3] LOAD
----------------------------------------------------------------------
[LOAD] Writing to MASTER_DATA_CLEAN Google Sheet...
[LOAD] Successfully updated KEYWORDS: 786 rows
[LOAD] Successfully updated INSTALLS: 1278 rows
[LOAD] Successfully updated USERS: 1330 rows
[LOAD] Saving historical backup to Data Lake...
[LOAD] Historical backup saved for 2025-12-09
[LOAD] All data successfully loaded

======================================================================
Pipeline execution completed successfully
======================================================================
```

### Step 5: GitHub Actions Test

1. Go to GitHub repository
2. Click "Actions" tab
3. Select "Daily ETL Pipeline - ASO Data Processing"
4. Click "Run workflow" > "Run workflow"
5. Wait for completion (should take 2-3 minutes)
6. Check logs for any errors

### Step 6: Verify Results

Check Google Drive:
- [ ] MASTER_DATA_CLEAN has 3 worksheets with data
- [ ] Data Lake has 3 CSV files in correct month folder
- [ ] Dates are in YYYY-MM-DD format
- [ ] Platform column shows "Apple" and "Google"
- [ ] Stage column shows "Pre-Agencia" and "Con-Agencia"

## Monitoring

### Daily Execution
- Pipeline runs at 9:00 AM Paris time
- Check GitHub Actions history for status
- Email alerts sent if errors occur

### Control Panel
To pause pipeline:
1. Open 00_Control_Panel
2. Set cell B3 to "OFF" or "FALSE"

To resume:
1. Set cell B3 back to "ON" or "TRUE"

## Troubleshooting Quick Reference

| Error | Solution |
|-------|----------|
| Pipeline is DISABLED | Set cell B3 to ON in 00_Control_Panel |
| Root folder not found | Share AppASO folder with service account |
| RAW folder not found | Share folder 1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL |
| File not found | Check exact filename in RAW folder |
| Email fails | Use App Password, not regular Gmail password |
| Worksheet not found | MASTER_DATA_CLEAN will auto-create worksheets |
| Year/month folder not found | Create folder structure in Data Lake |

## Support

For detailed documentation, see:
- `README.md` - Complete project documentation
- `SECRETS_SETUP.md` - Detailed secret configuration guide
- `REFACTORING_SUMMARY.md` - Technical implementation details

## Architecture Diagram

```
GitHub Actions (Daily 9 AM)
        ↓
    Authentication
        ↓
    Control Check (B3)
        ↓
    ┌─────────────────────────────┐
    │  EXTRACTION (RAW Folder)    │
    │  - 6 Excel files            │
    │  - Auto-detect format       │
    └─────────────────────────────┘
        ↓
    ┌─────────────────────────────┐
    │  TRANSFORMATION             │
    │  - Column mapping           │
    │  - Date standardization     │
    │  - Platform unification     │
    │  - Stage classification     │
    └─────────────────────────────┘
        ↓
    ┌─────────────────────────────┐
    │  LOAD                       │
    │  - 3 Sheets in Master       │
    │  - 3 CSVs in Data Lake      │
    └─────────────────────────────┘
        ↓
    Success / Email on Error
```
