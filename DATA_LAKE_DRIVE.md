# Data Lake Drive - Historical Backup

## Overview

Historical backup system that uploads previous day's processed data to a Google Sheet with daily worksheets.

**Key Points:**
- ✅ Single Google Sheet: `Data_Lake_Historic`
- ✅ Worksheet per day: `YYYYMMDD_datatype` (e.g., `20251210_keywords`)
- ✅ D-1 Strategy: Uploads yesterday's data, deletes local CSV
- ✅ No Service Account quota issues

## Daily Flow

📅 **Day 10:** Processes data → Updates MASTER_DATA_CLEAN → Creates backup worksheet `20251210_*`  
📅 **Day 11:** Processes data → Updates MASTER_DATA_CLEAN → Creates backup worksheet `20251211_*`  
📅 **Day 12:** Processes data → Updates MASTER_DATA_CLEAN → Creates backup worksheet `20251212_*`

**Result:**
- MASTER_DATA_CLEAN: Always has current/latest data (Looker Studio uses this)
- Data_Lake_Historic: Complete historical archive with one backup per day

## Configuration

### Google Sheet

- **Name:** `Data_Lake_Historic`
- **ID:** `1tzaLbkXtBxnuKBkVftwN5bW6nEfIYEB12kxXjGaGFko`
- **Location:** `02_Data_Lake_Historic/` folder
- **Permissions:** Service Account must have Editor access

### Settings

```python
# src/config/settings.py
DATA_LAKE_HISTORIC_SHEET_ID = "1tzaLbkXtBxnuKBkVftwN5bW6nEfIYEB12kxXjGaGFko"
```

### Control Flags

**Panel de Control B4:**
- `TRUE`: Enables local backup AND Drive upload
- `FALSE`: Disables all backup

**Environment Override (testing):**
```powershell
$env:RUN_BACKUP_DRIVE = "true"
```

## Structure

### Worksheet Naming

```
20251210_keywords   → December 10, 2025 - Keywords data
20251210_installs   → December 10, 2025 - Installs data
20251210_users      → December 10, 2025 - Users data
20251211_keywords   → December 11, 2025 - Keywords data
...
```

### Sheet Organization

```
Data_Lake_Historic
├── 20251210_keywords (786 rows, 15 cols)
├── 20251210_installs (1278 rows, 12 cols)
├── 20251210_users (1243 rows, 10 cols)
├── 20251211_keywords
├── 20251211_installs
├── 20251211_users
└── ... (grows daily)
```

## Testing

### Normal Run (reads Panel B4)

```powershell
python etl_pipeline.py
```

### Force Backup ON

```powershell
$env:RUN_BACKUP_DRIVE = "true"
python etl_pipeline.py
```

### Simulate Future Date

```powershell
# Simulate Day 13 → uploads Day 12 files
$env:EXECUTION_DATE = "2025-12-13"
python etl_pipeline.py
```

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| "No files found for YYYY-MM-DD" | First day OR files already uploaded | Normal - wait for next day |
| Permission denied | Service Account lacks access | Share Sheet with SA email as Editor |
| Worksheet already exists | Re-running same day | Handled automatically (deletes old) |

## Implementation Details

### Main Function: `_load_to_data_lake_drive()`

1. Check if `RUN_BACKUP_DRIVE` enabled
2. Use current execution date
3. Take processed DataFrames (already in memory)
4. Open `Data_Lake_Historic` Sheet
5. For each data type (keywords, installs, users):
   - Use DataFrame from `data` dictionary
   - Create worksheet `YYYYMMDD_{datatype}`
   - Upload data directly

### Helper: `_add_worksheet_from_csv()`

1. Sanitize DataFrame (NaN → empty, Inf → None)
2. Delete existing worksheet if present
3. Create new worksheet (sized to data)
4. Upload headers + rows

## Why This Approach?

**Previous Attempts:**
- ❌ Partitioned folders: Service Account quota errors
- ❌ CSV files in Drive: Cannot create new files
- ❌ D-1 local files: Doesn't work across environments (local vs GitHub)

**Current Solution:**
- ✅ Single Sheet: No file creation needed
- ✅ Worksheets only: Service Account can add worksheets
- ✅ From Data Warehouse: Uses MASTER_DATA_CLEAN as source (always available)
- ✅ Environment agnostic: Works in local, GitHub Actions, anywhere
- ✅ Simple structure: One Sheet vs complex folder tree
