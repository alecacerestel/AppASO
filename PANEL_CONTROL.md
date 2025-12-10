# Panel de Control - Configuración de Flags

## Celdas de Control en Google Sheets

En el archivo `00_Control_Panel` (worksheet "Config"):

| Celda | Nombre | Descripción | Tipo | Default |
|-------|--------|-------------|------|---------|
| B3 | Execute Pipeline | Habilita/deshabilita ejecución completa | Checkbox | TRUE |
| B4 | Backup Pipeline | Guarda CSVs históricos en data/processed/ | Checkbox | TRUE |
| B5 | Force ML Retrain | Fuerza re-entrenamiento de modelos ML (futuro) | Checkbox | FALSE |
| B6 | Send Email Alerts | Envía notificaciones por email | Checkbox | TRUE |

## Flujo de Integración

### 1. App Script (Google Sheets)
```javascript
// Lee las celdas B3-B6
const executePipeline = sheet.getRange(3, 2).getValue();
const backupEnabled = sheet.getRange(4, 2).getValue();
const forceML = sheet.getRange(5, 2).getValue();
const sendEmails = sheet.getRange(6, 2).getValue();

// Envía a GitHub Actions
const payload = {
  "ref": "main",
  "inputs": {
    "run_backup": backupEnabled.toString(),
    "run_ml": forceML.toString(),
    "send_alerts": sendEmails.toString()
  }
};
```

### 2. GitHub Actions (.github/workflows/daily_etl.yml)
Recibe los inputs y los pasa como variables de entorno:
```yaml
env:
  RUN_BACKUP: ${{ github.event.inputs.run_backup || 'true' }}
  RUN_ML: ${{ github.event.inputs.run_ml || 'false' }}
  SEND_ALERTS: ${{ github.event.inputs.send_alerts || 'true' }}
```

### 3. Python (src/config/settings.py)
Lee las variables de entorno:
```python
RUN_BACKUP: bool = os.getenv("RUN_BACKUP", "true").lower() == "true"
RUN_ML: bool = os.getenv("RUN_ML", "false").lower() == "true"
SEND_ALERTS: bool = os.getenv("SEND_ALERTS", "true").lower() == "true"
```

## Comportamiento por Flag

### B3 - Execute Pipeline (CRÍTICO)
- **TRUE**: Pipeline se ejecuta normalmente
- **FALSE**: Pipeline se detiene gracefully, sin procesar datos
- **Control**: Verificado en `etl_pipeline.py` antes de iniciar ETL

### B4 - Backup Pipeline
- **TRUE**: Guarda CSVs en `data/processed/` con formato `{type}_YYYYMMDD.csv`
- **FALSE**: Skip backup, solo actualiza Google Sheets
- **Control**: `src/etl/load.py` → `_load_to_data_lake()`

### B5 - Force ML Retrain
- **TRUE**: Activa re-entrenamiento de modelos ML (feature no implementada aún)
- **FALSE**: Comportamiento normal
- **Control**: `etl_pipeline.py` → logging informativo por ahora
- **Futuro**: Disparar pipeline de ML después de ETL

### B6 - Send Email Alerts
- **TRUE**: Envía emails de éxito y error
- **FALSE**: Suprime todas las notificaciones por email
- **Control**: `src/utils/notifications.py` → ambos métodos
- **Uso**: Útil para testing sin spam de emails

## Testing Local

Usa `test_panel_control.ps1` para simular diferentes configuraciones:

```powershell
# Edita las variables en el script
$env:RUN_BACKUP = "true"      # B4
$env:RUN_ML = "false"         # B5
$env:SEND_ALERTS = "false"    # B6 (false para testing sin emails)

# Ejecuta
.\test_panel_control.ps1
```

## Logs Esperados

### B4 = TRUE (Backup enabled)
```
[LOAD] Saving historical backup to local data lake...
[LOAD] Saved keywords_20251210.csv (786 rows)
[LOAD] Saved installs_20251210.csv (1278 rows)
[LOAD] Saved users_20251210.csv (1243 rows)
```

### B4 = FALSE (Backup disabled)
```
[LOAD] Backup disabled (Panel B4 = FALSE). Skipping local data lake save.
```

### B5 = TRUE (ML requested)
```
[ML] Force ML retrain requested (Panel B5 = TRUE)
[ML] Feature not implemented yet - will be added in future version
```

### B6 = FALSE (Emails disabled)
```
[EMAIL] Email alerts disabled (Panel B6 = FALSE). Skipping success notification.
```

## Próximos Pasos

1. ✅ Configurar GitHub Token en App Script (NUEVO, el anterior fue revocado)
2. 🔄 Implementar Data Lake en Drive (Opción A - mantener Warehouse actual)
3. 🔜 Implementar ML pipeline cuando se active B5
