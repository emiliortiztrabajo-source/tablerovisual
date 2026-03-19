# Dashboard Financiero Argentina

Proyecto Django listo para ejecutar en VS Code con SQLite, Bootstrap y datos mock para un dashboard financiero argentino.

## Requisitos

- Python 3.11+
- pip

## Instalacion

```bash
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
python manage.py migrate
python manage.py update_dashboard_data
python manage.py runserver
```

## URL principal

```bash
http://127.0.0.1:8000/
```

## Estructura

```text
project/
dashboard/
templates/
static/
services/
```

## Notas

- El proyecto usa `Django 5.2 LTS` para mantener compatibilidad con `Python 3.11`.
- Si luego subes a Python 3.12+, puedes evaluar migrar a Django 6.x.
- Los servicios están desacoplados en `services/` y hoy usan mocks con `NotImplementedError` como punto de entrada para APIs reales.
- El comando `python manage.py update_dashboard_data` vuelve a cargar las series y cotizaciones.
