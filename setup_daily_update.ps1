# Windows Task Scheduler setup for daily Django dashboard update

# This script registers a scheduled task to run the dashboard update every day at 7:00 AM.
# It uses the current Python virtual environment and project path.

$venvPath = "$PSScriptRoot\.venv\Scripts\python.exe"
$managePy = "$PSScriptRoot\manage.py"
$action = "$venvPath $managePy update_dashboard_data"
$taskName = "DjangoDashboardDailyUpdate"

$trigger = New-ScheduledTaskTrigger -Daily -At 7:00am
$actionObj = New-ScheduledTaskAction -Execute $venvPath -Argument "$managePy update_dashboard_data"
Register-ScheduledTask -TaskName $taskName -Trigger $trigger -Action $actionObj -Description "Actualiza el dashboard financiero diariamente" -Force
Write-Host "Tarea programada creada: $taskName (7:00 AM diaria)"