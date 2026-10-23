$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

Write-Host "1/3 Compilando con Maven..." -ForegroundColor Cyan
mvn -q clean package
if ($LASTEXITCODE -ne 0) { throw "Falló la compilación" }

Write-Host "2/3 Generando app-image Windows (KafkaOffsetExplorer.exe) con jpackage..." -ForegroundColor Cyan
if (Test-Path dist) { Remove-Item dist -Recurse -Force }
jpackage --type app-image `
  --name KafkaOffsetExplorer `
  --input target `
  --main-jar kafka-offset-explorer-1.0.0.jar `
  --main-class com.offsetexplorer.MainApp `
  --dest dist `
  --java-options "-Xmx512m"
if ($LASTEXITCODE -ne 0) { throw "Falló jpackage" }

Write-Host "3/3 Listo: dist\KafkaOffsetExplorer\KafkaOffsetExplorer.exe" -ForegroundColor Green
