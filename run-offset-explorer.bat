@echo off
rem Lanza la aplicacion desde el fat JAR (requiere Java 17+ en el PATH)
cd /d "%~dp0"
if not exist target\kafka-offset-explorer-1.0.0.jar (
  echo Primero compile: mvn clean package
  exit /b 1
)
java -jar target\kafka-offset-explorer-1.0.0.jar
