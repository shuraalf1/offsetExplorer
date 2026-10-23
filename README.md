# Kafka Offset Explorer & Event Tracer — Guía de uso

Aplicación de escritorio Java 17 (Swing + FlatLaf) para explorar Kafka: multi-ambiente, SASL/SSL, visor JSON/String/Hex, reseteo de offsets y trazabilidad de eventos por Key.
Ver [requirements.md](requirements.md) y [ARCHITECTURE.md](ARCHITECTURE.md).

## Requisitos
- Windows 10/11 x64, **JDK 17+** (incluye `jpackage`), **Maven 3.9+** (ambos en el `PATH`).
- Acceso de red a un clúster Kafka (o uno local, ver abajo).

## Paso a paso
1. Abrir PowerShell en la carpeta del proyecto:
   `cd C:\Users\prueb\Downloads\offsetexplorer`
2. Compilar: `mvn clean package` → genera `target\kafka-offset-explorer-1.0.0.jar`.
3. Ejecutar (opción A, JAR): `.\run-offset-explorer.bat`
4. Generar el ejecutable (opción B, .exe): `powershell -ExecutionPolicy Bypass -File .\build-windows-exe.ps1`
   → `dist\KafkaOffsetExplorer\KafkaOffsetExplorer.exe` (incluye su propio runtime; carpeta portable).

## Uso
1. **Ambientes**: botón *Gestionar ambientes...* → crear/duplicar/eliminar. Se guardan en `%USERPROFILE%\.kafka-offset-explorer\profiles.json`. El combo superior cambia de ambiente al instante.
2. **SASL/SSL**: elegir protocolo (`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`), mecanismo (`PLAIN`, `SCRAM-SHA-256/512`, `CUSTOM` con JAAS propio), usuario/password, truststore/keystore.
3. **Clúster y Tópicos**: *Conectar / Refrescar* para ver brokers, tópicos y offsets por partición.
4. **Mensajes**: *Cargar tópicos*, elegir modo (últimos N, earliest, offset, timestamp), filtro opcional; seleccione una fila para ver JSON / String / Hex / Headers. *Publicar...* envía un mensaje de prueba.
5. **Consumer Groups / Offsets**: *Listar grupos* → *Ver lag* → *Resetear offsets...* (Earliest, Latest, offset exacto, timestamp). El grupo debe estar **inactivo**.
6. **Trazabilidad**: indique los tópicos del flujo (precargados desde el ambiente), la Key y el modo de coincidencia; *Rastrear* muestra la línea de tiempo con Δ por salto. *Generar flujo demo* publica un evento con la misma Key en cada tópico para probar.

## Kafka local para probar (opcional, Docker)
```powershell
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0
```
Usar el ambiente `Kafka Localhost` (`localhost:9092`, PLAINTEXT).

## Notas
- Lecturas y trazas usan `assign()` sin commit: no alteran ningún consumer group.
- Las contraseñas se guardan en texto plano en `profiles.json`; proteja ese archivo.
- Los perfiles de ejemplo DEV/QA/PROD tienen hosts ficticios; edítelos.
