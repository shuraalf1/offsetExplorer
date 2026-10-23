# Documento de Requerimientos — Kafka Offset Explorer & Event Tracer

## 1. Propósito del Sistema
**Kafka Offset Explorer (Clean-Room Edition)** es una aplicación de escritorio escrita en Java 17 con interfaz gráfica moderna (Swing + FlatLaf) diseñada para administrar, inspeccionar y rastrear eventos en clústeres de Apache Kafka en múltiples ambientes (Local, Desarrollo, QA, Producción).

---

## 2. Requerimientos Funcionales (RF)

### RF-01: Gestión Multi-Ambiente (Perfiles de Conexión)
- **RF-01.1**: El sistema debe permitir crear, editar, duplicar, eliminar y exportar/importar perfiles de conexión correspondientes a distintos ambientes (`DEV`, `QA`, `STAGING`, `PROD`, `LOCAL`).
- **RF-01.2**: Debe existir un selector rápido de ambiente en la barra superior que permita cambiar de clúster activo en cualquier momento sin reiniciar la aplicación.
- **RF-01.3**: Los perfiles deben persistirse localmente en formato JSON (`~/.kafka-offset-explorer/profiles.json`).

### RF-02: Seguridad y Autenticación SASL/SSL
- **RF-02.1**: Soporte para protocolos de seguridad: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` y `SASL_SSL`.
- **RF-02.2**: Soporte para mecanismos SASL: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` y configuración JAAS personalizada.
- **RF-02.3**: Soporte para configuración TLS/SSL con ruta y contraseña de `Truststore` (JKS/PKCS12), `Keystore` opcional (mTLS) y opción para habilitar o deshabilitar la verificación de hostname (`ssl.endpoint.identification.algorithm`).

### RF-03: Explorador de Clúster, Tópicos y Particiones
- **RF-03.1**: Visualización de metadatos del clúster (Cluster ID, Controller Node, lista de Brokers con host/puerto/rack).
- **RF-03.2**: Listado de tópicos con filtrado en tiempo real, mostrando número de particiones, factor de replicación, offsets mínimos/máximos y total estimado de mensajes.
- **RF-03.3**: Creación y eliminación de tópicos desde la interfaz.

### RF-04: Visualización de Mensajes (JSON / String / Hex) y Publicación
- **RF-04.1**: Consumo bajo demanda de mensajes de un tópico (todas las particiones o una específica) desde:
  - `Earliest` (inicio)
  - `Latest - N` (últimos N mensajes)
  - `Offset específico`
  - `Timestamp específico`
- **RF-04.2**: Visualizador de carga útil (*Payload Viewer*) con pestañas para:
  - **JSON Pretty-Print**: Formateo automático con sangría, validación sintáctica y resaltado de estructura.
  - **String (UTF-8)**: Texto plano sin procesar.
  - **Hexadecimal**: Volcado binario en formato hex/ASCII para inspección de bytes.
  - **Headers**: Tabla de cabeceras Kafka (`key` / `value` en UTF-8 y bytes).
- **RF-04.3**: Herramienta integrada para publicar mensajes de prueba (Key, Value, Headers, Partición) para validar flujos.

### RF-05: Consumer Groups y Reseteo de Offsets
- **RF-05.1**: Listado de todos los *Consumer Groups* del clúster junto con su estado (`Stable`, `Empty`, `Dead`, `PreparingRebalance`).
- **RF-05.2**: Cálculo en tiempo real de *Lag* por tópico y partición (`Log End Offset - Committed Offset = Lag`).
- **RF-05.3**: Función de **Reseteo de Offsets** (`alterConsumerGroupOffsets`) que permita posicionar el offset de un grupo de consumidores en:
  - `Earliest` (al inicio de la partición)
  - `Latest` (al final actual de la partición)
  - `Specific Offset` (un número de offset exacto)
  - `Timestamp` (el primer offset cuyo timestamp sea mayor o igual a la fecha/hora indicada)

### RF-06: Motor de Trazabilidad de Eventos por Key (Event Traceability)
- **RF-06.1**: El usuario debe poder seleccionar una lista de tópicos que conforman un flujo o pipeline de negocio (con posibilidad de seleccionar varios tópicos del clúster o ingresar una lista separada por comas).
- **RF-06.2**: Búsqueda distribuida en los tópicos seleccionados filtrando por la **Key del evento** (modos: *Coincidencia Exacta*, *Contiene*, *Expresión Regular*).
- **RF-06.3**: Construcción automática de una **Línea de Tiempo Cronológica (Trace Timeline)** ordenada por timestamp del evento, mostrando:
  - Orden del salto (`Hop #1`, `Hop #2`, ...)
  - Tópico donde se encontró el evento
  - Partición y Offset
  - Timestamp exacto
  - Latencia acumulada (`Δ Total`) y latencia entre saltos consecutivos (`Δ Salto`)
  - Resumen de Headers y Payload
- **RF-06.4**: Vista detallada del evento en cada salto del flujo con formateo JSON para inspeccionar cómo mutó el mensaje a través de los tópicos.

---

## 3. Requerimientos No Funcionales (RNF)

- **RNF-01 (Portabilidad y Empaquetado)**: Desarrollado sobre **Java 17 LTS** y **Apache Maven**, generando tanto un `Fat JAR` autónomo como un ejecutable nativo para Windows (`KafkaOffsetExplorer.exe`) mediante `jpackage`.
- **RNF-02 (Concurrencia y Responsividad)**: Todas las operaciones de red con Kafka (`AdminClient`, `KafkaConsumer`, `KafkaProducer`) deben ejecutarse en hilos de fondo (`SwingWorker` / `CompletableFuture`) para no bloquear el hilo de interfaz gráfica (EDT).
- **RNF-03 (Seguridad de Lectura)**: El consumo de mensajes y el rastreo de trazabilidad deben realizarse sin confirmar offsets (`enable.auto.commit = false`) y utilizando asignaciones directas de particiones (`assign`) para no alterar ningún *Consumer Group* productivo.
- **RNF-04 (Usabilidad)**: Interfaz moderna con soporte de tema oscuro/claro mediante **FlatLaf**, tablas ordenables y exportación de trazas.
