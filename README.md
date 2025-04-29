
# TAPI Challenge - ETL + Dataviz

## Introducción

El objetivo de este proyecto es construir un pipeline de procesamiento de datos que permita calcular de manera precisa el revenue mensual de TAPI. Esto implica considerar tanto las comisiones recibidas de los proveedores como las comisiones a pagar a los clientes (revenue share), asegurando que cada mes quede correctamente cerrado y sin recalculaciones posteriores.

La solución fue diseñada utilizando exclusivamente servicios de AWS, combinando almacenamiento en S3, procesamiento serverless con AWS Glue, orquestación con Airflow (a través de MWAA) y visualización mediante Tableau conectado a Amazon Athena.

## Arquitectura de la solución

![image](https://github.com/user-attachments/assets/222dc4d5-f437-40fc-94ee-b539561461df)


El almacenamiento de datos en Amazon S3 se estructuró en tres zonas:

- **Raw:** almacenamiento de los archivos en su formato original, sin alteraciones.
- **Bronze:** normalización de datos, limpieza de registros no válidos y conversión a formato parquet.
- **Silver:** aplicación de reglas de negocio, cálculos financieros y modelado de esquema estrella para consultas analíticas.

Esta segmentación permite desacoplar las distintas fases del procesamiento y facilita tanto la trazabilidad como la eficiencia en el análisis.

En términos de procesamiento:

- **AWS Glue Jobs** fueron utilizados para las transformaciones, aprovechando su naturaleza serverless para reducir costos de infraestructura.
- **Apache Airflow**, desplegado en MWAA, gestiona la orquestación de tareas y dependencias entre procesos.
- **Amazon Athena** expone la capa silver de manera serverless para su consulta por parte de Tableau.

## Flujo de procesamiento

El pipeline diseñado sigue la siguiente lógica:

1. **Carga desde Raw a Bronze:**
   - Conversión de archivos de CSV a Parquet.
   - Eliminación de registros cuyo estado no sea "confirmado".
   - Definición explícita de tipos de datos.
   - Diferenciación en la estrategia de carga:
     - Para transacciones: **append** de nuevos registros, conservando histórico.
     - Para dimensiones (comisiones, revenue share): **overwrite** completo mensual, para simplificar actualizaciones.

2. **Transformación de Bronze a Silver:**
   - Cálculo de:
     - Comisiones a pagar a cada cliente.
     - Comisiones cobradas a cada proveedor.
     - Revenue neto de TAPI (monto transacción - comisión cliente + comisión proveedor).
   - Modelado de un esquema estrella:
     - **Fact table:** `fact_payments`.
     - **Dimensiones auxiliares:** `dim_clients`, `dim_providers`, entre otras.
   - Consolidación de los datos de manera que cada mes quede congelado, preservando la foto histórica del revenue.

3. **Orquestación con Airflow:**
   - Dos tareas iniciales en paralelo:
     - Carga de transacciones (raw → bronze).
     - Carga de dimensiones (raw → bronze).
   - Una vez finalizadas, ejecución del proceso de transformación (bronze → silver).
   - Monitoreo y gestión de fallos desde la consola de Airflow.

4. **Exposición de datos para visualización:**
   - Publicación de la capa silver en Amazon Athena.
   - Conexión de Athena a Tableau para la construcción de dashboards dinámicos y análisis mensual de revenue.

## Justificación de decisiones

- **Separación de capas:** permite modularidad, mayor control de calidad y optimización de tiempos de consulta.
- **Conversión a parquet en bronze:** mejora la performance de lectura en Athena y otros motores de consulta.
- **Cálculo de revenue en silver:** asegura que cada cierre mensual utilice las comisiones vigentes del momento, sin necesidad de recalcular ni mantener versiones históricas de las dimensiones.
- **Glue serverless:** reduce la necesidad de administración de infraestructura y se adapta al volumen de procesamiento mensual.
- **Airflow en MWAA:** facilita la programación, monitoreo y manejo de errores en los flujos de datos de forma centralizada.
- **Athena + Tableau:** combinación que permite consultar datos actualizados de forma económica y conectar visualizaciones sin necesidad de infraestructura adicional.

## Future Enhancements

- Incorporar una **tabla histórica de comisiones** por cliente y proveedor para auditorías futuras.
- Agregar un **módulo de validación automatizada** con alertas en caso de inconsistencias entre montos de transacciones y comisiones.
- Integrar con **CI/CD** para despliegue automático de DAGs y scripts de Glue.
- Incorporar **versionado de datos** con Delta Lake para facilitar el rollback de datasets ante errores.
- Optimizar consultas en Tableau mediante **materialización de vistas** en Athena.

## Conclusión

La arquitectura propuesta logra un pipeline de procesamiento eficiente, escalable y confiable, permitiendo a TAPI entender su revenue real mes a mes de manera rápida y segura. La estructura modular, combinada con servicios serverless y un esquema de datos pensado para el análisis, garantiza tanto la facilidad de mantenimiento como la capacidad de extender la solución en el futuro si fuese necesario.
