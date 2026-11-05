# Validador de Datos Financieros con PySpark y Pruebas Automatizadas

Este proyecto implementa un validador de calidad de datos robusto y modular utilizando **PySpark**, diseñado para asegurar la integridad de transacciones financieras mediante reglas de negocio claras y pruebas automatizadas con `pytest`.

---

## Preguntas Clave del Proyecto (FAQ Conceptual)

### 1. ¿Qué diferencia hay entre un DataFrame de Spark y una lista de Python?

- **DataFrame de Spark**: Está diseñado para **procesamiento distribuido** a gran escala (Big Data). Los datos se dividen en particiones a través de múltiples nodos de un clúster. Utiliza evaluación perezosa (_lazy evaluation_) y un optimizador de consultas (_Catalyst Optimizer_) para ejecutar operaciones de forma eficiente en paralelo.
- **Lista de Python**: Es una estructura de datos residente en la **memoria local** de una sola máquina. Su procesamiento es secuencial (monohilo) y se ejecuta inmediatamente, lo cual la hace ideal para conjuntos de datos pequeños, pero incapaz de manejar volúmenes masivos que superen la memoria RAM disponible.

### 2. ¿Por qué Spark no es necesario para siete registros?

- **Sobrecarga de inicialización (_Overhead_)**: Levantar una sesión de Spark (`SparkSession`) requiere recursos computacionales para coordinar la JVM (Java Virtual Machine), el driver y los executors.
- **Rendimiento**: Para un conjunto de datos minúsculo (como 7 registros), la sobrecarga de la infraestructura distribuida hace que el proceso sea más lento en comparación con estructuras nativas de Python (como listas o diccionarios) o pandas, donde no hay costos de coordinación de clúster.

### 3. ¿Qué cambia cuando se deben validar decenas de millones de transacciones?

- **Distribución y Paralelismo**: El procesamiento deja de ocurrir en una sola máquina y se reparte entre los nodos del clúster, permitiendo procesar gigabytes o terabytes de información.
- **Gestión de Memoria y Operaciones Costosas**: Operaciones como `groupBy` o `join` (utilizadas en nuestras reglas) provocan un **shuffle** (intercambio de datos entre nodos), lo que exige una correcta configuración de particiones y recursos para evitar cuellos de botella o errores de memoria (`Out of Memory`).

### 4. ¿Qué es una validación de datos y qué es una prueba automatizada del validador?

- **Validación de datos**: Es el proceso de inspeccionar los datos operativos para garantizar que cumplan con las reglas de negocio y calidad establecidas (por ejemplo, que un ID no sea nulo o que un monto esté dentro de un rango válido).
- **Prueba automatizada del validador**: Es un bloque de código (escrito con herramientas como `pytest`) que verifica que el _código de nuestras reglas de validación_ funcione correctamente ante diferentes escenarios simulados (casos válidos, casos erróneos, nulos, duplicados), garantizando la fiabilidad del software.

### 5. ¿Qué problemas puede causar una transacción duplicada en un pipeline financiero?

- **Inexactitud contable**: Puede generar cobros dobles a clientes, duplicación de ingresos o desajustes graves en los reportes financieros.
- **Distorsión de métricas**: Altera los indicadores clave de rendimiento (KPIs) y los balances de auditoría, dificultando la conciliación bancaria y dañando la confianza operativa.

### 6. ¿Qué controles aplicarías para comparar la cantidad de registros de entrada y salida de un ETL?

- **Conteo de filas (_Row Count Reconciliation_)**: Validar formalmente que $Count(Entrada) = Count(Salida) + Count(Registros Rechazados/Errores)$.
- **Totales de Control (_Control Totals / Checksums_)**: Sumarizar montos monetarios totales antes y después del proceso para verificar que ninguna transacción monetaria se haya perdido o duplicado en el camino.
- **Bitácoras y Trazabilidad (_Lineage_)**: Mantener registros de auditoría detallados que documenten cuántas filas pasaron por cada regla de filtrado o transformación.
