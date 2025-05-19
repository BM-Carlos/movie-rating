# Movie-rating
# Proyecto de Ingeniería de Datos: Películas y Series

Este proyecto fue desarrollado para poner en práctica y reforzar mis habilidades en ingeniería de datos. Decidí construir una pequeña arquitectura de procesamiento utilizando datos públicos de las APIs de TMDB e IMDb. El objetivo es limpiar, enriquecer y exportar los datos en formatos listos para análisis o visualización.

Comencé extrayendo información desde la API de TMDB, incluyendo metadatos generales de películas y series, así como los proveedores de streaming disponibles en España. Además, los enriquecí con las valoraciones y número de votos de IMDb, obtenidos a través de la API de OMDB. Todo este proceso se llevó a cabo en Python, organizando el proyecto en tres carpetas: `extraction` para la recolección de datos, `transform` para los scripts que hacen limpieza y enriquecimiento de los datos y finalmente `load` donde guardo el resultado en diferentes formatos.

Durante el desarrollo, utilicé el módulo `logging` para registrar información útil durante la ejecución y facilitar la depuración.

## Stack Tecnológico

- Python 3.10
  - pandas
  - requests
  - pathlib
  - difflib
  - logging
  - time
  - datetime
- Fuentes de datos:
  - API pública de TMDB
  - API pública de OMDB

La arquitectura sigue una versión simplificada del modelo medallion, estructurada en tres capas:

- En `bronze`, guardo los datos obtenidos de las APIs con transformaciones mínimas.
- En `silver`, limpio y unifico los datos, combinando películas y series en una base común y aplicando transformaciones como formato de fechas, creación de columnas y detección de títulos populares.
- En `gold`, almaceno el dataset final, listo para ser usado. Lo exporto en distintos formatos (CSV, Parquet, JSON) para simular distintos casos de uso reales.

Los archivos de salida están listos para ser utilizados en herramientas de visualización, cargados en bases de datos, o usados como fuente de datos para análisis adicionales.

## Estructura de carpetas del proyecto
```
movie-rating/
├── data/
│ ├── 1_bronze/
│ ├── 2_silver/
│ └── 3_gold/
├── logs/
├── scripts/
│ ├── 1_extraction/
│ ├── 2_transformation/
│ └── 3_load/
└── README.md
```
## Conocimientos puestos en práctica
- Trabajar con respuestas JSON anidadas desde APIs públicas
- Limpiar y normalizar datos reales con valores nulos, formatos inconsistentes y listas embebidas
- Uso práctico de `datetime`, `pathlib` y manipulación de datos con `pandas`
- Aplicación de un modelo de arquitectura por capas (bronze → silver → gold)
- Exportación de datos en formatos variados como CSV, Parquet y JSON

---
Este proyecto me ha permitido consolidar mis conocimientos de base en ingeniería de datos, y tengo pensado seguir expandiéndolo en el futuro con nuevas fuentes, automatización o despliegue en la nube.
