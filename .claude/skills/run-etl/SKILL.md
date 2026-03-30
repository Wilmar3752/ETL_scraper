---
name: run-etl
description: Invoca la Lambda etl-scraper, espera que termine y muestra los logs de CloudWatch
allowed-tools: Bash
---

Ejecuta el ETL completo siguiendo estos pasos:

1. Invoca la Lambda `etl-scraper` en background con `aws lambda invoke` y guarda la respuesta en `/tmp/etl-response.json`
2. Mientras espera, obtén el nombre del log stream más reciente de `/aws/lambda/etl-scraper`
3. Espera hasta que aparezca el END RequestId en los logs (poll cada 15 segundos, máximo 10 intentos)
4. Muestra los logs completos del run
5. Muestra el resultado final (success/error por producto)

Si el resultado tiene errores, analiza los logs y explica qué falló y cómo corregirlo.
