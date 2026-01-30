
#Mi primer ETL con Python

##Descripcion
Pipeline ETL que procesa datos e-commerce para gnerar metricas de ventas.

##Cómo correr
'''bash
pip install pandas pyarrow
python etl.py
'''

##Desiciones de limpieza
-**Nulos**: Eliminé filas sin customer_id, product_id o total /(campos críticos)
-**Duplicados**: Eliminpe Duplicados por order_id, quedandime con el mas reciente
-**Tipos**: Convertí order_date a datetime, total y quantity a numerico

##Output
-´ventas_por_cliente.csv´: Total gastado y cantidad de ordenes por cliente
-´ventas_por_mes.csv´:Ventas totales por mes
-´orders_clean.parquet´: Dataset limpio en formato optimizado

## Autor
[Luis A. Morales]- [2026/01/30]
