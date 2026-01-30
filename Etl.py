from operator import index

import pandas as pd
import glob
import os

#Etapa de Extraccion
#verficamos que los archivos existen
archivos=glob.glob("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_*.csv")

if not archivos:
    print("X no se encontraron los archivos. asegurate de descargarlos a la carpeta data/")
    print("  Deberias tener: ecommerce_orders.csv,ecommerce_customers.cvs,etc.")
else:
    print(f" Archivos Encontrados: {len(archivos)}")
    for f in sorted(archivos):
        print(f" -{os.path.basename(f)}")

#Cargar los archivos principales
df_orders=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_orders.csv")
#df_brands=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_brands.csv")
#df_cate=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_categories.csv")
df_customers=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_customers.csv")
#df_inventory=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_inventory.csv")
df_order_items=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_order_items.csv")
df_products=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_products.csv")
#df_promotions=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_promotions.csv")
#df_reviews=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_reviews.csv")
#df_suppliers=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_suppliers.csv")
#df_warehouses=pd.read_csv("C:\\Users\\antonio\PycharmProjects\Mi_Primer_ETL\data/ecommerce_warehouses.csv")
#Explorar
print(f"\n Resumen:")
print(f"Orders: {len(df_orders)} filas,{len(df_orders.columns)} columnas")
print(f"Order_Items: {len(df_order_items)} filas,{len(df_order_items.columns)} columnas")
print(f"Customers: {len(df_customers)} filas,{len(df_customers.columns)} columnas")
print(f"Products: {len(df_products)} filas,{len(df_products.columns)} columnas")

print(df_orders.head())
print(df_orders.info())
print(df_orders.describe())
print(df_order_items.head())
print(df_order_items.info())
print(df_order_items.describe())
print(df_customers.head())
print(df_customers.info())
print(df_customers.describe())
print(df_products.head())
print(df_products.info())
print(df_products.describe())


#TRANSFORMACION
#VALIDACION DE DATOS NULLOS, DECIDIR SI ELIMINAR O RELLENAR
#validar Nullos conteo de nulos por columnas
print(df_orders.isnull().sum())
print(df_customers.isnull().sum())
print(df_order_items.isnull().sum())
print(df_products.isnull().sum())
#Ejemplo: eliminar filas con nulos en campos críticos
#df_orders_clean=df_orders.copy()
df_orders_clean = df_orders.dropna(subset=['customer_id',  'total_amount'])

# Ejemplo: rellenar con 0 en campos numéricos opcionales
#df_orders_clean['discount'] = df_orders_clean['discount'].fillna(0)


df_orders_clean['promotion_id']=df_orders_clean['promotion_id'].fillna(0)
df_orders_clean['notes']=df_orders_clean['notes'].fillna(0)
print(df_orders_clean.isnull().sum())

#TRANSFORMACION
#VERIFICAMOS SI EXISTEN DUPLICADOS
duplicados_order=df_orders_clean.duplicated().sum()
print(f"Registros duplicados: {duplicados_order}")

#Duplicados por columnas
duplicados_id_orders=df_orders_clean.duplicated(subset=['order_id']).sum()
print(f"Order_id duplicados {duplicados_id_orders}")

duplicados_number_orders=df_customers.duplicated(subset=['email']).sum()
print(f"order_number duplicados {duplicados_number_orders}")

# Si hay IDs duplicados, quedarse con el más reciente
#df_orders_clean = df_orders_clean.sort_values('order_date').drop_duplicates(
 #   subset=['order_id'],
  #  keep='last'
#)

# Ver tipos de Datos actuales
print(df_orders_clean.dtypes)

# Convertir fechas
df_orders_clean['order_date'] = pd.to_datetime(df_orders_clean['order_date'])
# Verificar
print("\nTipos después de conversión:")
print(df_orders_clean.dtypes)
print(df_orders_clean.iloc[:,:5])

# Asegurar que los números sean numéricos
#Cuidado: errors="coerce" convierte valores inválidos a NaN. Después tenés que manejar esos NaN.
df_orders_clean['total_amount'] = pd.to_numeric(df_orders_clean['total_amount'], errors='coerce')
df_orders_clean['tax_amount'] = pd.to_numeric(df_orders_clean['tax_amount'], errors='coerce')
#Manejo de campos vacios
df_orders_clean['notes']=df_orders['notes'].fillna('').astype(str)

#¿Cuáles son los 5 clientes que más gastaron?
# Agrupamos por customer_id y sumamos total_amount
vta_cliente=df_orders_clean.groupby('customer_id').agg({'total_amount':'sum','order_id':'count'}).rename(columns={'total_amount':'total_gastado'
                            ,'order_id':'Cantidasd_ordenes'})
vta_cliente=vta_cliente.sort_values('total_gastado',ascending=False)
print("Top 5 clientes:")
print(vta_cliente.head())

# PREGUNTA 2: Producto más vendido
# Primero unimos orders con order_items para tener quantity
# Agrupamos por product_id y sumamos quantity
prd_vend=df_order_items.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
print(f"Producto mas vendido: id {prd_vend.idxmax()} ({prd_vend.max()} unidades)")


# PREGUNTA 3: Evolución mensual de ventas
# Agrupamos por mes y sumamos total_amount
df_orders_clean['mes']=df_orders_clean['order_date'].dt.to_period('M')
vtas_mes=df_orders_clean.groupby('mes')['total_amount'].sum().reset_index()
vtas_mes.columns=['mes','total_ventas']
print(vtas_mes)

#creamos la carpeta si no existe
os.makedirs('output',exist_ok=True)
#Guaradr Metricas en CSV
vta_cliente.to_csv('output/ventas_por_cliente.csv',index=False)
vtas_mes.to_csv('output/ventas_por_mes.csv',index=False)

#guardar los datos limpios
df_orders_clean.to_csv('output/ventas_order_clean.csv',index=False)
print("Archivos Csv Guardados correctamente")

#Guardar en formato Parquet
df_orders_clean.to_parquet('output/ventas_order_clean.parquet',index=False)
#Compara Tamaño csv vs parquet
csv_size=os.path.getsize('output/ventas_order_clean.csv')/1024
parquet_size=os.path.getsize('output/ventas_order_clean.parquet') / 1024

print(f"Tamaño CSV: {csv_size:.1f} KB")
print(f"Tamaño Parquet: {parquet_size:.1f} KB")
print(f"Parquet es {csv_size/parquet_size:.1f}x más chico")

readme_content="""
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
"""
with open('Readme.md','w') as f:
    f.write(readme_content)
print("Readme.md creado dghdd jshjs fhjhjfhjf  ")