#!/usr/bin/env python
# coding: utf-8

# 1. IMPORTANDO LIBRERÍAS

# In[101]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# In[102]:


spark = SparkSession.builder.getOrCreate()


# 2. CAPA LANDING

# In[103]:


# 4.1 Estructura del dataframe.
df_schema_bancos = StructType([
StructField("COD_BANCO", StringType(),False),
StructField("BANCO_CAJA", StringType(),True),
StructField("ESTADO", StringType(),True),
StructField("TIPO", StringType(),True),
])


# In[104]:


ruta_bancos_raw = "gs://esp-bigdata-grupo3/Datalake/Workload/Grupo3/Bancos.csv"
ruta_bancos_landing = "gs://esp-bigdata-grupo3/Datalake/Landing/Grupo3/Bancos/"


# In[105]:


df_bancos = spark.read.format("CSV").option("header","true").option("delimiter",";").schema(df_schema_bancos).load(ruta_bancos_raw)
df_bancos.show(5)


# In[106]:


df_bancos.write.mode("overwrite").format("parquet").save(ruta_bancos_landing)


# In[107]:


# 4.1 Estructura del dataframe.
df_schema_comercios = StructType([
StructField("COD_COMERCIO", StringType(),False),
StructField("NOMBRE_COMERCIO", StringType(),True),
StructField("ESTADO", StringType(),True),
])


# In[108]:


ruta_comercios_raw = "gs://esp-bigdata-grupo3/Datalake/Workload/Grupo3/Comercios.csv"
ruta_comercios_landing = "gs://esp-bigdata-grupo3/Datalake/Landing/Grupo3/Comercios/"


# In[109]:


df_comercios = spark.read.format("CSV").option("header","true").option("delimiter",";").schema(df_schema_comercios).load(ruta_comercios_raw)
df_comercios.show(5)


# In[110]:


df_comercios.write.mode("overwrite").format("parquet").save(ruta_comercios_landing)


# In[111]:


# 4.1 Estructura del dataframe.
df_schema_contracargos = StructType([
StructField("TIPO_CONTROVERSIA", StringType(),False),
StructField("ESTADO_CONTRACARGO", StringType(),True),
StructField("DETALLE", StringType(),True),
StructField("FECHA_TRANSACCION", StringType(),True),
StructField("FECHA_PROCESO", StringType(),True),
StructField("CODIGO_COMERCIO", StringType(),True),
StructField("NUMERO_REFERENCIA", StringType(),True),
StructField("IMPORTE_TRANSACCION", DoubleType(),True),
StructField("IMPORTE_DISPUTABLE", DoubleType(),True),
StructField("COD_BANCO", StringType(),True),
StructField("FECHA_EMISION", StringType(),True),
StructField("FECHA_ACCION", StringType(),True),
StructField("GESTOR", StringType(),True),
StructField("ACCION", StringType(),True),
StructField("ID_TRANSACCION", StringType(),True),
StructField("NUMERO_VOUCHER", StringType(),True),
StructField("NUMERO_AUTORIZACION", StringType(),True),
StructField("CORREO", StringType(),True),
StructField("FECHA_VENCIMIENTO", StringType(),True),
StructField("CANAL", StringType(),True),
])


# In[112]:


ruta_contracargos_raw = "gs://esp-bigdata-grupo3/Datalake/Workload/Grupo3/Contracargos.csv"
ruta_contracargos_landing = "gs://esp-bigdata-grupo3/Datalake/Landing/Grupo3/Contracargos/"


# In[114]:


df_contracargos = spark.read.format("CSV").option("header","true").option("delimiter",";").option("charset", "iso-8859-1").schema(df_schema_contracargos).load(ruta_contracargos_raw)
df_contracargos.show(5)


# In[115]:


df_contracargos[['TIPO_CONTROVERSIA']].show(5)


# In[116]:


df_contracargos.write.mode("overwrite").format("parquet").save(ruta_contracargos_landing)


# 3. CAPA CURATED

# In[117]:


df_bancos_landing = spark.read.format("parquet").option("header","true").load(ruta_bancos_landing)


# In[118]:


df_bancos_procesado = df_bancos_landing.withColumn('ESTADO', regexp_replace('ESTADO', ' ', ''))
df_bancos_procesado = df_bancos_procesado.withColumn('BANCO_CAJA',upper(col('BANCO_CAJA')))


# In[119]:


ruta_bancos_curated = "gs://esp-bigdata-grupo3/Datalake/Curated/Grupo3/Bancos/"
df_bancos_procesado.write.mode("overwrite").format("parquet").save(ruta_bancos_curated)


# In[120]:


df_comercios_landing = spark.read.format("parquet").option("header","true").load(ruta_comercios_landing)


# In[121]:


df_comercios_procesado = df_comercios_landing.withColumn('ESTADO', regexp_replace('ESTADO', ' ', ''))
df_comercios_procesado = df_comercios_procesado.withColumn('NOMBRE_COMERCIO',upper(col('NOMBRE_COMERCIO')))


# In[122]:


ruta_comercios_curated = "gs://esp-bigdata-grupo3/Datalake/Curated/Grupo3/Comercios/"
df_comercios_procesado.write.mode("overwrite").format("parquet").save(ruta_comercios_curated)


# In[123]:


df_contracargos_landing = spark.read.format("parquet").option("header","true").load(ruta_contracargos_landing)


# In[124]:


df_contracargos_landing[['GESTOR']].tail(10)


# In[125]:


df_contracargos_procesado = df_contracargos_landing.withColumn('TIPO_CONTROVERSIA', regexp_replace('TIPO_CONTROVERSIA', ' ', ''))
df_contracargos_procesado = df_contracargos_procesado.fillna('GESTOR DESCONOCIDO')


# In[126]:


df_contracargos_procesado[['GESTOR']].tail(10)


# In[127]:


ruta_contracargos_curated = "gs://esp-bigdata-grupo3/Datalake/Curated/Grupo3/Contracargos/"
df_contracargos_procesado.write.mode("overwrite").format("parquet").save(ruta_contracargos_curated)


# 4. CAPA FUNCTIONAL

# In[128]:


ruta_functional = "gs://esp-bigdata-grupo3/Datalake/Functional/Grupo3/ContracargosEnriquecido/"


# In[129]:


df_bancos_curated = spark.read.format("parquet").option("header","true").load(ruta_bancos_curated)
df_comercios_curated = spark.read.format("parquet").option("header","true").load(ruta_comercios_curated)
df_contracargos_curated = spark.read.format("parquet").option("header","true").load(ruta_contracargos_curated)


# In[130]:


df_contracargos_curated.columns


# In[131]:


df_comercios_curated.columns


# In[132]:


df_bancos_curated.columns


# In[134]:


df_bancos_curated.createOrReplaceTempView("tb_bancos")
df_comercios_curated.createOrReplaceTempView("tb_comercios")
df_contracargos_curated.createOrReplaceTempView("tb_Contracargos")

df_join = spark.sql("SELECT a.TIPO_CONTROVERSIA,a.ESTADO_CONTRACARGO,a.DETALLE,a.FECHA_TRANSACCION,a.IMPORTE_TRANSACCION,a.IMPORTE_DISPUTABLE,b.BANCO_CAJA,a.FECHA_ACCION,a.GESTOR,a.ACCION,a.CANAL,c.NOMBRE_COMERCIO FROM tb_Contracargos a inner join tb_bancos b on b.COD_BANCO = a.COD_BANCO inner join tb_comercios c on c.COD_COMERCIO = a.CODIGO_COMERCIO")

df_join.show(5)


# In[135]:


df_join.columns


# In[136]:


df_join.show(5)


# In[137]:


df_join.write.mode("overwrite").format("parquet").save(ruta_functional)


# In[ ]:


print("Las capas Landing, Curated y Functional han sido pobladas satisfactoriamente.")

