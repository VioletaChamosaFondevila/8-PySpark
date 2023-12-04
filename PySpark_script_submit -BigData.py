

# **CASO NEGOCIO SPARK**

# encoding: utf-8

from pyspark.sql.functions import * 
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark_IMF_Big_Data').getOrCreate()




# CASO 1

df_facturas_mes_ant = spark.read.table('df_facturas_mes_ant')

print("Total de contratos existentes el mes anterior " + str(df_facturas_mes_ant.groupBy("id_cliente").agg(count("id_oferta").alias("n_contratos")).filter((col("n_contratos")>1)).count()))






# CASO 2

df_facturas_mes_ant = spark.read.table('df_facturas_mes_ant')
df_facturas_mes_actual = spark.read.table('df_facturas_mes_actual')

df_facturas_mes_actual_flg = df_facturas_mes_actual.join(df_facturas_mes_ant.select("id_cliente")\
.withColumn("flg_mes_ant", lit(1)),["id_cliente"],"left")\
.dropDuplicates(["id_cliente","id_oferta","importe"])\
.withColumn("importe_dto", when(col("flg_mes_ant") == 1, (col("importe")*1.07).cast(DecimalType(10,2)))
.otherwise(col("importe")))\
.drop("flg_mes_ant")\
.orderBy(asc("id_cliente"), desc('importe'))\
.show()






# CASO 3

df_facturas_mes_actual = spark.read.table('df_facturas_mes_actual')
df_ofertas = spark.read.table('df_ofertas')

df_facturas_mes_actual_covid = df_facturas_mes_actual.join(df_ofertas.filter(col("descripcion").contains("datos ilimitados")).select('id_oferta')\
.withColumn("flg_mes_ant", lit(1)), ["id_oferta"],"left")\
.withColumn("importe_dto", when(col("flg_mes_ant") == 1, (col("importe")*1.15).cast(DecimalType(10,2)))
.otherwise(col("importe")))\
.drop("flg_mes_ant")\
.show()






# CASO 4

df_clientes = spark.read.table('df_clientes')
df_consumos_diarios = spark.read.table('df_consumos_diarios')

df_consumo_grupos = df_consumos_diarios.join(df_clientes,["id_cliente"],"left") \
.withColumn("grupo_edad", when(col("edad") < 26, lit(1))
.when(col("edad").between(26,40),lit(2))
.when(col("edad").between(41,65),lit(3))
.otherwise(lit(4)))

df = df_consumo_grupos.groupBy("grupo_edad").agg(mean("consumo_datos_MB").cast(DecimalType(10,2)).alias("media_consumo_datos"),
mean("sms_enviados").cast(DecimalType(10,2)).alias("media_sms_enviados"),
mean("minutos_llamadas_movil").cast(DecimalType(10,2)).alias("media_minutos_movil"),
mean("minutos_llamadas_fijo").cast(DecimalType(10,2)).alias("media_minutos_fijo"))\
.orderBy("grupo_edad").show()







# CASO 5

df_clientes = spark.read.table('df_clientes')
df_consumos_diarios = spark.read.table('df_consumos_diarios')

df_consumo_days = df_consumos_diarios.withColumn('Day', date_format('fecha', 'EEEE'))\
.drop('sms_enviados','fecha','minutos_llamadas_fijo')\
.join(df_clientes.select('id_cliente','sexo'), ['id_cliente'], 'left')\
.filter(col("Day").isin(["Friday","Saturday","Sunday"])) \
.groupBy("sexo").agg(mean("consumo_datos_MB").cast(DecimalType(10,2)).alias("consumo_datos_medio"),mean("minutos_llamadas_movil").cast(DecimalType(10,2)).alias("minutos_medios"))\
.show()





# CASO 6

df_clientes = spark.read.table('df_clientes')
df_consumos_diarios = spark.read.table('df_consumos_diarios')


df_clientes_grupos = df_clientes.withColumn("grupo_edad", when(col("edad") < 26, lit(1))
.when(col("edad").between(26,40),lit(2))
.when(col("edad").between(41,65),lit(3))
.otherwise(lit(4)))

df_consumo_15 = df_consumos_diarios.withColumn('day', df_consumos_diarios['fecha'].substr(9, 10)).filter(col('day')<16)\
.groupBy("id_cliente").agg(sum("consumo_datos_MB").alias("consumo_tot_15"),max('sms_enviados').alias("max_sms_enviados_15"))\
.join(df_clientes_grupos.select("id_cliente","nombre","edad","grupo_edad"), ["id_cliente"], 'left')


window_consumo_max = Window.partitionBy("grupo_edad")

df_max_consumer_15 = df_consumo_15.withColumn("datos_moviles_total_15", max("consumo_tot_15").over(window_consumo_max.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))\
.filter(col("datos_moviles_total_15") == col("consumo_tot_15"))\
.select("nombre", "edad", "grupo_edad","consumo_tot_15","max_sms_enviados_15" )\
.show()








# CASO 7

df_clientes = spark.read.table('df_clientes')
df_facturas_mes_ant = spark.read.table('df_facturas_mes_ant')
df_facturas_mes_actual = spark.read.table('df_facturas_mes_actual')
df_consumos_diarios = spark.read.table('df_consumos_diarios')


df_clientes_new = df_facturas_mes_actual.select("id_cliente","importe").join(df_facturas_mes_ant.select("id_cliente"),["id_cliente"],"leftanti") \
.groupBy("id_cliente").agg(sum("importe").alias("importe_total_mes_actual"))\
.join(df_clientes.select("id_cliente","nombre","edad")\
.withColumnRenamed("nombre","nombre_cliente_nuevo"),["id_cliente"],"left")


df_consumos_minutos = df_consumos_diarios.groupBy("id_cliente").agg(sum("minutos_llamadas_movil").alias("tot_minutos_m"),
sum("minutos_llamadas_fijo").alias("tot_minutos_f"))\
.withColumn("total_minutos", col("tot_minutos_m") + col("tot_minutos_f"))\
.join(df_clientes_new,["id_cliente"], "right")\
.orderBy(desc("total_minutos"))\
.select("nombre_cliente_nuevo","edad","importe_total_mes_actual","total_minutos")\
.show()








# CASO 8

df_clientes = spark.read.table('df_clientes')
df_facturas_mes_actual = spark.read.table('df_facturas_mes_actual')
df_consumos_diarios = spark.read.table('df_consumos_diarios')


df_consumos_diarios_sms = df_consumos_diarios.withColumn("SMS", when(col("sms_enviados") == 0, lit(1)).otherwise(lit(0)))\
.groupBy("id_cliente").agg(sum("SMS").alias('n_dias_sin_sms'))

df_ant_clientes = df_facturas_mes_actual.select("id_cliente").join(df_consumos_diarios_sms,["id_cliente"],"left")\
.dropDuplicates(["id_cliente"])\
.join(df_clientes.select("id_cliente","nombre","edad"),["id_cliente"],"left")\
.select("nombre","edad",'n_dias_sin_sms').show(30)









# CASO 9

df_clientes = spark.read.table('df_clientes')
df_facturas_mes_actual = spark.read.table('df_facturas_mes_actual')
df_consumos_diarios = spark.read.table('df_consumos_diarios')


df_clientes_1_oferta = df_facturas_mes_actual.select('id_cliente','id_oferta').groupBy('id_cliente').agg(count('id_oferta').alias('ofertas'))\
.filter(col('ofertas') == 1)\
.join(df_consumos_diarios, ['id_cliente'],'left').drop('fecha', 'ofertas')


window_cliente = Window.partitionBy("id_cliente")

df_consumo_cliente = df_clientes_1_oferta.withColumn("suma_datos",sum("consumo_datos_MB").over(window_cliente.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))\
.withColumn("suma_sms",sum("sms_enviados").over(window_cliente.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))\
.withColumn("suma_min_movil",sum("minutos_llamadas_movil").over(window_cliente.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))) \
.withColumn("suma_min_fijo",sum("minutos_llamadas_fijo").over(window_cliente.rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)))\
.select("id_cliente","suma_datos","suma_sms","suma_min_movil","suma_min_fijo").dropDuplicates()


df_sum_consumo_cliente_2 = df_consumo_cliente.withColumn("flag", lit(1))

window_calculate_max = Window.partitionBy("flag").rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing)

df_max = df_sum_consumo_cliente_2.withColumn("max_datos", max("suma_datos").over(window_calculate_max))\
.withColumn("max_sms", max("suma_sms").over(window_calculate_max))\
.withColumn("max_min_movil", max("suma_min_movil").over(window_calculate_max))\
.withColumn("max_min_fijo", max("suma_min_fijo").over(window_calculate_max))

df_escalado = df_max.withColumn("scaled_datos", col("suma_datos") / col("max_datos"))\
.withColumn("scaled_sms", col("suma_sms") / col("max_sms"))\
.withColumn("scaled_min_movil", col("suma_min_movil") / col("max_min_movil"))\
.withColumn("scaled_min_fijo", col("suma_min_fijo") / col("max_min_fijo"))


df_coefiente_clientes = df_escalado.withColumn("coeficiente_cliente", (col("scaled_datos")*0.3 + col("scaled_sms")*0.1 + col("scaled_min_movil")*0.4 + col("scaled_min_fijo")*0.2).cast(DecimalType(17,3)))\
.select("id_cliente", "coeficiente_cliente").join(df_clientes.select('id_cliente', 'nombre', 'edad'),['id_cliente'], 'left')\
.select('nombre', 'edad', "coeficiente_cliente").orderBy(desc("coeficiente_cliente")).show()










# CASO 10

df_clientes = spark.read.table('df_clientes')
df_facturas_mes_actual = spark.read.table('df_facturas_mes_actual')
df_consumos_diarios = spark.read.table('df_consumos_diarios')


df_consumo_diario = df_facturas_mes_actual.join(df_clientes.select("id_cliente", "edad"),["id_cliente"],"left")\
.withColumn("grupo_edad", when(col("edad") < 26, lit(1)).when(col("edad").between(26,40),lit(2)).when(col("edad").between(41,65),lit(3)).otherwise(lit(4)))\
.dropDuplicates(["id_cliente"])\
.select('id_cliente', 'grupo_edad')\
.join(df_consumos_diarios.select('consumo_datos_MB', 'id_cliente', 'fecha'),['id_cliente'],'left')\
.withColumn('consumo_datos_GB', col('consumo_datos_MB')/1024).drop('consumo_datos_MB')


df_max_clientes = df_consumo_diario.groupBy('id_cliente', 'grupo_edad').agg(sum('consumo_datos_GB').alias('consumo_tot_GB'))


window_max = Window.partitionBy("grupo_edad").orderBy(desc('consumo_tot_GB'))

df_max = df_max_clientes.withColumn("count_people", row_number().over(window_max))\
.filter(col("count_people") < 4)\
.drop("count_people")


consumo_grupos_mes_act = df_max.select('id_cliente').join(df_consumo_diario,['id_cliente'],'left')


window_grupo = Window.partitionBy("grupo_edad").orderBy("fecha")

df_maximo_consumo = consumo_grupos_mes_act.withColumn("sum_datos", sum("consumo_datos_GB").over(window_grupo))\
.dropDuplicates(["sum_datos"])\
.withColumn("mayor_20", when( col("sum_datos") > 20, row_number().over(window_grupo)))\
.filter(col("mayor_20").isNotNull())\
.withColumn("count_rows", row_number().over(window_grupo))\
.filter(col("count_rows") == 1 )


df_total_consum = df_max.groupBy("grupo_edad").agg(sum("consumo_tot_GB").cast(DecimalType(10,2)).alias('datos_movil_total'))


df_final_consum = df_maximo_consumo.select("grupo_edad","fecha").join(df_total_consum, ["grupo_edad"], 'full').orderBy("grupo_edad").show()

