import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, IntegerType

from src.validator import (
    crear_spark,
    obtener_errores,
    obtener_errores_cliente,
    obtener_errores_id_duplicado,
    valor_maximo,
    LIMITE_VALOR,
)

# Esquema exacto para las pruebas unitarias
TEST_SCHEMA = StructType([
    StructField('id_limpio', LongType(), True),
    StructField('cliente_id_limpio', IntegerType(), True),
    StructField('valor_limpio', LongType(), True),
])


@pytest.fixture(scope='module')
def spark():
    sesion = crear_spark()
    yield sesion
    sesion.stop()


def test_datos_validos_no_generan_errores(spark):
    datos = [(1, 101, 50000), (2, 102, 10000), (3, 103, 75000)]
    df = spark.createDataFrame(datos, TEST_SCHEMA)
    assert obtener_errores(df).count() == 0


def test_datos_invalidos_son_detectados(spark):
    datos = [(1, 101, 50000), (2, 102, -20000),
             (3, 103, 0), (4, 104, 0)]
    df = spark.createDataFrame(datos, TEST_SCHEMA)
    errores = obtener_errores(df)

    assert errores.count() == 3
    ids = {fila['id_limpio'] for fila in errores.select('id_limpio').collect()}
    assert ids == {2, 3, 4}
    

def test_cliente_id_nulo_es_detectado(spark):
    # Evaluamos cliente_id_limpio nulo (None)
    datos = [(1, None, 50000)]  
    df = spark.createDataFrame(datos, TEST_SCHEMA)
    errores = obtener_errores_cliente(df)  

    assert errores.count() == 1


def test_id_duplicado_es_detectado(spark):
    # El id_limpio 1 se repite dos veces (Regla R2)
    datos = [(1, 101, 50000), (1, 102, 30000), (2, 103, 20000)]
    df = spark.createDataFrame(datos, TEST_SCHEMA)
    errores = obtener_errores_id_duplicado(df)

    assert errores.count() == 2
    ids_unicos = {fila['id_limpio'] for fila in errores.select('id_limpio').collect()}
    assert ids_unicos == {1}
    

def test_valor_maximo_excedido_es_detectado(spark):
    datos = [(1, 101, 50000), (2, 102, 150000000)]
    df = spark.createDataFrame(datos, TEST_SCHEMA)
    errores = valor_maximo(df, LIMITE_VALOR)

    assert errores.count() == 1
    valores = {fila['valor_limpio'] for fila in errores.select('valor_limpio').collect()}
    assert valores == {150000000}