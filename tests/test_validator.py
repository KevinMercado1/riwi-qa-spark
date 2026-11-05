import pytest

from src.validator import (
    SCHEMA,
    crear_spark,
    obtener_errores,
    obtener_errores_cliente,
    obtener_errores_id_duplicado,
    valor_maximo,
    LIMITE_VALOR,
)


@pytest.fixture(scope='module')
def spark():
    sesion = crear_spark()
    yield sesion
    sesion.stop()


def test_datos_validos_no_generan_errores(spark):
    datos = [(1, 101, 50000), (2, 102, 10000), (3, 103, 75000)]
    df = spark.createDataFrame(datos, SCHEMA)
    assert obtener_errores(df).count() == 0


def test_datos_invalidos_son_detectados(spark):
    datos = [(1, 101, 50000), (2, 102, -20000),
             (3, 103, None), (4, 104, 0)]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores(df)

    assert errores.count() == 3
    ids = {fila['id'] for fila in errores.select('id').collect()}
    assert ids == {2, 3, 4}
    

def test_cliente_id_nulo_es_detectado(spark):
    datos = [(1, None, 50000)]  
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores_cliente(df)  

    assert errores.count() == 1


def test_id_duplicado_es_detectado(spark):
    # El id 1 se repite (Regla R2)
    datos = [(1, 101, 50000), (1, 102, 30000), (2, 103, 20000)]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = obtener_errores_id_duplicado(df)

    assert errores.count() == 2
    ids = {fila['id'] for fila in errores.select('id').collect()}
    assert ids == {1}
    

def test_valor_maximo_excedido_es_detectado(spark):
    datos = [(1, 101, 50000), (2, 102, 150000000)]
    df = spark.createDataFrame(datos, SCHEMA)
    errores = valor_maximo(df, LIMITE_VALOR)

    assert errores.count() == 1
    valores = {fila['valor'] for fila in errores.select('valor').collect()}
    assert valores == {150000000}