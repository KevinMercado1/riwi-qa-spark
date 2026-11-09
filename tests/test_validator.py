import pytest
from pyspark.sql import SparkSession

from src.validator import (
    crear_spark,
    obtener_errores,
    obtener_errores_cliente,
    obtener_errores_id_duplicado,
    valor_maximo,
    LIMITE_VALOR,
)

@pytest.fixture(scope='session')
def spark():
    sesion = crear_spark()
    yield sesion


def helper_df(spark, datos):
    """Crea un DataFrame tipado mediante Spark SQL, sin depender de Pandas."""
    def sql_value(value, tipo):
        literal = 'NULL' if value is None else str(value)
        return f'CAST({literal} AS {tipo})'

    filas = ', '.join(
        '(' + ', '.join((
            sql_value(id_limpio, 'BIGINT'),
            sql_value(cliente_id_limpio, 'INT'),
            sql_value(valor_limpio, 'BIGINT'),
        )) + ')'
        for id_limpio, cliente_id_limpio, valor_limpio in datos
    )
    return spark.sql(
        f'SELECT * FROM VALUES {filas} '
        'AS datos(id_limpio, cliente_id_limpio, valor_limpio)'
    )


def test_datos_validos_no_generan_errores(spark):
    datos = [(1, 101, 50000), (2, 102, 10000), (3, 103, 75000)]
    df = helper_df(spark, datos)
    assert obtener_errores(df).count() == 0


def test_datos_invalidos_son_detectados(spark):
    datos = [(1, 101, 50000), (2, 102, -20000),
             (3, 103, 0), (4, 104, 0)]
    df = helper_df(spark, datos)
    errores = obtener_errores(df)

    assert errores.count() == 3
    ids = {fila['id_limpio'] for fila in errores.select('id_limpio').collect()}
    assert ids == {2, 3, 4}
    

def test_cliente_id_nulo_es_detectado(spark):
    datos = [(1, None, 50000)]  
    df = helper_df(spark, datos)
    errores = obtener_errores_cliente(df)  

    assert errores.count() == 1


def test_id_duplicado_es_detectado(spark):
    datos = [(1, 101, 50000), (1, 102, 30000), (2, 103, 20000)]
    df = helper_df(spark, datos)
    errores = obtener_errores_id_duplicado(df)

    assert errores.count() == 2
    ids_unicos = {fila['id_limpio'] for fila in errores.select('id_limpio').collect()}
    assert ids_unicos == {1}
    

def test_valor_maximo_excedido_es_detectado(spark):
    datos = [(1, 101, 50000), (2, 102, 150000000)]
    df = helper_df(spark, datos)
    errores = valor_maximo(df, LIMITE_VALOR)

    assert errores.count() == 1
    valores = {fila['valor_limpio'] for fila in errores.select('valor_limpio').collect()}
    assert valores == {150000000}