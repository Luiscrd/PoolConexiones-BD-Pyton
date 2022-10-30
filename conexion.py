from logger_base import log
from psycopg2 import pool
import sys


class Conexion:

    _DATABASE = 'tesdb2'
    _USERNAME = 'postgres'
    _PASSWORD = '1234'
    _DB_PORT = '5432'
    _HOST = 'localhost'
    _MIN_CON = 1
    _MAX_CON = 5
    _pool = None

    @classmethod
    def obtenrPool(cls):

        if cls._pool is None:

            try:

                cls._pool = pool.SimpleConnectionPool(
                    cls._MAX_CON,
                    cls._MAX_CON,
                    host=cls._HOST,
                    user=cls._USERNAME,
                    password=cls._PASSWORD,
                    port=cls._DB_PORT,
                    database=cls._DATABASE
                )

                log.debug(f'Conexión Correcta: {cls._pool}')

                return cls._pool

            except Exception as e:

                log.error(f'Ocurrió un error al obtener el pool: {e}')

                sys.exit()

        else:

            return cls._pool

    @classmethod
    def obtenerConexion(cls):

        conexion = cls.obtenrPool().getconn()

        log.debug(f'Conexión obtenida del pool: {conexion}')

        return conexion

    @classmethod
    def liberarConexion(cls, conexion):

        cls.obtenrPool().putconn(conexion)

        log.debug(f'Regresamos la conexión al pool: {conexion}')

    @classmethod
    def cerrarConexiones(cls):

        cls.obtenrPool().closeall()

        log.debug('Conexión cerrada')


if __name__ == '__main__':

   conexion1 = Conexion.obtenerConexion()

   Conexion.liberarConexion(conexion1)

   conexion2 = Conexion.obtenerConexion()

   Conexion.liberarConexion(conexion2)