from conexion import Conexion
from cursor_del_pool import CursorDelPool
from persona import Persona
from logger_base import log


class PersonaDAO:

    _SELECCIONAR = 'SELECT * FROM persona ORDER BY id_persona'
    _INSERTAR = 'INSERT INTO persona(nombre, apellido, email) VALUES(%s, %s, %s)'
    _ACTUALIZAR = 'UPDATE persona SET nombre=%s, apellido=%s, email=%s WHERE id_persona=%s'
    _ELIMINAR = 'DELETE FROM persona WHERE id_persona=%s'

    @classmethod
    def selecionar(cls):

        with CursorDelPool() as cursor:

            cursor.execute(cls._SELECCIONAR)

            registros = cursor.fetchall()

            personas = []

            for registro in registros:

                persona = Persona(registro[0], registro[1], registro[2], registro[3])

                personas.append(persona)

            return personas

    @classmethod
    def insertar(cls, persona):

        with CursorDelPool() as cursor:

            valores = (persona.nombre, persona.apellido, persona.email)

            cursor.execute(cls._INSERTAR, valores)

            log.debug(f'Persona insertada: {persona}')

            return cursor.rowcount

    @classmethod
    def actualizar(cls, persona):

        with CursorDelPool() as cursor:

            valores = (persona.nombre, persona.apellido, persona.email, persona.id_persona)

            cursor.execute(cls._ACTUALIZAR, valores)

            log.debug(f'Persona Actualizada: {persona}')

            return cursor.rowcount

    @classmethod
    def eliminar(cls, persona):

        with CursorDelPool() as cursor:

            valores = (persona.id_persona,)

            cursor.execute(cls._ELIMINAR, valores)

            log.debug(f'Persona Eliminada: {persona}')

            return cursor.rowcount


if __name__ == '__main__':

    #persona1 = Persona(nombre='Pedro', apellido='Alvarez', email='palvarez@mail.com')

    #personas_insertadas = PersonaDAO.insertar(persona1)

    #log.debug(f'Personas insertadas: {personas_insertadas}')

    persona2 = Persona(id_persona=10, nombre='Carlos', apellido='Saquete', email='csaquete@mail.com')

    personas_actualizadas = PersonaDAO.actualizar(persona2)

    log.debug(f'Personas actualizadas: {personas_actualizadas}')

    #persona3 = Persona(id_persona=11)

    #personas_eliminadas = PersonaDAO.eliminar(persona3)

    #log.debug(f'Personas eliminadas: {personas_eliminadas}')

    personas = PersonaDAO.selecionar()

    for persona in personas:

        log.debug(persona)
