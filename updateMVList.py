# -*- coding: utf-8 -*-

import psycopg2
from config import config
from datetime import datetime

def updateTable():
    """Connect to the PostgreSQL database server and add missing materialized views in the management table"""

    conn = None
    try:
        # Read connection parameters
        params = config()

        # Connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        print("--------------------\nStart: " + str(datetime.now()))

        # Create a cursor
        cur = conn.cursor()

    #----------#

        # List and add missing Materialized View
        print("Updating the materialized view list...")

        query = """
        WITH updateList AS(
            SELECT (ns.nspname)::information_schema.sql_identifier AS nsp
                ,(c.relname)::information_schema.sql_identifier AS rel
            FROM pg_namespace ns, pg_class c
            WHERE (c.relnamespace = ns.oid) AND (c.relkind = 'm')
                AND CONCAT((ns.nspname)::information_schema.sql_identifier, (c.relname)::information_schema.sql_identifier) NOT IN (
                    SELECT CONCAT(schema, view)
                    FROM public.list_materialized_view)
        )

        INSERT INTO public.list_materialized_view (schema, view, auto_refresh, periodicity)
        SELECT nsp AS schema,rel AS view,False AS auto_refresh,'Daily' AS periodicity
        FROM updateList;
        """

        try:
            cur.execute(query)
            conn.commit()
            print("SQL Output:" + cur.statusmessage + "\nUpdate complete.\n")
        except psycopg2.Error as e:
            print("Update Error:" + str(e.pgerror))

    #----------#

        # Close the communication with the PostgreSQL server
        cur.close()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    updateTable()
