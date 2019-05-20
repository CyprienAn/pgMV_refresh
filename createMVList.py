# -*- coding: utf-8 -*-

import psycopg2
from config import config

def createTable():
    """Connect to the PostgreSQL database server and create the management table"""

    conn = None
    try:
        # Read connection parameters
        params = config()

        # Connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(**params)

        # Create a cursor
        cur = conn.cursor()

    #----------#

        # List and create a table with all Materialized View
        print("Start process...")
        print("Listing and creating table of materialized view...")

        dropQuery = "DROP TABLE IF EXISTS public.list_materialized_view;"
        cur.execute(dropQuery)
        print("SQL Output : ", cur.statusmessage)

        createQuery = """
        CREATE TABLE public.list_materialized_view (id serial, schema text NOT NULL, view text NOT NULL, auto_refresh boolean NOT NULL DEFAULT False, periodicity text NOT NULL DEFAULT 'Daily', last_auto_refresh date NOT NULL DEFAULT '2000-01-01'::date,
        CONSTRAINT list_materialized_view_pkey PRIMARY KEY (id))
        WITH (OIDS=FALSE);
        ALTER TABLE public.list_materialized_view OWNER TO """ + params["user"] + """;
        COMMENT ON TABLE public.list_materialized_view IS 'Management table for automatic refresh of materialized views';"""

        cur.execute(createQuery)
        print("SQL Output : ", cur.statusmessage)

        insertQuery = """
        INSERT INTO public.list_materialized_view (schema, view)
        SELECT (ns.nspname)::information_schema.sql_identifier AS schema,(c.relname)::information_schema.sql_identifier AS view
        FROM pg_namespace ns, pg_class c
        WHERE ((c.relnamespace = ns.oid) AND (c.relkind = 'm'))
        ORDER BY schema, view;
        """

        cur.execute(insertQuery)
        print("SQL Output : ", cur.statusmessage)

        conn.commit()

        print("Process complete.")
        print("Now you have to update manually the 'public.list_materialized_view' to decide the 'auto-refresh' and 'periodicity' values")

    #----------#

        # Close the communication with the PostgreSQL server
        cur.close()

    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    createTable()
