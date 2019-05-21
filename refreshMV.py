# -*- coding: utf-8 -*-

import psycopg2
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date
from config import config

def refreshMV():
    """Connect to the PostgreSQL database server and refresh materialized views with the management table"""

    # Creation of logger object
    logger = logging.getLogger()
    # Set logger level
    logger.setLevel(logging.DEBUG)
    # Set formatter
    formatter = logging.Formatter('%(levelname)s::%(message)s')
    # Create Handler to write logs in a file
    file_handler = RotatingFileHandler('refreshMV.log', 'a', 1000000, 1)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    conn = None
    try:
        logger.info("Start: " + str(datetime.now()))

        # Read connection parameters
        params = config()

        # Connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        logger.info("Connecting to the PostgreSQL database...")

        # Create a cursor
        cur = conn.cursor()

    #----------#
        # List and add missing Materialized View
        logger.info("Updating the materialized view list...")

        query = """
        WITH updateList AS(
            SELECT (ns.nspname)::information_schema.sql_identifier AS nsp
                ,(c.relname)::information_schema.sql_identifier AS rel
            FROM pg_namespace ns, pg_class c
            WHERE (c.relnamespace = ns.oid) AND (c.relkind = 'm')
                AND CONCAT((ns.nspname)::information_schema.sql_identifier, (c.relname)::information_schema.sql_identifier) NOT IN (
                    SELECT CONCAT(schema, view)
                    FROM public.list_materialized_view)
            ORDER BY (ns.nspname)::information_schema.sql_identifier, (c.relname)::information_schema.sql_identifier
        )

        INSERT INTO public.list_materialized_view (schema, view)
        SELECT nsp AS schema, rel AS view
        FROM updateList;
        """

        try:
            cur.execute(query)
            conn.commit()
            logger.info("SQL Output:" + cur.statusmessage + ". Update complete.\n")
        except psycopg2.Error as e:
            logger.error("Update Error:" + str(e.pgerror))

    #----------#
        # Refresh materialized views
        logger.info("Start refreshing materialized view...")

        selectQuery = """
        SELECT id, schema, view, auto_refresh, periodicity, last_auto_refresh
        FROM public.list_materialized_view
        ORDER BY priority, id;
        """

        try:
            cur.execute(selectQuery)
        except psycopg2.Error as e:
            logger.error(str(e.pgerror))

        queryResult = cur.fetchall()

        for item in queryResult:
            currentId = item[0]
            currentSchema = item[1]
            currentView = item[2]
            currentAuto = item[3]
            currentPerio = item[4]
            currentLast = item[5]

            matview = currentSchema + "." + currentView

            if currentAuto is True:
                dateDay = date.today()
                dateList = currentLast
                timeDelta = dateDay - dateList

                if (timeDelta.days >= 1 and currentPerio == "Daily") or (timeDelta.days >= 7 and currentPerio == "Weekly") or (timeDelta.days >= 30 and currentPerio == "Monthly"):
                    refreshQuery = "REFRESH MATERIALIZED VIEW " + matview + ";"
                    try:
                        cur.execute(refreshQuery)
                        exitMsg = cur.statusmessage
                    except psycopg2.Error as e:
                        exitMsg = str(e.pgerror)

                    if exitMsg == "REFRESH MATERIALIZED VIEW":
                        dateQuery = """
                        UPDATE public.list_materialized_view
                        SET last_auto_refresh = date(now())
                        WHERE id = """ + str(currentId) + ";"

                        try:
                            cur.execute(dateQuery)
                            logger.info("[REFRESH]" + matview)
                        except psycopg2.Error as e:
                            exitMsg = str(e.pgerror)
                            logger.error("[ERROR]" + matview + ":" + exitMsg)

                    else:
                        logger.error("[ERROR]" + matview + ":" + exitMsg)

                elif currentPerio != "Daily" and currentPerio != "Weekly" and currentPerio != "Monthly":
                    logger.warning("[ERROR]" + matview + ":Value for 'periodicity' must be 'Daily', 'Weekly' or 'Monthly'.")

                else:
                    logger.info("[ ]" + matview)

            elif currentAuto is False:
                logger.info("[ ]" + matview)

            else:
                logger.warning("[ERROR]" + matview + ":Value for 'auto_refresh' must be True or False.")

        logger.info("Refresh complete.\n")
        conn.commit()

    #----------#
        # Close the communication with the PostgreSQL server
        cur.close()
        logger.info("Database connection closed.")
        logger.info("End: " + str(datetime.now()) + '\n--------------------\n')
    except(Exception, psycopg2.DatabaseError) as error:
        logger.error(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':
    refreshMV()
