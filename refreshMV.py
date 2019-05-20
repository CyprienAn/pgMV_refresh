# -*- coding: utf-8 -*-

import psycopg2
from datetime import datetime, date
from config import config
from updateMVList import updateTable

def refreshMV():
    """Connect to the PostgreSQL database server and refresh materialized views"""

    conn = None
    try:
        # Read connection parameters
        params = config()

        # Connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # Create a cursor
        cur = conn.cursor()

    #----------#

        # Refresh materialized views
        print("Start refreshing materialized view...\n")

        selectQuery = """
        SELECT id, schema, view, auto_refresh, periodicity, last_auto_refresh
        FROM public.list_materialized_view
        ORDER BY id;
        """

        try:
            cur.execute(selectQuery)
        except psycopg2.Error as e:
            print(str(e.pgerror))

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
                            print("[REFRESH]" + matview)
                        except psycopg2.Error as e:
                            exitMsg = str(e.pgerror)
                            print("[ERROR]" + matview + ":" + exitMsg)

                    else:
                        print("[ERROR]" + matview + ":" + exitMsg)

                elif currentPerio != "Daily" and currentPerio != "Weekly" and currentPerio != "Monthly":
                    print("[ERROR]" + matview + ":Value for 'periodicity' must be 'Daily', 'Weekly' or 'Monthly'.")

                else:
                    print("[ ]" + matview)
                    
            elif currentAuto is False:
                print("[ ]" + matview)

            else:
                print("[ERROR]" + matview + ":Value for 'auto_refresh' must be True or False.")

        print("\nRefresh complete.\nEnd: " + str(datetime.now()))

        conn.commit()

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
    refreshMV()
