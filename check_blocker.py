from db.db import DBLoader
from tunnel.tunnel import Tunneler
import os, time
from dotenv import load_dotenv

load_dotenv()

def _get_tunnel():
    DB_HOST=os.environ['DB_HOST']
    try:
        server = Tunneler(DB_HOST, 5432)

        server = server.connect()

        server.start()
    except:
        server = {
            'local_bind_host': DB_HOST,
            'local_bind_port': 5432,
        }  

    return server

def main():
    server = _get_tunnel()
    conn = DBLoader(server, os.environ['DATABASE'], application_name=f"Blocker Checker")
    conn = conn.connect()
    query = """
        SELECT
            activity.pid,
            activity.usename,
            activity.query,
            blocking.pid AS blocking_id,
            blocking.query AS blocking_query
        FROM pg_stat_activity AS activity
        JOIN pg_stat_activity AS blocking ON blocking.pid = ANY(pg_blocking_pids(activity.pid));
    """
   
    while(True):
        cur = conn.cursor()
        cur.execute(query)
        blocker = cur.fetchall()
        print(blocker)
        conn.commit()
        time.sleep(60)


if __name__ == "__main__":
    main()