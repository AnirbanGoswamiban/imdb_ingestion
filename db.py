from logger import send_log_async

def insert_imdb_batch(pool, imdb_ids):
    conn = pool.getconn()
    cur = conn.cursor()

    try:
        cur.executemany("""
            INSERT INTO public.dl_imdb_table (imdb, processed)
            VALUES (%s, 'pending')
            ON CONFLICT (imdb) DO NOTHING
        """, [(imdb,) for imdb in imdb_ids])

        conn.commit()

    except Exception as e:
        conn.rollback()
        send_log_async(
            "error",
            "db.py -> insert_imdb_batch()",
            {"imdb_ids_count": len(imdb_ids)},
            str(e)
        )

    finally:
        cur.close()
        pool.putconn(conn)