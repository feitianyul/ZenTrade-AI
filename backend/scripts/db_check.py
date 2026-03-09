import os

import pymysql


def main() -> None:
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3308"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "hatech@1618")
    database = os.getenv("DB_NAME", "panda")
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, phone, password_hash, tenant_id "
                "FROM users ORDER BY created_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                print(row[0], row[1][:8], row[2][:8], row[3])
            cur.execute("SELECT COUNT(*) FROM users")
            print("user_count", cur.fetchone()[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
