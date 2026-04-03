import os
import psycopg2
from connect import get_connection


def _load_sql(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def setup_database():
    create_table = """
        CREATE TABLE IF NOT EXISTS phonebook (
            id         SERIAL PRIMARY KEY,
            first_name VARCHAR(50) NOT NULL,
            last_name  VARCHAR(50) NOT NULL,
            phone      VARCHAR(20) NOT NULL,
            UNIQUE (first_name, last_name)
        );
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_table)
            cur.execute(_load_sql("functions.sql"))
            cur.execute(_load_sql("procedures.sql"))
        conn.commit()


def search_contacts(pattern):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM search_contacts(%s);", (pattern,))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def upsert_contact(first_name, last_name, phone):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL upsert_contact(%s, %s, %s);", (first_name, last_name, phone))
        conn.commit()


def bulk_insert_contacts(names, phones):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL bulk_insert_contacts(%s::text[], %s::text[]);", (names, phones))
            conn.commit()
            cur.execute("SELECT * FROM invalid_contacts;")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def get_contacts_page(page=1, page_size=10):
    offset = (page - 1) * page_size
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM get_contacts_paginated(%s, %s);", (page_size, offset))
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in rows]


def delete_by_phone(phone):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL delete_contact(p_phone => %s);", (phone,))
        conn.commit()


def delete_by_name(first_name, last_name):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CALL delete_contact(p_first_name => %s, p_last_name => %s);", (first_name, last_name))
        conn.commit()


if __name__ == "__main__":
    setup_database()

    upsert_contact("Ali",   "Tursunov",  "+77001234567")
    upsert_contact("Zara",  "Bekova",    "+77009876543")
    upsert_contact("Dias",  "Seitkali",  "+77771112233")
    upsert_contact("Ali",   "Tursunov",  "+77000000001")

    invalid = bulk_insert_contacts(
        names=["Madi Aliev", "BadName", "Kamila Ospanova"],
        phones=["+77055556677", "not-a-phone", "+77013334455"],
    )
    print("Invalid:", invalid)

    print(search_contacts("ali"))
    print(get_contacts_page(page=1, page_size=3))

    delete_by_phone("+77009876543")
    delete_by_name("Dias", "Seitkali")