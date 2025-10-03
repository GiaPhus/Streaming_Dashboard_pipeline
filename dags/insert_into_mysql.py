import json
import mysql.connector
from crawl_data import get_user_with_purchase

def insert_record_to_mysql(record):
    user = record["user"]
    purchase = record["purchase"]

    conn = mysql.connector.connect(
    host="mysql",
    port=3306,
    user="appuser",
    password="apppass",
    database="staging_db"
    )
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO users (user_id, gender, title, first_name, last_name, email,
                           dob_date, dob_age, registered_date, registered_age,
                           phone, cell, nat)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE email=VALUES(email)
    """, (
        user["login"]["uuid"],
        user["gender"],
        user["name"]["title"],
        user["name"]["first"],
        user["name"]["last"],
        user["email"],
        user["dob"]["date"],
        user["dob"]["age"],
        user["registered"]["date"],
        user["registered"]["age"],
        user["phone"],
        user["cell"],
        user["nat"]
    ))

    # --- Insert logins ---
    cursor.execute("""
        INSERT INTO logins (user_id, username, password, salt, md5, sha1, sha256)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE username=VALUES(username)
    """, (
        user["login"]["uuid"],
        user["login"]["username"],
        user["login"]["password"],
        user["login"]["salt"],
        user["login"]["md5"],
        user["login"]["sha1"],
        user["login"]["sha256"]
    ))

    # --- Insert locations ---
    loc = user["location"]
    cursor.execute("""
        INSERT INTO locations (user_id, street_number, street_name, city, state, country, postcode,
                               latitude, longitude, timezone_offset, timezone_desc)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE city=VALUES(city)
    """, (
        user["login"]["uuid"],
        loc["street"]["number"],
        loc["street"]["name"],
        loc["city"],
        loc["state"],
        loc["country"],
        str(loc["postcode"]),
        loc["coordinates"]["latitude"],
        loc["coordinates"]["longitude"],
        loc["timezone"]["offset"],
        loc["timezone"]["description"]
    ))

    # --- Insert purchases ---
    cursor.execute("""
        INSERT INTO purchases (purchase_id, user_id, product, unit_price, quantity, total, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE total=VALUES(total)
    """, (
        purchase["purchase_id"],
        purchase["user_id"],
        purchase["product"],
        purchase["unit_price"],
        purchase["quantity"],
        purchase["total"],
        purchase["timestamp"]
    ))

    conn.commit()
    cursor.close()
    conn.close()


# Demo chạy
if __name__ == "__main__":
    record = get_user_with_purchase()
    insert_record_to_mysql(record)
    print(" Insert thành công vào MySQL")
