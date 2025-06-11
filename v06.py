import requests
import mysql.connector
from datetime import datetime
from multiprocessing import Pool, cpu_count
import time
import csv
import os

# Log fonksiyonu
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def process_awb(req):
    awb_id = req['id']
    awb_number = req['awb_number']
    order_id = req['order_id']
    current_index = req['current_parcel_num']
    user_id = req['user_id']

    ORDER_ITEMS_CSV = f"/tmp/order_items_{awb_id}.csv"
    ORDERS_LOGS_CSV = f"/tmp/orders_logs_{awb_id}.csv"

    try:
        db = mysql.connector.connect(host="localhost", port=3307, user="root", password="", database="SmartEmkWarehouseDB", allow_local_infile=True)
        cursor = db.cursor(dictionary=True)

        cursor.execute("SELECT name FROM users WHERE id = %s", (user_id,))
        user = cursor.fetchone()
        created_by = user['name'] if user else "unknown"

        log(f"🚚 AWB işleniyor: {awb_number} (başlangıç: {current_index})")
        cursor.execute("UPDATE awb_requests SET status = %s, current_parcel_num = %s, msg = %s, updated_at = NOW() WHERE id = %s", ("In Process", current_index, None, awb_id))
        db.commit()

        response = requests.get(f"https://united.az/v1/public/parcel-chutes/{awb_number}")
        if not response.ok:
            cursor.execute("UPDATE awb_requests SET status = %s, msg = %s, updated_at = NOW() WHERE id = %s", ("404", response.text, awb_id))
            db.commit()
            log(f"❌ {awb_number} için veri bulunamadı.")
            return

        data = response.json().get("data", [])
        total = len(data)
        if not total:
            cursor.execute("UPDATE awb_requests SET status = %s, updated_at = NOW() WHERE id = %s", ("404", awb_id))
            db.commit()
            return

        cursor.execute("SELECT code, id FROM stocks")
        stock_cache = {row['code']: row['id'] for row in cursor.fetchall()}

        cursor.execute("SELECT stock_id, id FROM current_stocks WHERE box_id = 0")
        curr_stk_cache = {(row['stock_id']): row['id'] for row in cursor.fetchall()}

        cursor.execute("SELECT name, id FROM customers")
        customer_cache = {row['name']: row['id'] for row in cursor.fetchall()}

        cursor.execute("SELECT name, id FROM orders")
        order_cache = {row['name']: row['id'] for row in cursor.fetchall()}

        with open(ORDER_ITEMS_CSV, "w", newline='') as f_items, open(ORDERS_LOGS_CSV, "w", newline='') as f_logs:
            items_writer = csv.writer(f_items)
            logs_writer = csv.writer(f_logs)

            for i in range(current_index, total):
                item = data[i]
                barcode = item.get("barcode", "none")
                chute = item.get("chute", 0)
                city = item.get("city", "unnamed")
                order_name = f"{awb_number}_{awb_id}_200"

                stock_id = stock_cache.get(barcode)
                if not stock_id:
                    cursor.execute("INSERT INTO stocks (name, code, production_date, weight, volume) VALUES (%s, %s, NOW(), 0, 0)", (barcode, barcode))
                    db.commit()
                    stock_id = cursor.lastrowid
                    stock_cache[barcode] = stock_id

                curr_stk_id = curr_stk_cache.get(stock_id)
                if not curr_stk_id:
                    cursor.execute("INSERT INTO current_stocks (stock_id, box_id, amount,weight,volume,created_at,updated_at) VALUES (%s, 0,0,0,0,NOW(),NOW())", (stock_id,))
                    db.commit()
                    curr_stk_id = cursor.lastrowid
                    curr_stk_cache[stock_id] = curr_stk_id

                cursor.execute("SELECT id FROM barcodes WHERE curr_stk_id = %s AND barcode = %s", (curr_stk_id, barcode))
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO barcodes (curr_stk_id, barcode) VALUES (%s, %s)", (curr_stk_id, barcode))
                    db.commit()

                customer_id = customer_cache.get(city)
                if not customer_id:
                    cursor.execute("INSERT INTO customers (name, address, post_code, phone, email) VALUES (%s, %s, %s, %s, %s)", (city, "null", "null", "null", "null"))
                    db.commit()
                    customer_id = cursor.lastrowid
                    customer_cache[city] = customer_id

                if order_id:
                    order_id_to_use = order_id
                else:
                    order_id_to_use = order_cache.get(order_name)
                    if not order_id_to_use:
                        cursor.execute("INSERT INTO orders (name, priority, start_date, end_date, ignore_all, ignore_stock) VALUES (%s, 'Default', NOW(), NOW(), 1, 1)", (order_name,))
                        db.commit()
                        order_id_to_use = cursor.lastrowid
                        order_cache[order_name] = order_id_to_use

                now = datetime.now()
                items_writer.writerow([
                    order_id_to_use, i + 1, curr_stk_id, customer_id, 'Default', 1, 0, 0, chute, 'New', '', now, now
                ])
                logs_writer.writerow([
                    0, order_id_to_use, i + 1, curr_stk_id, customer_id, 'Default', 1, 0, 0, chute, 'New', '', created_by, 'insert from API', now
                ])



        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{ORDER_ITEMS_CSV}'
            INTO TABLE order_items
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            (
                order_id, order_sort_num, curr_stk_id, customer_id,
                type, orderQty, pickingQty, putawayQty, putaway_pin,
                status, shipping_number, created_at, updated_at
            )
        """)

        cursor.execute(f"""
            LOAD DATA LOCAL INFILE '{ORDERS_LOGS_CSV}'
            INTO TABLE orders_logs
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 0 LINES
            (
                order_item_id, order_id, order_sort_num, curr_stk_id, customer_id,
                type, orderQty, pickingQty, putawayQty, putaway_pin, status, 
                shipping_number, used_barcode_num, created_by, created_at, action
            )
        """)

        db.commit()

        cursor.execute("UPDATE awb_requests SET status = %s, current_parcel_num = %s, updated_at = NOW() WHERE id = %s", ("Completed", total, awb_id))
        db.commit()
        db.close()

        log(f"✅ Tamamlandı: {awb_number} ({total} kayıt)")

    except Exception as e:
        try:
            cursor.execute("UPDATE awb_requests SET status = %s, msg = %s, updated_at = NOW() WHERE id = %s", ("Fail", str(e), awb_id))
            db.commit()
            db.close()
        except:
            pass
        log(f"💥 Genel hata: {e}")


class Uploader:

    def __init__(self):
        while True:
            db_main = mysql.connector.connect(host="localhost", port=3307, user="root", password="",
                                              database="SmartEmkWarehouseDB")
            cursor_main = db_main.cursor(dictionary=True)
            cursor_main.execute("SELECT * FROM awb_requests WHERE status = 'New'")
            all_awbs = cursor_main.fetchall()
            db_main.close()

            if not all_awbs:
                log("📭 Bekleyen AWB yok.")
                time.sleep(1)
                continue

            with Pool(cpu_count()) as pool:
                pool.map(process_awb, all_awbs)

            time.sleep(1)
