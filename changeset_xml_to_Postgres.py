import json
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values
from multiprocessing import Process, Queue, cpu_count
import time

file_path = 'F:\\Sachen_F\\OSM_Data\\Geofabrik\\changesets_250421_V2.xml'
#batch_size = 100000

def get_db_connection():
    conn = psycopg2.connect(
        dbname="gis",
        user="mika",
        password="123456",
        host="localhost",
        port="5432"
    )
    return conn

# Consumer: inserts batches into database
def db_inserter(batch_queue, worker_id):
    print(f"[Worker {worker_id}] Started database inserter.")

    conn = get_db_connection()
    cur = conn.cursor()
    inserted_batches = 0
    inserted_rows = 0

    while True:
        batch = batch_queue.get()
        if batch == "DONE":
            print(f"[Worker {worker_id}] Received DONE signal. Inserted {inserted_batches} batches ({inserted_rows} rows).")
            break

        try:
# Change name of postgreSQL-table here
            execute_values(cur, """
                INSERT INTO changesets_250421
                (changeset_id, username, uid, num_changes, comments_count, created_at, closed_at, 
                 min_lat, min_lon, max_lat, max_lon, tags)
                VALUES %s
                ON CONFLICT (changeset_id) DO NOTHING;
            """, batch)
            conn.commit()
            inserted_batches += 1
            inserted_rows += len(batch)

            print(f"[Worker {worker_id}] Inserted batch of {len(batch)} rows (Total rows: {inserted_rows})")

        except Exception as e:
            print(f"[Worker {worker_id}] Error inserting batch: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print(f"[Worker {worker_id}] Exiting.")

# Parse xml with defined batch_size
def parse_and_dispatch(file_path, batch_queue, batch_size=100000):
    print("[Main] Starting to parse XML file...")
    start_time = time.time()

    context = ET.iterparse(file_path, events=('start', 'end'))
    _, root = next(context)

    batch = []
    changeset_count = 0
    batch_count = 0

    for event, elem in context:
        if event == 'end' and elem.tag == 'changeset':
            changeset_id = int(elem.attrib.get('id'))
            user = elem.attrib.get('user')
            uid = int(elem.attrib.get('uid', 0))
            num_changes = int(elem.attrib.get('num_changes', 0))
            comments_count = int(elem.attrib.get('comments_count', 0))
            created_at = elem.attrib.get('created_at')
            closed_at = elem.attrib.get('closed_at')
            min_lat = float(elem.attrib.get('min_lat', 0))
            min_lon = float(elem.attrib.get('min_lon', 0))
            max_lat = float(elem.attrib.get('max_lat', 0))
            max_lon = float(elem.attrib.get('max_lon', 0))

            tags = {tag.attrib.get('k'): tag.attrib.get('v') for tag in elem.findall('tag')}
            tags_json = json.dumps(tags)

            batch.append((
                changeset_id, user, uid, num_changes, comments_count,
                created_at, closed_at, min_lat, min_lon, max_lat, max_lon, tags_json
            ))

            changeset_count += 1

            if len(batch) >= batch_size:
                batch_queue.put(batch)
                batch_count += 1
                print(f"[Main] Dispatched batch #{batch_count} with {len(batch)} changesets (Total changesets parsed: {changeset_count})")
                batch = []

            elem.clear()

    # Final leftover batch
    if batch:
        batch_queue.put(batch)
        batch_count += 1
        print(f"[Main] Dispatched FINAL batch #{batch_count} with {len(batch)} changesets.")

    print(f"[Main] Finished parsing {changeset_count} changesets in {time.time() - start_time:.2f} seconds.")

    # Signal consumers to stop
    for _ in range(cpu_count()):
        batch_queue.put("DONE")

def main():
    print(f"=== Starting import of {file_path} ===")
    start = time.time()

    batch_queue = Queue(maxsize=10)  # allow some backpressure

    num_consumers = min(4, cpu_count())  # choose number of worker processes
    consumers = []

    print(f"[Main] Starting {num_consumers} worker processes...")
    for i in range(num_consumers):
        p = Process(target=db_inserter, args=(batch_queue, i))
        p.start()
        consumers.append(p)

    parse_and_dispatch(file_path, batch_queue)

    for p in consumers:
        p.join()

    print(f"=== Import complete in {time.time() - start:.2f} seconds. ===")

if __name__ == '__main__':
    main()