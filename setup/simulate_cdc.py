import random
import uuid
from datetime import datetime, timedelta
import psycopg2
from pymongo import MongoClient
from psycopg2.extras import execute_batch
from faker import Faker

from dagster_code.resources.config import POSTGRES_CONFIG, MONGO_CONFIG

PROFILE = "medium"

LOAD_PROFILES = {
    "light":  {"plans_insert": 200,  "plans_update": 100,  "txns_insert": 2000,  "txns_update": 500,  "users_insert": 50,   "users_update": 100},
    "medium": {"plans_insert": 1000, "plans_update": 500,  "txns_insert": 10000, "txns_update": 2000, "users_insert": 200,  "users_update": 500},
    "heavy":  {"plans_insert": 5000, "plans_update": 2000, "txns_insert": 50000, "txns_update": 10000,"users_insert": 1000, "users_update": 2000}
}

CFG = LOAD_PROFILES[PROFILE]

fake = Faker()
PRODUCT_TYPES = ['fixed_savings', 'target_savings', 'flexi_savings']
FREQUENCIES = ['daily', 'weekly', 'monthly']
OCCUPATIONS = ['Engineer', 'Doctor', 'Teacher', 'Trader', 'Entrepreneur']
STATES = ['Lagos', 'Abuja', 'Kano', 'Rivers', 'Oyo', 'Ogun']


def simulate_postgres_cdc():
    conn = psycopg2.connect(
        host=POSTGRES_CONFIG["postgres_host"],
        port=POSTGRES_CONFIG["postgres_port"],
        user=POSTGRES_CONFIG["postgres_user"],
        password=POSTGRES_CONFIG["postgres_password"],
        database=POSTGRES_CONFIG["postgres_database"]
    )
    cur = conn.cursor()

    cur.execute("SELECT plan_id FROM savings_plan LIMIT %s", (CFG['plans_update'],))
    for (plan_id,) in cur.fetchall():
        cur.execute("""
            UPDATE savings_plan
            SET amount = amount * %s,
                status = CASE WHEN random() < 0.1 THEN 'completed' ELSE status END,
                updated_at = NOW()
            WHERE plan_id = %s
        """, (random.uniform(1.01, 1.15), plan_id))

    cur.execute("SELECT DISTINCT customer_uid FROM savings_plan LIMIT %s", (CFG['plans_insert'],))
    user_ids = [r[0] for r in cur.fetchall()]
    new_plans = []
    for uid in user_ids:
        start = datetime.now().date()
        end = start + timedelta(days=random.randint(90, 720))
        new_plans.append((
            str(uuid.uuid4()),
            random.choice(PRODUCT_TYPES),
            uid,
            round(random.uniform(5000, 300000), 2),
            random.choice(FREQUENCIES),
            start,
            end,
            'active',
            datetime.now(),
            datetime.now(),
            None
        ))

    execute_batch(cur, """
        INSERT INTO savings_plan (plan_id, product_type, customer_uid, amount, frequency, start_date, end_date, status, created_at, updated_at, deleted_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, new_plans, page_size=1000)

    cur.execute("SELECT plan_id FROM savings_plan ORDER BY RANDOM() LIMIT %s", (CFG['txns_insert'],))
    plan_ids = [r[0] for r in cur.fetchall()]
    new_txns = []
    for pid in plan_ids:
        new_txns.append((
            str(uuid.uuid4()),
            pid,
            round(random.uniform(100, 10000), 2),
            'NGN',
            random.choice(['buy', 'sell']),
            round(random.uniform(0.95, 1.05), 4),
            datetime.now(),
            datetime.now(),
            None
        ))

    execute_batch(cur, """
        INSERT INTO savingsTransaction (txn_id, plan_id, amount, currency, side, rate, txn_timestamp, updated_at, deleted_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, new_txns, page_size=2000)

    cur.execute("SELECT txn_id FROM savingsTransaction ORDER BY RANDOM() LIMIT %s", (CFG['txns_update'],))
    for (txn_id,) in cur.fetchall():
        cur.execute("""
            UPDATE savingsTransaction
            SET rate = rate * %s,
                updated_at = NOW()
            WHERE txn_id = %s
        """, (random.uniform(0.98, 1.05), txn_id))

    conn.commit()
    cur.close()
    conn.close()


def simulate_mongodb_snapshot():
    client = MongoClient(MONGO_CONFIG["mongo_uri"])
    db = client[MONGO_CONFIG["mongo_database"]]
    users = db["users"]

    for user in users.find().limit(CFG['users_update']):
        users.update_one(
            {'_id': user['_id']},
            {'$set': {
                'occupation': random.choice(OCCUPATIONS),
                'state': random.choice(STATES)
            }}
        )

    count = users.count_documents({})
    new_users = [{
        "_id": f"USER{str(count + i + 1).zfill(8)}",
        "firstName": fake.first_name(),
        "lastName": fake.last_name(),
        "occupation": random.choice(OCCUPATIONS),
        "state": random.choice(STATES)
    } for i in range(CFG['users_insert'])]

    if new_users:
        users.insert_many(new_users)

    client.close()

def main():
    print(f"\n🌀 Simulating CDC activity ({PROFILE.upper()} load profile)...")
    start = datetime.now()

    simulate_postgres_cdc()
    simulate_mongodb_snapshot()

    elapsed = (datetime.now() - start).total_seconds()
    print(f"✅ CDC simulation complete in {elapsed:.1f}s\n")

    print(f"Summary ({PROFILE} profile):")
    print(f" - PostgreSQL → {CFG['plans_insert']} new plans, {CFG['txns_insert']} txns inserted")
    print(f" - MongoDB → {CFG['users_insert']} new users")
    print(f" - Updates applied across all datasets\n")

if __name__ == "__main__":
    main()