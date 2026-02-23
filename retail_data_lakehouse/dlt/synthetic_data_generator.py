import sys
import os
from datetime import datetime, timezone, timedelta
import time, random
from concurrent.futures import ThreadPoolExecutor
import pyspark.sql.functions as F 

from pipeline_config import *
try:
    from faker import Faker
except:
    %pip install faker
    from faker import Faker

fake = Faker('en_IN')

# --- DATA GENERATION CONFIGURATION ---
CONFIG = {
    "BATCH_INTERVAL_STREAMING": 15,  # 15 seconds = 4 batches/minute = data accumulates quickly
    "LATENCY_MAX_STREAMING": 60,     # Max 60s event time delay (simulates network latency)
    "INITIAL_STORE_COUNT": 50,       # 50 retail stores (mid-size chain)
    "INITIAL_CUSTOMER_COUNT": 5000,  # 5K customers (realistic customer base)
    "INITIAL_PRODUCT_COUNT": 500,    # 500 products (diverse catalog)
    "TRANSACTION_PER_BATCH": 100,    # 100 transactions/batch × 4 batches/min = 400/min = 24K/hour
    "CUSTOMER_BATCH_SIZE": 40,       # 40 new/updated customers per batch (customer growth)
    "PRODUCT_BATCH_SIZE": 25,        # 25 new/updated products per batch (inventory/price updates)
    "STORE_BATCH_SIZE": 2            # 2 new/updated stores per batch (operational changes)
}

# --- Pre-generated lists for realism ---
product_types = [
    # Western - Men
    'T-Shirt', 'Jeans', 'Jacket', 'Shirt', 'Shorts', 'Blazer', 'Sweater',
    # Western - Women
    'Dress', 'Skirt', 'Top', 'Tunic', 'Blouse', 'Gown', 'Denim Jacket',
    # Indian Ethnic - Women
    'Saree', 'Salwar Kameez', 'Lehenga', 'Kurti', 'Dupatta', 'Anarkali', 'Churidar',
    # Indian Ethnic - Men
    'Kurta', 'Sherwani', 'Nehru Jacket', 'Pathani Suit', 'Dhoti', 'Pajama',
    # Unisex/General
    'Hoodie', 'Track Pants', 'Tracksuit', 'Sweatshirt',
    # Unisex - Accessories/Footwear
    'Shoes', 'Sandals', 'Chappal', 'Loafers', 'Belt', 'Cap', 'Scarf', 'Socks', 'Muffler', 'Backpack'
]

colors = [
    'Red', 'Blue', 'Green', 'Black', 'White', 'Yellow', 'Gray', 'Purple', 'Orange', 'Pink', 'Maroon', 'Turquoise', 'Beige', 'Navy', 'Olive', 'Mustard', 'Magenta', 'Peach', 'Teal', 'Gold', 'Silver'
]

sizes = [
    'XS', 'S', 'M', 'L', 'XL', 'XXL', 'Free', 'Kids-4', 'Kids-6', 'Kids-8', 'Kids-10', 'Kids-12'
]

genders = [
    'Men', 'Women', 'Unisex', 'Boys', 'Girls'
    ]

def get_category_from_product_type(ptype: str) -> str:
    """Map product type to appropriate category."""
    ptype_lower = ptype.lower()
    
    # Footwear
    if any(word in ptype_lower for word in ['shoe', 'sandal', 'chappal', 'loafer', 'slipper', 'boot']):
        return 'Footwear'
    
    # Accessories
    if any(word in ptype_lower for word in ['belt', 'cap', 'scarf', 'socks', 'muffler', 'backpack']):
        return 'Accessories'
    
    # Ethnic/Traditional
    if any(word in ptype_lower for word in ['saree', 'salwar', 'kameez', 'lehenga', 'kurti', 'dupatta', 
                                            'anarkali', 'churidar', 'kurta', 'sherwani', 'nehru', 'dhoti', 
                                            'pajama', 'pathani']):
        return random.choice(['Ethnic Wear', 'Traditional', 'Festive Wear', 'Weddingwear'])
    
    # Sportswear/Activewear
    if any(word in ptype_lower for word in ['track', 'sport', 'active', 'gym', 'workout', 'jersey']):
        return random.choice(['Sportswear', 'Activewear'])
    
    # Formal/Office
    if any(word in ptype_lower for word in ['shirt', 'blazer', 'suit', 'tie', 'formal']):
        return random.choice(['Formal Wear', 'Office Wear'])
    
    # Winter
    if any(word in ptype_lower for word in ['jacket', 'sweater', 'hoodie', 'sweatshirt']):
        return random.choice(['Winter Wear', 'Casual Wear'])
    
    # Party/Event
    if any(word in ptype_lower for word in ['gown', 'dress', 'party']):
        return random.choice(['Party Wear', 'Festive Wear', 'Casual Wear'])
    
    # Default
    return random.choice(['Casual Wear', 'Formal Wear', 'Office Wear', 'College Wear'])

# More diverse product names with gender label and category
product_names = []  # Will store tuples: (name, category)
for ptype in product_types:
    category = get_category_from_product_type(ptype)  # Get category once per product type
    for color in colors:
        for size in sizes:
            # Add gender (where applicable)
            if ptype in ['T-Shirt', 'Jeans', 'Jacket', 'Shirt', 'Blazer', 'Kurta', 'Sherwani', 'Nehru Jacket', 'Dhoti', 'Pajama', 'Pathani Suit']:
                gender = 'Men'
            elif ptype in ['Dress', 'Skirt', 'Top', 'Tunic', 'Blouse', 'Saree', 'Salwar Kameez', 'Lehenga', 'Kurti', 'Dupatta', 'Anarkali', 'Churidar']:
                gender = 'Women'
            elif ptype in ['Shorts', 'Gown', 'Denim Jacket', 'Hoodie', 'Track Pants', 'Tracksuit', 'Sweatshirt', 'Shoes', 'Sandals', 'Chappal', 'Loafers', 'Belt', 'Cap', 'Scarf', 'Socks', 'Muffler', 'Backpack']:
                gender = 'Unisex'
            else:
                gender = random.choice(['Men', 'Women', 'Unisex'])
            product_name = f"{color} {ptype} {size} ({gender})"
            product_names.append((product_name, category))  # Store as tuple

# Total possible combinations: ~44 product_types × 21 colors × 12 sizes = ~11,088 combinations
# Limit to 3000 for practicality, but more diverse
product_names = product_names[:3000]

store_locations = [
    'Downtown', 'Mall', 'Suburb', 'Airport', 'Outlet', 'High Street', 'MG Road', 'Connaught Place', 
    'Chandni Chowk', 'Bandra', 'Koramangala', 'Park Street', 'Ghatkopar', 'Vashi', 'Anna Salai',
    'Indiranagar', 'T. Nagar', 'Lajpat Nagar', 'Brigade Road', 'Jayanagar'
]

brands = [
    'Manyavar', 'FabIndia', "Biba", 'W for Woman', 'Pantaloon', 'Raymond', 'Van Heusen', 'Allen Solly', 'Peter England', 'Mufti', 'AND', 'Aurelia', 'Utsa', 'Westside', 'Pantaloons', 'H&M', 'Zara', 'Snitch', 'Marks & Spencer', 'Uniqlo', 'Adidas', 'Nike', 'Reebok', 'Urban Threads', 'Global Desi', 'U.S. Polo Assn.', 'Vivid Apparel', 'Heritage Denim', 'Metro Style', 'Luxe Layers', 'Stride & Co.', 'Blue Horizon', 'Modern Muse', 'Crestline', 'Vogue Venture', 'Atlas Attire', 'Bunaai', 'Roadster', 'Sassafras', 'Bewakoof', 'The Souled Store', 'Wrogn', 'OKhai'
]

payment_methods = [
    'Cash', 'Credit Card', 'Debit Card', 'Mobile Pay', 'Gift Card', 'UPI', 'Net Banking'
]

# --- Global lists to track existing IDs for referential integrity ---
existing_customers = []
existing_products = []
existing_stores = []

def load_existing_ids(path, id_column):
    try:
        df = spark.read.format("delta").load(path)
        ids = [row[id_column] for row in df.select(id_column).distinct().collect()]
        print(f"[{datetime.now()}] Success: Loaded {len(ids)} existing IDs from {path}")
        return ids
    except Exception as e:
        msg = str(e)
        error_message = (
            "DELTA_MISSING_TRANSACTION_LOG",
            "Incompatible format detected",
            "is not a Delta table",
            "Path does not exist",
            "does not exist",
            "No such file or directory",
            "not found",
        )
        if any(indicator in msg for indicator in error_message):
            print(f"[{datetime.now()}] Info: No existing Delta table at {path} ({msg.splitlines()[0]}). Starting fresh.")
            return []
        raise

def generate_sales_stream(path: str, config: dict):
    try:
        max_id = spark.read.format("delta").load(path).selectExpr("max(transaction_id)").collect()[0][0]
        transaction_id_counter = (max_id or 0) + 1
        print(f"[{datetime.now()}] Sales stream: Resuming transaction_id from {transaction_id_counter}")
    except Exception:
        transaction_id_counter = 1
        print(f"[{datetime.now()}] Sales stream: Starting transaction_id from 1")
    
    while True:
        now = datetime.now(timezone.utc)
        data = []

        if not (existing_customers and existing_products and existing_stores):
            print(f"[{datetime.now()}] Sale stream is waiting: Dimension data is not available.")
            time.sleep(10)
            continue

        for _ in range(config['TRANSACTION_PER_BATCH']):
           ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_STREAMING']))
           customer_id = random.choice(existing_customers) 
           product_id = random.choice(existing_products)
           store_id = random.choice(existing_stores)
           
           quantity = random.randint(1, 5)
           if random.random() < 0.05: quantity = -quantity
           unit_price = round(random.uniform(99.0, 999.0), 2)
           total_amount = round(quantity*unit_price, 2)
           discount = round(random.uniform(0, 10), 2)
           if random.random() < 0.02: discount = -discount

           data.append({
                "transaction_id": transaction_id_counter,
                "store_id": store_id, 
                "event_time": ts,
                "customer_id": customer_id, 
                "product_id": product_id, 
                "quantity": quantity,
                "unit_price": unit_price, 
                "total_amount": total_amount, 
                "payment_method": random.choice(payment_methods),
                "discount_applied": discount, 
                "tax_amount": round(total_amount * (0.05 if unit_price <= 1000 else 0.12), 2) # GST: 5% if unit_price <= 1000, else 12%
            })
           transaction_id_counter += 1

        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Sale stream: Ingested {len(data)} new transactions.")
           
        time.sleep(config['BATCH_INTERVAL_STREAMING'])

def generate_customers_stream(path: str, config: dict):
    global existing_customers
    existing_customers = load_existing_ids(path, "customer_id")

    if not existing_customers:
        print(f"[{datetime.now()}] Customer stream: Bootstrapping initial {config['INITIAL_CUSTOMER_COUNT']} customers...")
        now = datetime.now(timezone.utc)
        initial_data = []
        for c_id in range(1, config['INITIAL_CUSTOMER_COUNT']+1):
            gender = random.choice(['Male', 'Female', 'Other'])
            if gender == 'Male':
                name = fake.name_male()
            elif gender == 'Female':
                name = fake.name_female()
            else:
                name = fake.name()
            
            email = fake.email() if random.random() >= 0.02 else None
            initial_data.append({
                "customer_id": c_id,
                "name": name,
                "email": email,
                "address": fake.address().replace('\n', ', '),
                "join_date": now - timedelta(days=random.randint(1,365)),
                "loyalty_points": random.randint(0, 1000),
                "phone_number": fake.phone_number(),
                "age": random.randint(16, 100),
                "gender": gender,
                "last_update_time": now
            })

        if initial_data:
            df = spark.createDataFrame(initial_data)
            df.write.format("delta").mode("append").save(path)
            existing_customers = [row["customer_id"] for row in initial_data]
            print(f"[{datetime.now()}] Customer stream: Initial bootstrap complete.")

    customer_id_counter = max(existing_customers, default = 0) + 1

    while True:
        now = datetime.now(timezone.utc)
        data = []
        num_customers = random.randint(1, config['CUSTOMER_BATCH_SIZE'])

        for _ in range(num_customers):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_STREAMING']))
            if existing_customers and random.random() < 0.3:
                customer_id = random.choice(existing_customers)
            else:
                customer_id = customer_id_counter
                if customer_id not in existing_customers:
                    existing_customers.append(customer_id)
                customer_id_counter += 1
            
            gender = random.choice(['Male', 'Female', 'Other'])
            if gender == 'Male':
                name = fake.name_male()
            elif gender == 'Female':
                name = fake.name_female()
            else:
                name = fake.name()
            
            email = fake.email() if random.random() >= 0.02 else None
            data.append({
                "customer_id": customer_id,
                "name": name,
                "email": email,
                "address": fake.address().replace('\n', ', '),
                "join_date": now - timedelta(days=random.randint(1,365)),
                "loyalty_points": random.randint(0, 1000),
                "phone_number": fake.phone_number(),
                "age": random.randint(16, 70),
                "gender": gender,
                "last_update_time": ts
            })
        
        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)  
            print(f"[{datetime.now()}] Customer stream: Ingested {len(data)} new/updated records.")
            
        time.sleep(config['BATCH_INTERVAL_STREAMING'])

def generate_products_stream(path: str, config: dict):
    global existing_products
    existing_products = load_existing_ids(path, "product_id")

    if not existing_products:
        print(f"[{datetime.now()}] Item stream: Bootstrapping initial {config['INITIAL_PRODUCT_COUNT']} products...")
        now = datetime.now(timezone.utc)
        initial_data = []
        for pid in range(1, config['INITIAL_PRODUCT_COUNT'] + 1):
            name, category = random.choice(product_names)
            name_parts = name.split()
            initial_data.append({
                "product_id": pid,
                "name": name,
                "category": category,
                "brand": random.choice(brands),
                "price": round(random.uniform(99.0, 999.0), 2),
                "stock_quantity": random.randint(0, 200),
                "size": name_parts[-2] if len(name_parts) >= 2 else random.choice(sizes),
                "color": name_parts[0] if name_parts else fake.color_name(),
                "description": fake.sentence(nb_words=8),
                "last_update_time": now
            })
        
        if initial_data:
            df = spark.createDataFrame(initial_data)
            df.write.format("delta").mode("append").save(path)
            existing_products = [row["product_id"] for row in initial_data]
            print(f"[{datetime.now()}] Item stream: Initial bootstrap complete.")
    
    product_id_counter = max(existing_products, default = 0) + 1

    while True:
        now = datetime.now(timezone.utc)
        data = []
        num_items = random.randint(1, config['PRODUCT_BATCH_SIZE'])
        for _ in range(num_items):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_STREAMING']))
            if existing_products and random.random() < 0.3:
                product_id = random.choice(existing_products)
            else:
                product_id = product_id_counter
                if product_id not in existing_products:
                    existing_products.append(product_id)
                product_id_counter += 1
            
            name, category = random.choice(product_names)
            name_parts = name.split()
            data.append({
                "product_id": product_id,
                "name": name,
                "category": category,
                "brand": random.choice(brands),
                "price": round(random.uniform(99.0, 999.0), 2),
                "stock_quantity": random.randint(0, 200),
                "size": name_parts[-2] if len(name_parts) >= 2 else random.choice(sizes),
                "color": name_parts[0] if name_parts else fake.color_name(),
                "description": fake.sentence(nb_words=8),
                "last_update_time": ts
            })
        
        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Item stream: Ingested {len(data)} new/updated records.")
            
        time.sleep(config['BATCH_INTERVAL_STREAMING'])

def generate_stores_stream(path: str, config: dict):
    global existing_stores
    existing_stores = load_existing_ids(path, "store_id")

    if not existing_stores:
        print(f"[{datetime.now()}] Store stream: Bootstrapping initial {config['INITIAL_STORE_COUNT']} stores...")
        now = datetime.now(timezone.utc)
        initial_data = []
        for s_id in range(1, config['INITIAL_STORE_COUNT'] + 1):
            if s_id not in existing_stores:
                existing_stores.append(s_id)
            initial_data.append({
                "store_id": s_id,
                "name": f"Store {s_id} - {random.choice(store_locations)}",
                "address": fake.address().replace('\n', ', '),
                "manager": fake.name() if random.random() >= 0.02 else None,
                "open_date": now - timedelta(days=random.randint(365, 1825)),
                "status": 'Open',
                "phone_number": fake.phone_number(),
                "last_update_time": now
            })
        
        if initial_data:
            df = spark.createDataFrame(initial_data)
            df.write.format("delta").mode("append").save(path)
            existing_stores = [row["store_id"] for row in initial_data]
            print(f"[{datetime.now()}] Store stream: Initial bootstrap complete.")
    
    store_id_counter = max(existing_stores, default = 0) + 1

    while True:
        now = datetime.now(timezone.utc)
        data = []
        num_stores = random.randint(1, config['STORE_BATCH_SIZE'])
        for _ in range(num_stores):
            ts = now - timedelta(seconds=random.uniform(0, config['LATENCY_MAX_STREAMING']))
            if existing_stores and random.random() < 0.4:
                store_id = random.choice(existing_stores)
            else:
                store_id = store_id_counter
                if store_id not in existing_stores:
                    existing_stores.append(store_id)
                store_id_counter += 1
            
            data.append({
                "store_id": store_id,
                "name": f"Store {store_id} - {random.choice(store_locations)}",
                "address": fake.address().replace('\n', ', '),
                "manager": fake.name() if random.random() >= 0.02 else None,
                "open_date": now - timedelta(days=random.randint(365, 1825)),
                "status": random.choice(['Open', 'Under Renovation']),
                "phone_number": fake.phone_number(),
                "last_update_time": ts
            })
        
        if data:
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(path)
            print(f"[{datetime.now()}] Store stream: Ingested {len(data)} new/updated records.")
            
        time.sleep(config['BATCH_INTERVAL_STREAMING'])

streams = [
    (RAW_SALES_PATH, generate_sales_stream),
    (RAW_CUSTOMERS_PATH, generate_customers_stream),
    (RAW_PRODUCTS_PATH, generate_products_stream),
    (RAW_STORES_PATH, generate_stores_stream)
]

with ThreadPoolExecutor(max_workers=len(streams)) as executor:
    for path, generator_func in streams:
        executor.submit(
            generator_func, path, CONFIG
        )
    executor.shutdown(wait=True)