#!/usr/bin/env python3

import threading
import time
import random
import sys
import argparse
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_batch
import logging

class StressTest:
    def __init__(self, host='localhost', port=5432, database='postgres', user='postgres', password=''):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.running = False
        self.errors = []
        self.stats = {
            'batch_inserts': 0,
            'point_inserts': 0,
            'updates': 0,
            'deletes': 0,
            'queries': 0,
            'consistency_checks': 0,
            'inconsistencies': 0
        }
        self.lock = threading.Lock()
        
    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            connect_timeout=10
        )
    
    def setup_tables(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_mooncake;")
                
                cur.execute("DROP TABLE IF EXISTS stress_regular CASCADE;")
                cur.execute("DROP TABLE IF EXISTS stress_columnstore CASCADE;")
                
                cur.execute("""
                    CREATE TABLE stress_regular (
                        id SERIAL PRIMARY KEY,
                        value INTEGER,
                        text_data TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                
                cur.execute("CALL mooncake.create_table('stress_columnstore', 'stress_regular');")
                
            conn.commit()
            logging.info("Tables created successfully")
        except Exception as e:
            logging.error(f"Error setting up tables: {e}")
            raise
        finally:
            conn.close()
    
    def cleanup_tables(self):
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS stress_regular CASCADE;")
                cur.execute("DROP TABLE IF EXISTS stress_columnstore CASCADE;")
            conn.commit()
            logging.info("Tables cleaned up successfully")
        except Exception as e:
            logging.error(f"Error cleaning up tables: {e}")
        finally:
            conn.close()
    
    def batch_insert_worker(self, batch_size=100, iterations=10):
        conn = self.get_connection()
        try:
            while self.running:
                data = [(random.randint(1, 10000), f'batch_text_{random.randint(1, 1000)}') 
                       for _ in range(batch_size)]
                
                with conn.cursor() as cur:
                    execute_batch(
                        cur,
                        "INSERT INTO stress_regular (value, text_data) VALUES (%s, %s)",
                        data
                    )
                conn.commit()
                
                with self.lock:
                    self.stats['batch_inserts'] += len(data)
                
                time.sleep(random.uniform(0.1, 0.5))
                
        except Exception as e:
            with self.lock:
                self.errors.append(f"Batch insert error: {e}")
        finally:
            conn.close()
    
    def point_operations_worker(self):
        conn = self.get_connection()
        try:
            while self.running:
                operation = random.choice(['insert', 'update', 'delete'])
                
                with conn.cursor() as cur:
                    if operation == 'insert':
                        cur.execute(
                            "INSERT INTO stress_regular (value, text_data) VALUES (%s, %s)",
                            (random.randint(1, 10000), f'point_text_{random.randint(1, 1000)}')
                        )
                        with self.lock:
                            self.stats['point_inserts'] += 1
                    
                    elif operation == 'update':
                        cur.execute(
                            "UPDATE stress_regular SET value = %s WHERE id = (SELECT id FROM stress_regular ORDER BY RANDOM() LIMIT 1)",
                            (random.randint(1, 10000),)
                        )
                        if cur.rowcount > 0:
                            with self.lock:
                                self.stats['updates'] += 1
                    
                    elif operation == 'delete':
                        cur.execute(
                            "DELETE FROM stress_regular WHERE id = (SELECT id FROM stress_regular ORDER BY RANDOM() LIMIT 1)"
                        )
                        if cur.rowcount > 0:
                            with self.lock:
                                self.stats['deletes'] += 1
                
                conn.commit()
                time.sleep(random.uniform(0.01, 0.1))
                
        except Exception as e:
            with self.lock:
                self.errors.append(f"Point operation error: {e}")
        finally:
            conn.close()
    
    def query_worker(self):
        conn = self.get_connection()
        try:
            while self.running:
                with conn.cursor() as cur:
                    query_type = random.choice(['count', 'aggregate', 'filter'])
                    
                    if query_type == 'count':
                        cur.execute("SELECT COUNT(*) FROM stress_columnstore")
                    elif query_type == 'aggregate':
                        cur.execute("SELECT AVG(value), MAX(value), MIN(value) FROM stress_columnstore")
                    elif query_type == 'filter':
                        cur.execute("SELECT * FROM stress_columnstore WHERE value > %s LIMIT 10", 
                                  (random.randint(1, 5000),))
                    
                    result = cur.fetchall()
                    
                    with self.lock:
                        self.stats['queries'] += 1
                
                time.sleep(random.uniform(0.05, 0.2))
                
        except Exception as e:
            with self.lock:
                self.errors.append(f"Query error: {e}")
        finally:
            conn.close()
    
    def consistency_checker(self):
        conn = self.get_connection()
        try:
            while self.running:
                with conn.cursor() as cur:
                    cur.execute("CALL mooncake.create_snapshot('stress_columnstore');")
                    
                    cur.execute("SELECT COUNT(*) FROM stress_regular")
                    regular_count = cur.fetchone()[0]
                    
                    cur.execute("SELECT COUNT(*) FROM stress_columnstore")
                    columnstore_count = cur.fetchone()[0]
                    
                    with self.lock:
                        self.stats['consistency_checks'] += 1
                        if regular_count != columnstore_count:
                            self.stats['inconsistencies'] += 1
                            logging.warning(f"Inconsistency detected: regular={regular_count}, columnstore={columnstore_count}")
                
                time.sleep(1.0)
                
        except Exception as e:
            with self.lock:
                self.errors.append(f"Consistency check error: {e}")
        finally:
            conn.close()
    
    def run_stress_test(self, duration=30, batch_workers=2, point_workers=3, query_workers=2):
        logging.info(f"Starting stress test for {duration} seconds")
        logging.info(f"Workers: {batch_workers} batch, {point_workers} point, {query_workers} query")
        
        self.setup_tables()
        self.running = True
        
        threads = []
        
        for _ in range(batch_workers):
            t = threading.Thread(target=self.batch_insert_worker)
            threads.append(t)
            t.start()
        
        for _ in range(point_workers):
            t = threading.Thread(target=self.point_operations_worker)
            threads.append(t)
            t.start()
        
        for _ in range(query_workers):
            t = threading.Thread(target=self.query_worker)
            threads.append(t)
            t.start()
        
        consistency_thread = threading.Thread(target=self.consistency_checker)
        threads.append(consistency_thread)
        consistency_thread.start()
        
        start_time = time.time()
        try:
            while time.time() - start_time < duration:
                time.sleep(1)
                elapsed = int(time.time() - start_time)
                if elapsed % 10 == 0:
                    with self.lock:
                        logging.info(f"Progress: {elapsed}s - Stats: {self.stats}")
        
        except KeyboardInterrupt:
            logging.info("Interrupted by user")
        
        finally:
            self.running = False
            
            for t in threads:
                t.join(timeout=5)
            
            self.print_results()
            self.cleanup_tables()
    
    def print_results(self):
        print("\n" + "="*50)
        print("STRESS TEST RESULTS")
        print("="*50)
        print(f"Batch inserts: {self.stats['batch_inserts']}")
        print(f"Point inserts: {self.stats['point_inserts']}")
        print(f"Updates: {self.stats['updates']}")
        print(f"Deletes: {self.stats['deletes']}")
        print(f"Queries: {self.stats['queries']}")
        print(f"Consistency checks: {self.stats['consistency_checks']}")
        print(f"Inconsistencies found: {self.stats['inconsistencies']}")
        print(f"Errors: {len(self.errors)}")
        
        if self.errors:
            print("\nERRORS:")
            for error in self.errors[-10:]:
                print(f"  - {error}")
        
        if self.stats['inconsistencies'] == 0 and len(self.errors) == 0:
            print("\n✅ STRESS TEST PASSED - No inconsistencies or errors found")
        else:
            print("\n❌ STRESS TEST FAILED - Issues detected")
        
        print("="*50)

def main():
    parser = argparse.ArgumentParser(description='pg_mooncake stress test')
    parser.add_argument('--host', default='/home/ubuntu/.pgrx', help='PostgreSQL host (use /home/ubuntu/.pgrx for Unix socket)')
    parser.add_argument('--port', type=int, default=28817, help='PostgreSQL port (pgrx default: 28817)')
    parser.add_argument('--database', default='pg_mooncake', help='Database name')
    parser.add_argument('--user', default='ubuntu', help='Database user')
    parser.add_argument('--password', default='', help='Database password')
    parser.add_argument('--duration', type=int, default=30, help='Test duration in seconds')
    parser.add_argument('--batch-workers', type=int, default=2, help='Number of batch insert workers')
    parser.add_argument('--point-workers', type=int, default=3, help='Number of point operation workers')
    parser.add_argument('--query-workers', type=int, default=2, help='Number of query workers')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    test = StressTest(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password
    )
    
    try:
        test.run_stress_test(
            duration=args.duration,
            batch_workers=args.batch_workers,
            point_workers=args.point_workers,
            query_workers=args.query_workers
        )
    except Exception as e:
        logging.error(f"Test failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
