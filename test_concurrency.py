import multiprocessing as mp
import time
from Query import Query
import psycopg2

class TestBarrier:
    """Custom barrier for coordinating processes"""
    def __init__(self):
        self.event = mp.Event()
        self.count = mp.Value('i', 0)
        self.process_ready = mp.Event()
        
    def wait(self):
        """Wait for both processes to reach this point"""
        with self.count.get_lock():
            self.count.value += 1
            if self.count.value == 2:
                self.event.set()
        self.event.wait()

def setup_test_data():
    """Setup test data in database"""
    query = Query()
    query.open_connection()
    query.prepare_statements()
    
    # Create test customer with rental plus plan
    query._customer_login_statement.execute("""
        INSERT INTO customers (cid, login, password, fname, lname, pid)
        VALUES (999, 'test_user', 'test_pass', 'Test', 'User', 2)
        ON CONFLICT (cid) DO UPDATE SET pid = 2
    """)
    
    # Clear any existing rentals
    query._rentals_customer_statement.execute("""
        UPDATE movierentals SET status = 'closed' 
        WHERE cid = 999 AND status = 'open'
    """)
    
    query._customer_db.commit()
    query.close_connection()

def rent_process(barrier, movie_ids):
    """Process that rents movies"""
    query = Query()
    query.open_connection()
    query.prepare_statements()
    
    # Signal ready and wait for other process
    barrier.process_ready.set()
    barrier.wait()
    
    # Rent movies
    for mid in movie_ids:
        query.transaction_rent(999, mid)
        # time.sleep(1)  # Small delay between rentals
    

    query.close_connection()

def change_plan_process(barrier):
    """Process that changes customer plan"""
    query = Query()
    query.open_connection()
    query._customer_db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    query.prepare_statements()
    
    class MockCursor:
        def __init__(self, cursor):
            self._cursor = cursor
            self._sql_result = None
            # Monkey patch the cursor to introduce a delay after checking rentals
            original_execute = cursor.execute
            def delayed_execute(*args, **kwargs):
                result = original_execute(*args, **kwargs)
                if "SELECT count(*)" in args[0]:
                    # After checking current rentals, pause to let rental process run
                    self._sql_result = cursor.fetchone()
                    # time.sleep(1)
                return result
            self._execute = delayed_execute
        
        def execute(self, *args, **kwargs):
            self._execute(*args, **kwargs)
        
        def fetchone(self):
            barrier.wait()
            time.sleep(0.5)
            return self._sql_result

    query._rentals_customer_statement = MockCursor(query._rentals_customer_statement)    
    # query._rentals_customer_statement.execute = delayed_execute
    
    # Signal ready and wait for other process
    # barrier.process_ready.set()
    # barrier.wait()
    
    # Try to downgrade from Premium to Basic plan
    query.transaction_choose_plan(999, 1)
    
    query.close_connection()

def verify_final_state():
    """Verify database state after concurrent operations"""
    query = Query()
    query.open_connection()
    query.prepare_statements()
    
    # Check final number of rentals
    query._rentals_customer_statement.execute("""
        SELECT count(*) FROM movierentals 
        WHERE cid = 999 AND status = 'open'
    """)
    rentals = query._rentals_customer_statement.fetchone()[0]
    
    # Check final plan
    query._customer_name_statement.execute("""
        SELECT pid FROM customers WHERE cid = 999
    """)
    plan_id = query._customer_name_statement.fetchone()[0]
    
    # Get plan limit
    query._plans_maxrentals_statement.execute("""
        SELECT max_movies FROM rentalplans WHERE pid = %s
    """, (plan_id,))
    max_allowed = query._plans_maxrentals_statement.fetchone()[0]
    
    query.close_connection()
    return rentals, plan_id, max_allowed

def test_concurrent_plan_change_and_rental():
    """
    Test that demonstrates the race condition between plan change and rental
    using real concurrent database operations.
    """
    # Setup test data
    setup_test_data()
    
    # Create synchronization barrier
    barrier = TestBarrier()
    
    # Create and start processes
    rent_proc = mp.Process(
        target=rent_process,
        args=(barrier, [101, 102, 103])  # Try to rent 3 movies
    )
    plan_proc = mp.Process(
        target=change_plan_process,
        args=(barrier,)
    )
    
    rent_proc.start()
    plan_proc.start()
    
    # Wait for processes to be ready
    barrier.process_ready.wait()
    barrier.process_ready.wait()
    
    # Wait for processes to complete
    rent_proc.join()
    plan_proc.join()
    
    # Verify final state
    rentals, plan_id, max_allowed = verify_final_state()
    
    # If plan was successfully changed to Basic (pid=1), we should not have more than 2 movies
    if plan_id == 1:  # Basic plan
        assert rentals <= max_allowed, (
            f"Concurrency violation detected:\n"
            f"Customer has {rentals} rentals but plan only allows {max_allowed}"
        )
    print("Congratulations! Your implementation is right!")

if __name__ == "__main__":
    for i in range(1):  # Run multiple times
        print(f"Test iteration {i+1}")
        test_concurrent_plan_change_and_rental()