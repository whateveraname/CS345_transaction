import psycopg2
import time

class Query:
    def __init__(self):
        self._imdb = None
        self._customer_db = None

        self.imdb_url = 'imdb2015'
        self.customer_url = 'customer'
        self.postgresql_user = 'postgres'
        self.postgresql_password = 'postgres'

        # Prepared statements placeholders
        self._search_statement = None
        self._director_mid_statement = None
        self._actor_mid_statement = None
        self._rents_movie_statement = None
        self._customer_login_statement = None
        self._customer_name_statement = None
        self._still_rent_statement = None
        self._plans_list_statement = None
        self._plans_maxrentals_statement = None
        self._rentals_customer_statement = None
        self._rentals_mid_list_statement = None
        self._movie_name_statement = None
        self._valid_plan_statement = None
        self._valid_movie_statement = None
        self._update_plan_statement = None
        self._current_plan_statement = None
        self._rent_statement = None
        self._return_statement = None
        self._begin_transaction_read_write_statement = None
        self._begin_transaction_read_only_statement = None
        self._commit_transaction_statement = None
        self._rollback_transaction_statement = None

    def open_connection(self):
        """Open connections to two databases: imdb and customer."""
        try:
            self._imdb = psycopg2.connect(
                host='localhost',
                database=self.imdb_url,
                user=self.postgresql_user,
                password=self.postgresql_password
            )
            self._customer_db = psycopg2.connect(
                host='localhost',
                database=self.customer_url,
                user=self.postgresql_user,
                password=self.postgresql_password
            )
            self._customer_db.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close_connection(self):
        """Close database connections."""
        if self._imdb:
            self._imdb.close()
        if self._customer_db:
            self._customer_db.close()

    def prepare_statements(self):
        """Prepare all the SQL statements."""
        try:
            self._search_statement = self._imdb.cursor()
            self._director_mid_statement = self._imdb.cursor()
            self._actor_mid_statement = self._imdb.cursor()
            self._rents_movie_statement = self._customer_db.cursor()
            self._customer_login_statement = self._customer_db.cursor()
            self._customer_name_statement = self._customer_db.cursor()
            self._still_rent_statement = self._customer_db.cursor()
            self._plans_list_statement = self._customer_db.cursor()
            self._plans_maxrentals_statement = self._customer_db.cursor()
            self._rentals_customer_statement = self._customer_db.cursor()
            self._rentals_mid_list_statement = self._customer_db.cursor()
            self._movie_name_statement = self._imdb.cursor()
            self._valid_plan_statement = self._customer_db.cursor()
            self._valid_movie_statement = self._imdb.cursor()
            self._update_plan_statement = self._customer_db.cursor()
            self._current_plan_statement = self._customer_db.cursor()
            self._rent_statement = self._customer_db.cursor()
            self._return_statement = self._customer_db.cursor()
            self._begin_transaction_read_write_statement = self._customer_db.cursor()
            self._begin_transaction_read_only_statement = self._customer_db.cursor()
            self._commit_transaction_statement = self._customer_db.cursor()
            self._rollback_transaction_statement = self._customer_db.cursor()

        except Exception as e:
            print(f"Error preparing statements: {e}")

    def helper_compute_remaining_rentals(self, cid):
        """Calculate remaining rentals."""
        try:
            self._current_plan_statement.execute("SELECT pid from customers WHERE cid = %s", (cid,))
            pid = self._current_plan_statement.fetchone()[0]
            self._rentals_customer_statement.execute("SELECT count(*) FROM movierentals WHERE cid = %s AND status = 'open'", (cid,))
            current_rentals = self._rentals_customer_statement.fetchone()[0]
            self._plans_maxrentals_statement.execute("SELECT max_movies FROM rentalplans WHERE pid = %s", (pid,))
            plan_max = self._plans_maxrentals_statement.fetchone()[0]
            remaining = plan_max - current_rentals
            return remaining
        except Exception as e:
            print(f"Error computing remaining rentals: {e}")

    def helper_compute_customer_name(self, cid):
        """Retrieve customer name by cid."""
        try:
            self._customer_name_statement.execute("SELECT fname, lname FROM customers WHERE cid = %s", (cid,))
            result = self._customer_name_statement.fetchone()
            if result:
                return f"{result[0]} {result[1]}"
            return "customer not found"
        except Exception as e:
            print(f"Error retrieving customer name: {e}")

    def helper_check_plan(self, plan_id):
        """Check if the plan id is valid."""
        try:
            self._valid_plan_statement.execute("SELECT pid FROM rentalplans WHERE pid = %s", (plan_id,))
            return self._valid_plan_statement.fetchone() is not None
        except Exception as e:
            print(f"Error checking plan: {e}")

    def helper_check_movie(self, mid):
        """Check if the movie id is valid."""
        try:
            self._valid_movie_statement.execute("SELECT id FROM movie WHERE id = %s", (mid,))
            return self._valid_movie_statement.fetchone() is not None
        except Exception as e:
            print(f"Error checking movie: {e}")

    def helper_who_has_this_movie(self, mid):
        """Check if someone has rented the movie."""
        try:
            self._rents_movie_statement.execute("SELECT cid FROM movierentals WHERE mid = %s AND status = 'open'", (mid,))
            result = self._rents_movie_statement.fetchone()
            return result[0] if result else -1
        except Exception as e:
            print(f"Error checking movie rental: {e}")

    def transaction_login(self, username, password):
        """Authenticate user login."""
        try:
            self._begin_transaction_read_only_statement.execute("BEGIN TRANSACTION READ ONLY")
            self._customer_login_statement.execute("SELECT cid FROM customers WHERE login = %s AND password = %s", (username, password))
            result = self._customer_login_statement.fetchone()
            self._commit_transaction_statement.execute("COMMIT")
            return result[0] if result else -1
        except Exception as e:
            self._rollback_transaction_statement.execute("ROLLBACK")
            print(f"Error during login: {e}")

    def transaction_personal_data(self, cid):
        try:
            remaining_rentals = self.helper_compute_remaining_rentals(cid)
            customer_name = self.helper_compute_customer_name(cid)
            print(f"Customer {customer_name}")
            print(f"You have {remaining_rentals} available movies for rent")
        except Exception as e:
            print(f"Error querying personal data")

    def transaction_search(self, cid, movie_title, print_stat=True):
        """Search for movies by title and display details."""
        try:
            self._search_statement.execute("SELECT * FROM movie WHERE name ILIKE %s ORDER BY id", (f"%{movie_title}%",))
            movies = self._search_statement.fetchall()

            for movie in movies:
                mid, name, year = movie
                print(f"ID: {mid} NAME: {name} YEAR: {year}")
                
                # Fetch directors
                self._director_mid_statement.execute("SELECT y.id, y.fname, y.lname FROM movie_directors x, directors y WHERE x.mid = %s AND x.did = y.id", (mid,))
                directors = self._director_mid_statement.fetchall()
                for director in directors:
                    print(f"\t\tDirector: {director[1]} {director[2]}")

                # Fetch actors
                self._actor_mid_statement.execute("SELECT y.id, y.fname, y.lname FROM casts x, actor y WHERE x.mid = %s AND x.pid = y.id", (mid,))
                actors = self._actor_mid_statement.fetchall()
                for actor in actors:
                    print(f"\t\tActor: {actor[1]} {actor[2]}")
                
                # Check availability
                if print_stat:
                    has_movie = self.helper_who_has_this_movie(mid)
                    if has_movie == -1:
                        print("\t\tAVAILABLE")
                    elif has_movie == cid:
                        print("\t\tYOU HAVE IT")
                    else:
                        print("\t\tUNAVAILABLE")
        except Exception as e:
            print(f"Error during search: {e}")

    def transaction_choose_plan(self, cid, pid):
        try:
            # self._begin_transaction_read_write_statement.execute("BEGIN TRANSACTION READ WRITE")
            # # self._begin_transaction_read_write_statement.execute("set transaction isolation level serializable")

            # # Check current rentals
            # self._rentals_customer_statement.execute("SELECT count(*) FROM movierentals WHERE cid = %s AND status = 'open'", (cid,))
            # current_rentals = self._rentals_customer_statement.fetchone()[0]

            # print(current_rentals)

            # # Check max rentals allowed by the new plan
            # self._plans_maxrentals_statement.execute("SELECT max_movies FROM rentalplans WHERE pid = %s", (pid,))
            # new_plan_max = self._plans_maxrentals_statement.fetchone()[0]

            # print(new_plan_max)

            # remaining = new_plan_max - current_rentals
            # print(remaining)

            # if remaining < 0:
            #     self._rollback_transaction_statement.execute("ROLLBACK TRANSACTION")
            #     print("You cannot switch to this plan unless you return some movies.")
            # else:
            #     # Update the customer's plan
            #     self._update_plan_statement.execute("UPDATE customers SET pid = %s WHERE cid = %s", (pid, cid))
            #     self._commit_transaction_statement.execute("COMMIT TRANSACTION")

            self._begin_transaction_read_write_statement.execute("BEGIN TRANSACTION READ WRITE")
            self._begin_transaction_read_write_statement.execute("set transaction isolation level serializable")
            # self._update_plan_statement.execute("SELECT pid from customers where cid = %s", (cid,))
            # old = self._update_plan_statement.fetchone()[0]
            self._update_plan_statement.execute("UPDATE customers SET pid = %s WHERE cid = %s", (pid, cid))
            remaining = self.helper_compute_remaining_rentals(cid)
            if remaining < 0:
                # self._update_plan_statement.execute("UPDATE customers SET pid = %s WHERE cid = %s", (old, cid))
                self._rollback_transaction_statement.execute("ROLLBACK TRANSACTION")
                print("select plan failed")
            else:
                self._commit_transaction_statement.execute("COMMIT TRANSACTION")
                # pass


        except Exception as e:
            print(f"Error choosing plan: {e}")

    def transaction_list_plans(self):
        """List all available rental plans."""
        try:
            self._plans_list_statement.execute("SELECT * FROM rentalplans")
            plans = self._plans_list_statement.fetchall()
            for plan in plans:
                pid, name, max_movies, fee = plan
                print(f"{pid}\t{name}\tmax {max_movies} movies\t${fee:.2f}")
        except Exception as e:
            print(f"Error listing plans: {e}")

    def transaction_list_user_rentals(self, cid):
        try:
            print("You are currently renting the following movies:")
            self._rentals_mid_list_statement.execute("SELECT mid FROM movierentals WHERE cid = %s AND status = 'open'", (cid,))
            movies = self._rentals_mid_list_statement.fetchall()

            for movie in movies:
                mid = movie[0]
                self._movie_name_statement.execute("SELECT name FROM movie WHERE id = %s", (mid,))
                movie_name = self._movie_name_statement.fetchone()[0]
                print(f"{mid}\t{movie_name}")
        except Exception as e:
            print(f"Error listing user rentals: {e}")

    def transaction_rent(self, cid, mid):
        try:
            self._begin_transaction_read_write_statement.execute("BEGIN TRANSACTION READ WRITE")
            self._begin_transaction_read_write_statement.execute("set transaction isolation level serializable")
            
            if not self.helper_check_movie(mid):
                print("The movie you requested does not exist.")
                self._rollback_transaction_statement.execute("ROLLBACK TRANSACTION")
                return

            remaining = self.helper_compute_remaining_rentals(cid)
            if remaining <= 0:
                print("You cannot rent more movies with your current plan.")
                self._rollback_transaction_statement.execute("ROLLBACK TRANSACTION")
                return

            has_movie = self.helper_who_has_this_movie(mid)
            if has_movie == -1:
                self._rent_statement.execute("INSERT INTO movierentals (mid, cid, status) VALUES (%s, %s, 'open')", (mid, cid))
                self._commit_transaction_statement.execute("COMMIT TRANSACTION")
                return
            self._rollback_transaction_statement.execute("ROLLBACK TRANSACTION")

            if has_movie == cid:
                print("You already rent this movie.")
            else:
                print("Somebody else is already renting this movie.")
        except Exception as e:
            print(f"Error renting movie: {e}")

    def transaction_return(self, cid, mid):
        try:
            self._begin_transaction_read_write_statement.execute("BEGIN TRANSACTION READ WRITE")
            has_movie = self.helper_who_has_this_movie(mid)
            if has_movie == cid:
                self._return_statement.execute("UPDATE movierentals SET status = 'closed' WHERE cid = %s AND mid = %s", (cid, mid))
                self._commit_transaction_statement.execute("COMMIT TRANSACTION")
                return
            self._rollback_transaction_statement.execute("ROLLBACK TRANSACTION")
            print("You are not currently renting this movie.")
        except Exception as e:
            print(f"Error returning movie: {e}")
