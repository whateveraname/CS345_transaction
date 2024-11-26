from Query import Query
import time
from IPython.display import clear_output
from IPython.display import display
import sys

class VideoStore:
    def __init__(self) -> None:
        self.query = Query()
        self.query.open_connection()
        self.query.prepare_statements()

    def usage(self):
        # prints the choices for commands and parameters
        print()
        print(" *** Please enter one of the following commands *** ")
        print("> search <movie title>")
        print("> plan [<plan id>]")
        print("> rent <movie id>")
        print("> return [<movie id>]")
        print("> fastsearch <movie title>")
        print("> quit")
        print()

    def menu(self, cid):
        while True:
            self.usage()

            self.query.transaction_personal_data(cid)

            response = input("> ").strip()

            clear_output(wait=True)
            print(response)

            if len(response) == 0:
                print("Sorry, please give a command")
                continue

            tokens = response.split()
            command = tokens[0]

            if command == "search":
                if len(tokens) > 1:
                    movie_title = " ".join(tokens[1:])
                    print(f"Searching for the movie '{movie_title}'")
                    start_time = time.time()
                    self.query.transaction_search(cid, movie_title, True)
                    end_time = time.time()
                    print(f"Search completed in {(end_time - start_time):.2f} seconds\n")
                else:
                    print("Error: need to type in movie title")

            elif command == "plan":
                if len(tokens) > 1:
                    try:
                        plan_id = int(tokens[1])
                        if self.query.helper_check_plan(plan_id):
                            print(f"Switching to plan {plan_id}")
                            self.query.transaction_choose_plan(cid, plan_id)
                        else:
                            print(f"Incorrect plan id {plan_id}")
                            print("Available plans are:")
                            self.query.transaction_list_plans()
                    except ValueError:
                        print("Error: provided plan number is not an integer")
                else:
                    print("Available plans:")
                    self.query.transaction_list_plans()

            elif command == "rent":
                if len(tokens) > 1:
                    try:
                        mid = int(tokens[1])
                        print(f"Renting the movie id {mid}")
                        self.query.transaction_rent(cid, mid)
                    except ValueError:
                        print("Error: need to give a numeric movie ID")
                else:
                    print("Error: need to give a movie ID")

            elif command == "return":
                if len(tokens) > 1:
                    try:
                        mid = int(tokens[1])
                        print(f"Returning the movie id {mid}")
                        self.query.transaction_return(cid, mid)
                    except ValueError:
                        print("Error: need to give a numeric movie ID")
                else:
                    self.query.transaction_list_user_rentals(cid)

            elif command == "fastsearch":
                if len(tokens) > 1:
                    movie_title = " ".join(tokens[1:])
                    print(f"Fast Searching for the movie '{movie_title}'")
                    start_time = time.time()
                    self.query.transaction_fast_search(cid, movie_title)
                    end_time = time.time()
                    print(f"Search completed in {(end_time - start_time):.2f} seconds\n")
                else:
                    print("Error: need to type in movie title")

            elif command == "quit":
                print("Exiting...")
                break

            else:
                print(f"Error: unrecognized command '{command}'")

    def login(self, username, password):
        cid = self.query.transaction_login(username, password)
        if (cid >= 0):
            self.menu(cid)
        else:
            print("Sorry, login failed...")

# login("george", "123")