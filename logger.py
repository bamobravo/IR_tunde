import time
from datetime import datetime

class Log:
    def __init__(self, filename:str, *headers:list[str], datemarkfilename=True, echo=True):
        """
        # Log
        Log Entries into a csv file

        ## Parameters:
        filename: name of the output csv file
        headers: List of table headers of the csv file (strings only)
        datemarkfilename: Append datetime of logging to the file name (boolean)
        echo: Print out log entries

        ## Usage:
        >>> log = Log(test_file.csv, "name", "date")
        >>> log.entry('John', '2022-08-05')
        John,2022-08-05

        ## Profiling block of code with context manager
        With context manager, duration (in nanoseconds) of running codes is taken and is appended to the log entry
        >>> log = Log(profiling.csv, "number")
        >>> for i in range(5):
        ...     with log.profile(str(i)):
        ...         time.sleep(2)
        0,1997986000
        1,1994582100
        2,1998166100
        3,2005464000
        4,2005434200
        >>>
        """
        self.filename = filename if not datemarkfilename else self.__rename(filename, f"({datetime.now().strftime('%Y-%m-%d_%H-%M-%S')})")
        self.headers = headers
        self.echo = echo
        self.__entrypresent = False

        with open(self.filename, 'w') as logfile:
            logfile.write(",".join(self.headers) + "\n")

    def __enter__(self):
        if self.__entrypresent:
            value = None
        else:
            value = lambda *row : self.__start(*row)

        self.start_time = time.perf_counter_ns()

        return value

    def profile(self, *row:list[str]):
        """Profile a block of code (Removes overhead)
        ## Parameter
        @row: List of log column entry (Duration is appended to this)
        """
        self.new_entry = row
        self.__entrypresent = True
        return self

    def __start(self, *row):
        self.new_entry = row

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit_time = time.perf_counter_ns()
        duration = self.exit_time - self.start_time
        self.enter(*self.new_entry, str(duration))
        self.__entrypresent = False

    def enter(self, *row:list[str], ):
    	"""Add a new entry into log file"""
        self.last_entry = row
        line = ",".join(row) + '\n'
        with open(self.filename, 'a') as logfile:
            logfile.write(line)

        if self.echo:
            print(line, end="")


    def __rename(self, filename, more):
        name, ext = filename.split(".")

        return (name + more + "." + ext)
