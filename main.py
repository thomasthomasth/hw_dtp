# Import the datapao_hw function from the homework module that implements the business logic for Task1 and Task2 (in addition to the sys package)

from datapao_hw import homework
import sys

 # Capture the argument provided in the console

task_name = sys.argv[1]

# Create an instance of the class that implements the business logic and run the well-defined methods.

homework_instance = homework(task_name = task_name)
homework_instance.read_csv()
homework_instance.task()
homework_instance.write_to_csv()
