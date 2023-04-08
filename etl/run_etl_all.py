"""----------------------------------------------------------------------
For first time running of ETL scripts or for resetting of ETL configs:

Script to run all other scripts inside the ETL folder. This uses the os
module to run Python in the command line. This will run the scripts 
according to their order.

Last modified: April 2023
----------------------------------------------------------------------"""

import os


# create blocks
os.system("python3 etl/create_prefect_blocks.py")

# create deployments
os.system("python3 etl/create_prefect_deployments.py")

# start prefect agent
os.system("prefect agent start -q default")