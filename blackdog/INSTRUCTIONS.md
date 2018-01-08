# Instructions

## Install Python modules

Enter docker bash with `sudo docker exec -it <CONTAINER_ID_or_NAME> /bin/bash` and add the modules:

1. `pip install pendulum`
1. `pip install pandas`

## Add PostgreSQL driver to Spark interpreter

1. Enter interpreter section;
2. Choose `spark`;
3. Add artifact `postgresql:postgresql:9.1-901.jdbc4`, save and restart.
