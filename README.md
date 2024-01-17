# Project description

The FPSO vessel contains some equipment and each equipment have multiple sensors. Every time a failure happens, we get all the sensors data from the failed equipment, and we store this information in a log file (the time is in GMT time zone).

The project provides three essential files:

`equipment_failure_sensors.rar`: The log file with sensor data when a failure occurs

`equipment_sensors.csv`: A file with the relationships between sensors and equipment

`equipment.json`: A file containing data about equipment.

## Challenge

- Extract equipment_failure_sensors.rar to a path/folder so you can read the .txt data.
- Structure your data so that queries are optimized according to the data retrieval process regarding equipment, sensors and dates.
- Manipulate file with 5 million items
- Create a method that receives data from files and inserts into database

## Execution instructions
### Clone this repository:
- git clone https://github.com/aninhasalesp/shape_challenge.git
- `cd shape_challenge` to enter the project folder

### Docker:
1. [Instalar o docker](https://docs.docker.com/get-docker/)
2. ``
3. To print result on the screen:
    ```
    adicionar algo aqui
    
    ```

### Poetry/Virtualenv:
1. Install [poetry](https://python-poetry.org/docs/#installation): `pip install poetry`
2. `poetry shell`
3. `poetry install`
4. Para ver a documentação: `python  --help`
5. Exemplos: 
    
