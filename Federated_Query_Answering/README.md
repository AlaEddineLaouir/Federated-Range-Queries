# Private Approximate Query over Horizontal Data Federation (Submitted  in EDBT 2025)

Our approach has two parties: the Aggregator and Data Providers. The code related to each party is in different folders.
We will show you how to lunch the test, and the Learning Based Attack we presented in our work.

### Our Approach Overview (Using only DP):

##### Installation Steps for Aggregator:
1. Run the following commands to create a virtual environment for Python:
    ```
    cd Aggregator
    ./install_dependencies.sh
    ```
3. On a machine, launch the Aggregator using the following command:
    ```
    cd Aggregator
    ./lunch_tests.sh 4 
    ```
    The number '4' indicates how many data providers you have.

##### Prerequisite for Data Providers:
- Ensure you have a PostgreSQL instance running on your machine(s) and create an empty database for our code, and adjust the information (login) related to PostgreSQL in the following files:
-- `` DataProvider/src/Connection.py ``
-- `` DataProvider/lunch_tests.sh ``

##### Installation Steps for Data Providers (Each Data Provider is on a Separate Machine):
1. Run the following command to create a virtual environment for Python:
    ```
    cd DataProvider
    ./install_dependencies.sh
    ```
3. On each separate machine, launch the following command to start each data provider:
    ```
    ./lunch_test.sh 0
    ```
    The number '0' indicates the id of the data provider (starting from 0), example the fourth will hav id '3'.

### Our Approach Overview (Using SMC):
##### Installation Steps for Aggregator:
1. Run the following commands to create a virtual environment for Python:
    ```
    cd Aggregator
    ./install_dependencies.sh
    ```
3. On a machine, launch the Aggregator using the following commands:
    ```
    cd Aggregator
    python3 src/Main_smc_.py 4 1 1 -M5 -I0
    ```
    Parameters are: number data providers (here '4'), number query 1-100 (from a random workload of 100 queries), the number of the iteration (is it the first time running the same query second) this helps when saving the results so they will easily identified. "-M5" indicated to the SMC framework (MyPC) there will be '5' participant (Aggregator and 4 data providers). The last parameter "-I0" indicated the id of this participant to the SMC framework.

##### Prerequisite for Data Providers:
- Ensure you have a PostgreSQL instance running on your machine(s) and create an empty database for our code, and adjust the informations (login) related to PostgreSQL in the following files:
-- `` DataProvider/src/Connection.py ``
-- `` DataProvider/lunch_tests.sh ``
- For this experiement, you need also to uncomment the last portion of the file `` DataProvider/src/DataProvider.py`` inside the __main__ function; it is highlighted.

##### Installation Steps for Data Providers (Each Data Provider is on a Separate Machine):
1. Run the following commands to create a virtual environment for Python:
    ```
    cd DataProvider
    ./install_dependencies.sh
    ```
3. On each separate machine, launch the following command to start each data provider:
    ```
    python3 src/DataProvider.py "database name" 0 127.0.0.1 -M5  -I1
    ```
    To this command you give it the database name you associated to the data provider, the second parameter indicates the id '0' of data provider in our system. You give it the IP address of the data provider machine. The last two are associated to the SMC framework(MyPC): "-M5" indicates the number of participants "-I1" indicates the id of the participant to MyPC.

### Our Approach Overview Resilience to Learning Based Attacks:
To lunch this experiment, you follow the same steps as presented for 'Our Approach Overview (Using only DP)' for data providers bu for the Aggregator:
##### Installation Steps for Aggregator (Resilience to Learning Based Attacks Tests):
1. Run the following commands to create a virtual environment for Python:
    ```
    cd Aggregator
    ./install_dependencies.sh
    ```
3. On a machine, launch the Aggregator using the following command:
    ```
    cd Aggregator
    ./lunch_nb_learn.sh 4 SequentialCOUNT
    ```
    The number '4' indicates how many data providers you have. the second parameter indicate which budget management/query to use: SequentialCOUNT, SequentialSUM ,AdvancedCOUNT, AdvancedSUM, CoalitionCOUNT, CoalitionSUM 



