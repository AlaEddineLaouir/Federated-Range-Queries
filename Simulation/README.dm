# Private Approximate Query over Horizontal Data Federation (Submitted  in EDBT 2025)



### Simulation Overview
This experiment aims to demonstrate the overhead that may arise when sharing data in Secure Multiparty Computation (SMC) protocols, with a focus on evaluating query performance.

### Step 1: Install Dependencies
To begin, install the necessary dependencies by following these commands:
```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt 
```
### Step 2: Launching Data Providers
To launch one of the four data providers, execute  each of the following commands in a separate terminal:

```
src/python3 main-party.py -M4 -I0
src/python3 main-party.py -M4 -I1
src/python3 main-party.py -M4 -I2
src/python3 main-party.py -M4 -I3
```


