#!/bin/bash -x
## Create python virtual env with dependencies
cd Data_Provider
virtualenv env_d_p
source env_d_p/bin/activate
pip3 install -r requirements.txt

