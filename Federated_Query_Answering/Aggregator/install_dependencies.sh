#!/bin/bash -x
## Create python virtual env with dependencies
cd Aggragator
virtualenv env_agg
source env_agg/bin/activate
pip3 install -r requirements.txt
