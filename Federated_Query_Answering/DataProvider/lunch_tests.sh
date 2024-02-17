#!/bin/bash -x

cd Data_Provider

# It doesn't harm to re-activiate the env just after its creation

source env_d_p/bin/activate

# Change the database name in the following command

python3 src/DataProvider.py  'DATABASE NAME' $1  $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')

