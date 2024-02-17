#!/bin/bash -x


cd Aggragator
virtualenv env_agg
source env_agg/bin/activate
pip3 install -r requirements.txt


# It doesn't harm to re-activiate the env just after its creation
source env_agg/bin/activate

# get the ip adresse of the local machin in network
$ip_adress = $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')


pyro5-ns  -n $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')   &


python3 src/Main_adult_.py $1 $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')
python3 src/Main_amazon_.py $1 $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')

