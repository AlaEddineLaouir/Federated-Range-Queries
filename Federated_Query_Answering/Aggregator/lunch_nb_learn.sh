# It doesn't harm to re-activiate the env just after its creation
source env_agg/bin/activate

# get the ip adresse of the local machin in network
$ip_adress = $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')


pyro5-ns -n $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p')   &



python3 src/MainNaiveBayes_adult.py postgres $(awk '/\|--/ && !/\.0$|\.255$/ {print $2}' /proc/net/fib_trie | sed -n '2 p') $2 $3
