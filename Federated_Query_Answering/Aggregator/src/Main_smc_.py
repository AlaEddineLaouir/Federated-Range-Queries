from workload.domain import Domain
from Aggregator import Aggregator

from mpyc.runtime import mpc
from mpyc.runtime import mpc
import asyncio
import sys


## Arguments are well paseed but i don see the exchange of values


if __name__ == "__main__":
    args  = sys.argv

    number_data_providers = int(args[1])
    query = int(args[2])
    iteration= int(args[3])
    ip = "127.0.0.1"


    adult_domain = Domain(('A' , 'C' , 'D' , 'E' , 'G' , 'K' , 'L' , 'M' , 'N'),(73,99,15,15,14,99,99,98,41))
    c= Aggregator("adult_synth",adult_domain,number_data_providers,ip)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(mpc.start())
    c.__query_mpc__(query,2,iteration)

