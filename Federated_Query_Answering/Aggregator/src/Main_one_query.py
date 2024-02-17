from workload.domain import Domain
from Aggregator import Aggregator

import sys


## Arguments are well paseed but i don see the exchange of values


if __name__ == "__main__":
    args  = sys.argv

    number_data_providers = int(args[1])
    ip = "127.0.0.1"


    adult_domain = Domain(('A' , 'C' , 'D' , 'E' , 'G' , 'K' , 'L' , 'M' , 'N'),(73,99,15,15,14,99,99,98,41))
    c= Aggregator("adult_synth",adult_domain,number_data_providers,ip)
    
    for i in range(5):
        c.__query__(0,2,i)
        c.__query__(1,2,i)
        c.__query__(2,2,i)
        c.__query__(3,2,i)
        c.__query__(4,2,i)