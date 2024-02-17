from workload.domain import Domain
from Aggregator import Aggregator

import sys




if __name__ == "__main__":
    args  = sys.argv
    print(args)
    number_data_providers = int(args[1])
    ip= str(args[2])


    adult_domain = Domain(('A' , 'C' , 'D' , 'E' , 'G' , 'K' , 'L' , 'M' , 'N'),(73,99,15,15,14,99,99,98,41))
    c= Aggregator("adult_synth",adult_domain,number_data_providers,ip)

    c.__create_workload__()
    c.__sampling_rate__()
    c.__epsilon__()



