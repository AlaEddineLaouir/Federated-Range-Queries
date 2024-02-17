from workload.domain import Domain
from Aggregator import Aggregator
import sys

if __name__ == "__main__":
    args  = sys.argv
    print(args)
    number_data_providers = int(args[0])
    ip      = str(args[1])
    
    amazon_domain = Domain(("overall","vote","age","category","country"),(5,10,70,28,196))
    c= Aggregator("amazon",amazon_domain,number_data_providers,ip)
    # input("If you want to start workload press S and then enter")
    c.__create_workload__()
    c.__sampling_rate__()
    c.__epsilon__()

