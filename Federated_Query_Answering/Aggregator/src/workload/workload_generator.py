
import numpy as np

import itertools
from .query import Query, QueryCondition



PREFIX       = lambda k: f"Prefix-{k}D"
RANDOM_RANGE = lambda size, dim_size, seed: f"Random-Range-queries-({size}-{dim_size}-{seed})"


def random_range_queries(domain, size, dim_size, seed=0):
    return RANDOM_RANGE(size, dim_size, seed), _gen_random_range(domain, size, dim_size, seed=seed, implicit=False)


def direct_random_range_queries(domain, size, dim_size, seed=0):
    return RANDOM_RANGE(size, dim_size, seed), _gen_random_range(domain, size, dim_size, seed=seed, direct=True)


def _gen_random_range(domain, size, dim_size, seed=0, implicit=False, merge=False, direct=False, marginal=False):
    domain_shape = domain.shape
    if type(domain_shape) is int:
        domain = (domain_shape,)
        
    prng = np.random.RandomState(seed)
    combinations = list(itertools.combinations(domain.attrs, dim_size))
    combination_len = len(combinations)
    queries = {}
    
    for i in range(size):
        random_idx = prng.choice(combination_len)
        selected_domains = combinations[random_idx]
        selected_domain_shape = [ domain[dom] for dom in selected_domains ]
        
        shape = tuple(prng.randint(1, dim+1, None) for dim in selected_domain_shape)
        lb = tuple(prng.randint(0, d - q + 1, None) for d,q in zip(selected_domain_shape, shape))
        if marginal:
            ub = lb
        else:
            ub = tuple(sum(x)-1 for x in zip(lb, shape))

        if queries.get(selected_domains):
            queries[selected_domains].append((lb, ub))
        else:
            queries[selected_domains] = [ (lb, ub) ]

    if direct:
        direct_queries = []
        for domains, bounds in queries.items():
            domains_queries = []
            for lb_tuple, ub_tuple in bounds:
                domains_queries.append(Query([ QueryCondition(d, lb, ub) for d, lb, ub in zip(domains, lb_tuple, ub_tuple) ]))
            direct_queries.append(domains_queries)
        return direct_queries

    
 

