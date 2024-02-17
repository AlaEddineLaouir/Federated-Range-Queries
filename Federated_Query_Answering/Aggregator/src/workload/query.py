class Query:
    """Range Query representation

        Query have some QueryConditions
    """
    def __init__(self, conditions):
        """
        Args:
            conditions (QueryCondition[]): list of QueryCondition
        """
        self.conditions = conditions
        self.size=0
        self.allo=0
        
    def show(self):
        """Print query information.

        Examples:
            >>> Query([ QueryCondition(0, 2, 3), QueryCondition(1, 0, 0) ]).show()
            [(0, 2, 3), (1, 0, 0)]
        
        """
        print([(condition.attribute, condition.start, condition.end) for condition in self.conditions])
    def __query_dicts__(self):
        values = []
        index_query=[]
        strata_query =[]
        for cond in self.conditions:
            values.append([cond.start,cond.end])
            strata_query.append("_"+str(cond.attribute)+"_")
            index_query.append(str(cond.attribute)+"_")
        return dict(zip(strata_query, values)), dict(zip(index_query, values))

        


class QueryCondition:
    """Range Query condition

        Attributes:
            attribute (int): dimension or attribute
            start (int): start of range at self.attribute
            end (int): end of range at self.attribute
    """    
    def __init__(self, attribute, start, end):
        self.attribute = attribute
        self.start = start
        self.end = end