from functools import reduce

class Domain:
    def __init__(self, attrs, shape):
        """ Construct a Domain object
        
        :param attrs: a list or tuple of attribute names
        :param shape: a list or tuple of domain sizes for each attribute
        """
        assert len(attrs) == len(shape), 'dimensions must be equal'
        self.attrs = tuple(attrs)
        self.shape = tuple(shape)
        self.config = dict(zip(attrs, shape))

    @staticmethod
    def fromdict(config):
        """ Construct a Domain object from a dictionary of { attr : size } values """
        return Domain(config.keys(), config.values())

    def axes(self, attrs):
        """ return the axes tuple for the given attributes
        :param attrs: the attributes
        :return: a tuple with the corresponding axes
        """
        return tuple(self.attrs.index(a) for a in attrs)

    def contains(self, other):
        """ determine if this domain contains another
        """
        return set(other.attrs) <= set(self.attrs)

    def size(self, attrs=None):
        """ return the total size of the domain """
        if attrs == None:
            return reduce(lambda x,y: x*y, self.shape, 1)
        return self.project(attrs).size()

    def sort(self, how='size'):
        """ return a new domain object, sorted by attribute size or attribute name """
        if how == 'size':
            attrs = sorted(self.attrs, key=self.size)
        elif how == 'name':
            attrs = sorted(self.attrs)
        return self.project(attrs)

    
    def __contains__(self, attr):
        return attr in self.attrs

    def __getitem__(self, a):
        """ return the size of an individual attribute
        :param a: the attribute
        """
        return self.config[a]

    def __iter__(self):
        """ iterator for the attributes in the domain """
        return self.attrs.__iter__()

    def __len__(self):
        return len(self.attrs)

    def __eq__(self, other):
        return self.attrs == other.attrs and self.shape == other.shape

    def __repr__(self):
        inner = ', '.join(['%s: %d' % x for x in zip(self.attrs, self.shape)])
        return 'Domain(%s)' % inner

    def __str__(self):
        return self.__repr__()