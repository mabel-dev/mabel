from functools import reduce




class DataSet(object):

    def __init__(self):
        pass

    def __repr__(self):
        pass

    def persist(self, MEMORY_OR_DISK):
        pass

    def load(self, LOCATION):
        pass

    def location(self):
        # where to seek back to
        pass

    def seek(self, position):
        # jump to a location
        pass

    def map(self):
        # function to all the datasets
        pass

    def filter(self):
        # filter the records
        pass

    def distinct(self):
        # remove duplicates
        pass

    def sample(self, fraction):
        # select a fraction of the dataset
        pass

    def shuffle(self):
        # randomize the set
        pass

    def split(self, fraction):
        # split the set in two
        pass

    def union(self, other):
        # add two datasets
        pass

    def __add__(self, other):
        # union
        pass

    def intersection(self, other):
        # common records
        pass

    def sort_by_key(self, ascending, key):
        # common records
        pass

    def group_by(self, key):
        # common records
        pass

    def collect(self):
        # convert to a list
        pass

    def aggregate(Self, operation):
        # perform a function on all of the items in the
        # set, e.g. fold([1,2,3],add) == 6
        pass

    def max(self, key=None):
        """
        Find the maximum item in this Data Set.

        Parameters:
            key : function, optional
                A function used to generate key for comparing
        """
        if key is None:
            return reduce(max)
        return reduce(lambda a, b: max(a, b, key=key))

    # min
    # sum
    # count
    # histogram
    # mean
    # variance
    # stddev
    # top
    # first
    # take(self, number)
    # keys = # the columns
    