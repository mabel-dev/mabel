"""
This is an contained reading pipeline, intended to be run in a thread/processes.

The processing pipeline is made of a set of steps operating on a defined and finite
dataset:

┌──────────┬────────────────────────────────────────────────────────────┐
│ Function │ Role                                                       │
├──────────┼────────────────────────────────────────────────────────────┤
│ DNF      │ Pre-filter the rows based on a subset of the filters       │
│          │                                                            │
│ Read     │ Read the raw text/binary rows from the file                │
│          │                                                            │
│ Parse    │ Interpret the rows into dictionaries                       │
│          │                                                            │
│ Filter   │ Apply full set of row filters to the read data             │
│          │                                                            │
│ Reduce   │ Aggregate                                                  │
└──────────┴────────────────────────────────────────────────────────────┘
"""



class AtomicReader:

    NOT_INDEXED = 1

    def __init__(self):
        pass

        # this is the representation of the filters in a which makes it
        # easier to use indexes - but can only really be used against
        # a subset of the operators
        self.dnf_filter 

        # this is a dictionary of the indexes for this file 
        self.index

        # this is the parser for the data
        self.parser

        # this is the filter of the collected data, this can be used 
        # against more operators
        self.filter_expression

        # this is aggregation and reducers for the data
        self.aggregator


    def prefilter(self):
        """
        Select rows from the file based on the filters and indexes, this filters
        the data before we've even seen it. This is usually faster as it avoids
        reading files with no records and parsing data we don't want.

        Go over the tree, if the predicate is an operator we can filter on and
        the field in the predicate is indexed, we can apply a pre-filter.

        ORs are fatal if both sides can't be pre-evaluated.
        """

        PRE_FILTERABLE_OPERATORS = ('=')

        def _inner_prefilter(predicate):
            # No filter doesn't filter
            if predicate is None:  # pragma: no cover
                return None

            # If we have a tuple extract out the key, operator and value and do the evaluation
            if isinstance(predicate, tuple):
                key, operator, value = predicate

                if operator in PRE_FILTERABLE_OPERATORS:
                    #do I have an index for this field?
                    if key in 
                return self.NOT_INDEXED

            if isinstance(predicate, list):
                # Are all of the entries tuples? These are ANDed together.
                if all([isinstance(p, tuple) for p in predicate]):
                    evaluations = [_inner_prefilter(p) for p in predicate]

                # Are all of the entries lists? These are ORed together.
                # All of the elements in an OR need to be indexable for use to be
                # able to prefilter.
                if all([isinstance(p, list) for p in predicate]):
                    evaluations = [_inner_prefilter(p) for p in predicate]
                    if not all([e == self.NOT_INDEXED for e in evaluations]):
                        return self.NOT_INDEXED
                    else


                # if we're here the structure of the filter is wrong
                raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

            raise InvalidSyntaxError("Unable to evaluate Filter")  # pragma: no cover

        return _inner_prefilter(self.dnf_fiter)



    def __call__(self):
        pass
