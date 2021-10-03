import operator
from siphashc import siphash
from collections import defaultdict

AGGREGATORS = {
    "SUM": operator.add,
    "MAX": max,
    "MIN": min,
    "COUNT": lambda x, y: x + 1,
    "AVG": lambda x, y: 1,
}


class TooManyGroups(Exception):
    pass


class GroupBy:
    """
    GroupBy does a lazy evaluation of the groups, the groups are calculated as part of
    calculating the aggregations. This was implemented like this so that generators
    can be aggregated - we have one opportunity to cycle of the records, and if the
    data is in a generator, there's a chance the dataset doesn't fit in memory.
    """

    def __init__(self, dictset, *columns):
        self._dictset = dictset
        if isinstance(columns, (list, set, tuple)):
            self._columns = columns
        else:
            columns = [columns]
        self._group_keys = {}

    def _map(self, *collect_columns):
        """
        Create Tuples of records in the Groups (GroupID, CollectedColumn, Value)

        The GroupID is a hash of the grouped columns, we do this because we don't actually
        care about the column values, just that we can uniquely identify records with
        the same values.

        For each column we're collecting, we emit a record of the column and the value
        in the column.

        This is akin to the MAP step in a MapReduce algo, we're creating a set of values
        which standardize the format of the data tp be processed and could allow the
        data to be processed in parallel.
        """

        for record in self._dictset:
            group_key = "//".join(
                [f"{record.get(column, '')}" for column in self._columns]
            )
            group_key = siphash("*" * 16, group_key)
            if group_key not in self._group_keys:
                self._group_keys[group_key] = [
                    (column, record.get(column)) for column in self._columns
                ]
                if len(self._group_keys) >= 249999:
                    raise TooManyGroups(
                        f"Groups are not selective enough and too many Groups have been found (stopped at {len(self._group_keys)})."
                    )

            for column in collect_columns:
                if column == "*":
                    yield (group_key, column, "*")
                if record.get(column):
                    yield (group_key, column, record[column])

    def aggregate(self, aggregations):
        """
        This implements steps akin to the REDUCE step in MapReduce.

        We work out with group to to map the result to and then REDUCE the resulant
        value from the set.
        """

        if not isinstance(aggregations, list):
            aggregations = [aggregations]
        if not all(isinstance(agg, tuple) for agg in aggregations):
            raise ValueError("`aggregate` expects a list of Tuples")

        requested_aggs = aggregations.copy()

        # averages need the sum and the count
        for func, col in aggregations:
            if func == "AVG":
                aggregations += [("SUM", col), ("COUNT", col)]

        columns_to_collect = {col for func, col in aggregations}

        collector = defaultdict(dict)
        # Iterate through the data in the groups formatted by the mapper. This data
        # is a list of Tuples of (GroupID, Column Name, Value)
        for record in self._map(*columns_to_collect):
            # For each aggregation, we need to perform the function against the
            # values as they come in - the collector holds the result up to this
            # point in the set.
            for func, col in aggregations:

                key = f"{func}({col})"

                existing = collector[record[0]].get(key)
                value = record[2]

                # the aggregation works by performing a simple calculation on
                # the last known value and the value currently seen. This means
                # we don't need another copy of the full data in memory.
                if existing:
                    if value or func == "COUNT":
                        value = AGGREGATORS[func](existing, value)
                    else:
                        value = existing
                elif func == "COUNT":
                    # the COUNT needs seeding with 1, the next cycles are just
                    # adding 1 to the last value.
                    value = 1

                # update the collector with the latest value
                collector[record[0]][key] = value

        # the order of the resulting data set is the order of the hashes - this
        # will appear random, but will ensure the order is consistent between
        # reruns.
        collector = dict(sorted(collector.items()))

        # We now need to expand out the hashed column names
        for group, results in collector.items():

            for func, col in requested_aggs:
                if func == "AVG":
                    results[f"AVG({col})"] = (
                        results[f"SUM({col})"] / results[f"COUNT({col})"]
                    )

            results = {
                f"{func}({col})": results[f"{func}({col})"]
                for func, col in requested_aggs
            }

            keys = self._group_keys[group]
            for key in keys:
                results[key[0]] = key[1]

            yield results

    def max(self, columns):
        """
        Get the maximum value of a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to collect the maximum value of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("MAX", column) for column in columns])

    def min(self, columns):
        """
        Get the minimum value of a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to collect the minimum value of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("MIN", column) for column in columns])

    def sum(self, columns):
        """
        Get the sum of values in a column, or set of columns, in each group.

        Parameters:
            columns: string or iterable
                The columns to calculate the sum of for each group.

        Yields:
            Dictionary
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("SUM", column) for column in columns])

    def count(self):
        """
        Count the number of items in each group.

        Yields:
            Dictionary
        """
        # COUNT is a little different, it doesn't have any fields to perform the
        # aggregation on.
        # This implementation could be improved by taking a copy of the
        # aggregate() function and removing the bits that aren't needed to just
        # count the values.
        return self.aggregate(("COUNT", "*"))

    def average(self, columns):
        """
        Calculate the average of the items in a group.
        """
        if not isinstance(columns, (tuple, list, set)):
            columns = [columns]
        return self.aggregate([("AVG", column) for column in columns])

    def groups(self):
        """
        Return the set of groups - this is similar to a DISTINCT function
        """
        collector = defaultdict(dict)
        for record in self._map("*"):
            collector[record[0]] = 1
        for group in self._group_keys:
            yield dict(self._group_keys[group])
