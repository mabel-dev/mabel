from typing import Callable
from ...logging import get_logger

class Groups():
    """
    Group By functionality for Iterables of Dictionaries
    
    Parameters:
        dictset: Iterable of dictionaries:
            The dataset to perform the Group By on
        column: string:
            The name of the field to group by
    
    Returns:
        Groups

    Warning:
        The 'Groups' object holds the entire dataset in memory so is unsuitable
        for large datasets.
    """
    __slots__ = ('groups')

    def __init__(self, dictset, column):
        groups = {}
        for item in dictset:
            my_item = item.copy()
            key = my_item.get(column)
            if groups.get(key) is None:
                groups[my_item.get(column)] = []
            del my_item[column]
            groups[key].append(my_item)
        self.groups = groups

    def count(self, group=None):
        """
        Count the number of items in groups
        
        Parameters:
            group: string (optional)
                If provided, return the count of just this group

        Returns:
            if a group is provided, return an integer
            if no group is provided, return a dictionary
        """
        if group is None:
            return {x:len(y) for x,y in self.groups.items()}
        else:
            try:
                return [len(y) for x,y in self.groups.items() if x == group].pop()
            except:
                return 0

    def aggregate(self, column, method):
        """
        Applies an aggregation function by group.
        
        Parameters:
            column: string
                The name of the field to aggregate on
            method: callable
                The function to aggregate with
        
        Returns:
            dictionary

        Examples:
            maxes = grouped.aggregate('age', max)
            means = grouped.aggregate('age', maths.mean)
        """
        response = {}
        for key, items in self.groups.items():
            values = [item.get(column) for item in items if item.get(column) is not None]
            response[key] = method(values)
        return response

    def apply(self, method: Callable):
        """
        Apply a function to all groups

        Parameters:
            method: callable
                The function to apply to the groups

        Returns:
            dictionary
        """
        return {key:method(items) for key, items in self.groups.items()}
            
    def __len__(self):
        """
        Returns the number of groups in the set.
        """
        return len(self.groups)

    def __repr__(self):
        """
        Returns the group names
        """
        return str(list(self.groups.keys()))
