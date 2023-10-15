from orso import DataFrame

from .base_database_writer import BaseDatabaseWriter


class NullWriter(BaseDatabaseWriter):
    def commit(self, dataframe: DataFrame):
        # save the data to an interim table
        naive_inserter = f"INSERT INTO `{self.dataset}` ( {', '.join(dataframe.column_names)} )"
        naive_inserter += ",\n".join(f"\t({row})" for row in dataframe) + ";"

        # row acts like a tuple when used
        # it has asdict, asmap (and others), if you want to access the values in the row

        print(naive_inserter)
