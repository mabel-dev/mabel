from orso import DataFrame


class NullWriter:
    def __init__(self, **kwargs):
        kwargs_passed = [f"{k}={v!r}" for k, v in kwargs.items()]
        self.formatted_args = ", ".join(kwargs_passed)
        self.dataset = kwargs.get("dataset")

    def commit(self, dataframe: DataFrame):
        # save the data to an interim table
        naive_inserter = f"INSERT INTO `{self.dataset}` ( {', '.join(dataframe.column_names)} )"
        naive_inserter += ",\n".join(f"\t({row})" for row in dataframe) + ";"

        # row acts like a tuple when used
        # it has asdict, asmap (and others), if you want to access the values in the row

        print(naive_inserter)

    def finalize(self):
        # merge the interim table into the main table

        print("finalizer")
