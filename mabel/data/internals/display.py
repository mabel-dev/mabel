from typing import Iterator, Iterable, List


def html_table(dictset: Iterator[dict], limit: int = 5):
    """
    Render the dictset as a HTML table.

    NOTE:
        This exhausts generators so is only recommended to be used on lists.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5

    Returns:
        string (HTML table)
    """

    def _to_html_table(data, columns):

        yield '<table class="table table-sm">'
        for counter, record in enumerate(data):
            if counter == 0:
                yield '<thead class="thead-light"><tr>'
                for column in columns:
                    yield "<th>" + column + "<th>\n"
                yield "</tr></thead><tbody>"

            if (counter % 2) == 0:
                yield '<tr style="background-color:#F4F4F4">'
            else:
                yield "<tr>"
            for column in columns:
                yield "<td>" + str(record.get(column)) + "<td>\n"
            yield "</tr>"

        yield "</tbody></table>"

    rows = []
    columns = []  # type:ignore
    for i, row in enumerate(dictset):
        rows.append(row)
        columns = columns + list(row.keys())
        if (i + 1) == limit:
            break
    columns = set(columns)  # type:ignore

    import types

    footer = ""
    if isinstance(dictset, types.GeneratorType):
        footer = f"\n<p>top {limit} rows x {len(columns)} columns</p>"
        footer += "\nNOTE: the displayed records have been spent"
    if isinstance(dictset, list):
        footer = f"\n<p>{len(dictset)} rows x {len(columns)} columns</p>"

    return "".join(_to_html_table(rows, columns)) + footer


def ascii_table(dictset: Iterator[dict], limit: int = 5):
    """
    Render the dictset as a ASCII table.

    NOTE:
        This exhausts generators so is only recommended to be used on lists.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to render
        limit: integer (optional)
            The maximum number of record to show in the table, defaults to 5

    Returns:
        string (ASCII table)
    """
    result = []
    columns: dict = {}
    cache = []

    # inspect values
    for count, row in enumerate(dictset):
        if count == limit:
            break

        cache.append(row)
        for k, v in row.items():
            length = max(len(str(v)), len(k))
            if length > columns.get(k, 0):
                columns[k] = length

    # draw table
    bars = []
    for header, width in columns.items():
        bars.append("─" * (width + 2))

    # display headers
    result.append("┌" + "┬".join(bars) + "┐")
    result.append("│" + "│".join([k.center(v + 2) for k, v in columns.items()]) + "│")
    result.append("├" + "┼".join(bars) + "┤")

    # display values
    for row in cache:
        result.append(
            "│"
            + "│".join([str(v).center(columns[k] + 2) for k, v in row.items()])
            + "│"
        )

    # display footer
    result.append("└" + "┴".join(bars) + "┘")

    return "\n".join(result)


BAR_CHARS = [r" ", r"▁", r"▂", r"▃", r"▄", r"▅", r"▆", r"▇", r"█"]


def draw_histogram_bins(bins: List[int]):
    """
    Draws a pre-binned set off histogram data
    """
    mx = max(bins)
    bar_height = mx / 8
    if bar_height == 0:
        return " " * len(bins)

    histogram = ""
    for value in bins:
        if value == 0:
            histogram += BAR_CHARS[0]
        else:
            height = int(value / bar_height)
            histogram += BAR_CHARS[height]

    return histogram


def histogram(values: Iterable[int], number_of_bins: int = 10):
    """ """
    bins = [0] * number_of_bins
    mn = min(values)
    mx = max(values)
    interval = ((1 + mx) - mn) / (number_of_bins - 1)

    for v in values:
        bins[int(v / interval)] += 1
    return draw_histogram_bins(bins)
