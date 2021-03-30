from typing import Iterator


def html_table(
        dictset: Iterator[dict],
        limit: int = 5):
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
    def _to_html_table(data, limit):

        first_row = True
        highlight = False

        yield '<table class="table table-sm">'
        for counter, record in enumerate(data):
            if first_row:
                yield '<thead class="thead-light"><tr>'
                for key, value in record.items():
                    yield '<th>' + key + '<th>\n'
                yield '</tr></thead><tbody>'
            first_row = False

            if counter >= limit:
                break

            if highlight:
                yield '<tr style="background-color:#F4F4F4">'
            else:
                yield '<tr>'
            highlight = not highlight
            for key, value in record.items():
                yield '<td>' + str(value) + '<td>\n'
            yield '</tr>'

        yield '</tbody></table>'

        import types
        if isinstance(data, types.GeneratorType):
            yield f'<p>top {limit} rows x {len(record.items())} columns</p>'
            yield 'NOTE: the displayed records have been spent'
        if isinstance(data, list):
            yield f'<p>{len(data)} rows x {len(record.items())} columns</p>'

    return ''.join(_to_html_table(dictset, limit))


def ascii_table(
        dictset: Iterator[dict],
        limit: int = 5):
    """
    Render the dictset as a ASCII table.

    NOTE: This exhausts generators so is only recommended to be used on lists.

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
        bars.append('─' * (width + 2))

    # print headers
    result.append('┌' + '┬'.join(bars) + '┐')
    result.append('│' + '│'.join([k.center(v + 2) for k, v in columns.items()]) + '│')
    result.append('├' + '┼'.join(bars) + '┤')

    # print values
    for row in cache:
        result.append('│' + '│'.join([str(v).center(columns[k] + 2) for k, v in row.items()]) + '│')

    # print footer
    result.append('└' + '┴'.join(bars) + '┘')

    return '\n'.join(result)
