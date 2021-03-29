import datetime


class BlobPaths(object):

    @staticmethod
    def split_filename(filename: str):
        """ see test cases for all handled edge cases """
        if not filename:
            return '', ''

        ext = ''
        name = ''
        parts = filename.split('.')
        if len(parts) == 1:
            return filename, ''
        if parts[0] == '':
            parts.pop(0)
            parts[0] = '.' + parts[0]
        if len(parts) > 1:
            ext = '.' + parts.pop()
        if ext.find('/') > 0:
            ext = ext.lstrip('.')
            parts.append(ext)
            ext = ''
        name = '.'.join(parts)
        if ext == '.':
            name = ''
        return name, ext

    @staticmethod
    def get_parts(path_string: str):
        if not path_string:
            raise ValueError('get_parts: path_string must have a value')

        parts = str(path_string).split('/')
        bucket = parts.pop(0)
        name, ext = BlobPaths.split_filename(parts.pop())
        path = '/'.join(parts) + '/'
        return bucket, path, name, ext

    @staticmethod
    def build_path(path: str, date: datetime.date = None):

        if not date:
            date = datetime.datetime.now()

        if not path:
            raise ValueError('build_path: path must have a value')
        if not path[0] == '/':
            path_string = path.lstrip('/')
        else:
            path_string = path

        path_string = path_string.replace('%date', '%Y-%m-%d')
        path_string = path_string.replace('%time', '%H%M%S')

        path_string = date.strftime(path_string)

        return path_string
