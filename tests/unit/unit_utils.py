try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

class StringIOContextManager(StringIO):
    """
    A StringIO that can be used as a context manager
    """
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass