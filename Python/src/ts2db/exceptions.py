class TableError(Exception):
    """
    A table level error.
    """
    pass

class NameError(Exception):
    """
    Column name error.
    """
    pass

class TypeError(Exception):
    """
    Column type error.
    """
    pass
