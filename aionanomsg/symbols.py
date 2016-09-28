"""
Export nanomsg symbols.
"""

from ._nanomsg import get_symbol_info


class NNSymbol(int):
    """ Acts like an integer value for compatibility but has
    extra props used for internal safety. """

    def __new__(cls, name, info):
        instance = super().__new__(cls, info['value'])
        instance.name = name
        instance.type = info['type']
        instance.ns = info['ns']
        instance.unit = info['unit']
        return instance

    def __str__(self):
        return '<%s %s=%d, type=%d, ns=%d, unit=%d>' % (type(self).__name__,
            self.name, self.__int__(), self.type, self.ns, self.unit)

_symbols = get_symbol_info()

locals().update(dict((name, NNSymbol(name, info))
                for name, info in _symbols.items()))
