

class DagHook(object):
    """
    Description:
        hook类，某些平台可能在execute前后需要做一些操作，这里提供pre和post方法。
    Attributes:
        /
    """
    def pre_handler(self, platformArgs):
        pass

    def post_handler(self, platformArgs):
        pass
