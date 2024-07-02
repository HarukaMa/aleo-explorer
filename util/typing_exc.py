
class Unreachable(Exception):
    def __init__(self, msg: str = 'Unreachable code reached'):
        self.msg = msg
        super().__init__(self.msg)