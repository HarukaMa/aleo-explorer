from aleo_types import Value


class Registers:

    def __init__(self):
        self._registers: dict[int, Value] = {}

    def __getitem__(self, index: int):
        if index not in self._registers:
            raise IndexError(index)
        return self._registers[index]

    def __setitem__(self, index: int, value: Value):
        self._registers[index] = value

    def dump(self):
        for i, r in self._registers.items():
            print(f"r{i} = {r}")
