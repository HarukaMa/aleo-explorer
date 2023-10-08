from aleo_types import Value


class Registers:

    def __init__(self):
        self._registers: list[Value] = []

    def __getitem__(self, index: int):
        if index >= len(self._registers):
            raise IndexError("register doesn't exist")
        return self._registers[index]

    def __setitem__(self, index: int, value: Value):
        if index > len(self._registers):
            raise IndexError("register not used in order")
        if index == len(self._registers):
            self._registers.append(value)
        else:
            self._registers[index] = value

    def dump(self):
        for i, r in enumerate(self._registers):
            print(f"r{i} = {r}")
