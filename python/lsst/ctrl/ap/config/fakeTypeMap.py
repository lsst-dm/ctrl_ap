class FakeTypeMap(dict):
    def __init__(self, configClass):
        self.configClass = configClass

    def __getitem__(self, k):
        return self.setdefault(k, self.configClass)
