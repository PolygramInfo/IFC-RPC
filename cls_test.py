class TestClass:

    def __new__(cls, *args, **kwargs):
        cls.var = "SomeVariable"

        return super().__new__(cls)
    
    @classmethod
    def __setvar__(cls, )