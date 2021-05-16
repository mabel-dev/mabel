class Mock:
    def __init__(
        self,
        response_dict: dict = {},
        attributes: list = [],
    ):
        """
        Generic Mock Class

        Methods and attributes are responded to, they are all assumed to be
        methods. If a method requires a specific result, it can be set using
        the response_dict.

        Paramters:
            response_dict: dictionary (optional)
                Fixed responses to give to method and attributes
            attributes: list (optional)
                List of attributes to return values for, items not on this list
                will return a method
        """
        self.response_dict = response_dict
        self.attributes = attributes

    def __getattr__(self, attr):
        print(f"\nMocked Object Method Called: {attr}")

        if attr in self.attributes:
            return self.response_dict.get(attr, attr)

        def wrapper(*args, **kwargs):
            if len(args) > 0:
                print("arguments:", ", ".join([repr(a) for a in args]))
            if len(kwargs) > 0:
                print(
                    "keyword arguments:",
                    ", ".join([f"{k}={v!r}" for k, v in kwargs.items()]),
                )

            return self.response_dict.get(attr, attr)

        return wrapper
