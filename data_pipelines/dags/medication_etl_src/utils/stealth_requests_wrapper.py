from stealth_requests import StealthSession

class StealthSessionWrapper(StealthSession):
    def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
