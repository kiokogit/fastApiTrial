class LinkedinEnrichError(Exception):
    def __init__(self, message, payload=None, details=None):
        super().__init__()
        self.message = message
        self.payload = payload
        self.details = details

    def to_dict(self):
        return {
            'message': self.message,
            'payload': self.payload,
            'details': self.details,
        }


class NoMatchesException(Exception):
    def __init__(self, *args: object, details=None) -> None:
        super().__init__(*args)
        self.details = details

    def to_dict(self):
        return {
            'message': self.args[0],
            'details': self.details,
        }
