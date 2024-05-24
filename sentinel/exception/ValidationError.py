class ValidationError(Exception):
    """Exceção levantada para erros na validação."""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
