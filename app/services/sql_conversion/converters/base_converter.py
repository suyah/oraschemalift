class BaseConverter:
    """
    A base class for all converters to ensure a consistent interface.
    """
    def __init__(self, source_dialect: str, target_dialect: str):
        self.source_dialect = source_dialect
        self.target_dialect = target_dialect

    def convert_statement(self, statement: str) -> tuple[str, list[dict]]:
        """
        The main conversion method that each converter must implement.

        Args:
            statement (str): A single SQL statement to convert.

        Returns:
            A tuple containing the converted SQL string(s) and a list of logs.
        """
        raise NotImplementedError("Each converter must implement its own convert_statement method.") 