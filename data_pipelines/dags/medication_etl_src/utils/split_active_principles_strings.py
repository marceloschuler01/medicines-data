def split_active_principles_strings(active_principles_string: str, sep: str = ",") -> list[str]:
    """
    Splits a string of active principles into a list of individual active principles.

    Parameters:
    active_principles_string (str): A string containing active principles separated by some character separator.
    sep (str): The character used to separate active principles in the input string. Default is a comma (",").

    Returns:
    list: A list of individual active principles.
    """

    if not active_principles_string:
        return []

    # Split the string by separator and strip whitespace from each element
    active_principles_list = [ap.strip() for ap in active_principles_string.split(sep) if ap.strip()]

    return active_principles_list