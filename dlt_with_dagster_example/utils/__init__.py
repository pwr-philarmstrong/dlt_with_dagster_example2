import re
import tomli
import logging

def to_snake_case(name):
    # Add an underscore before each capital letter followed by a lowercase letter or end of string
    name = re.sub(r'(?<!^)(?<!_)(?=[A-Z][a-z])', '_', name)
    # Add an underscore before each capital letter followed by another capital letter and a lowercase letter
    name = re.sub(r'(?<=[a-z])(?=[A-Z])', '_', name)
    # Convert the entire string to lowercase
    name = name.lower()
    return name

def load_secrets(secrets_file):
    """Load secrets from a TOML file."""
    try:
        with open(secrets_file, 'rb') as f:
            return tomli.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Secrets file not found at {secrets_file}")