from typing import Union

from radb import ast, parse
from sqlparse.sql import Statement, TokenList, Identifier, IdentifierList, Where


def translate(statement: Statement) -> ast.Project:
    """
    translates an sql statement into a relational algebra
    :param statement: sql statement
    :type statement: Statement
    :return: the parsed statement
    :rtype: ast.Project
    """

    # initialize the result object
    result = None

    # initialize flags to know in which part of the statement we are right now
    projections = None
    conditions = None
    renames = []
    tables = []

    for (index, token) in enumerate(statement.tokens):
        if token.is_keyword and token.normalized == 'SELECT':
            projections = get_projections(statement.tokens, index + 1)
        elif token.is_keyword and token.normalized == 'FROM':
            renames = get_tables_and_names(statement.tokens, index + 1)
        elif type(token) == Where:
            conditions = get_conditions(token)

    if conditions is not None:
        conditions = f'\select_{{{conditions}}}'

    if projections is not None:
        projections = f'\project_{{{projections}}}'

    if renames:
        # iterate through tables and parse table names and aliases.
        for (key, value) in renames:
            if key == value:
                tables.append(value)

            else:
                tables.append(f'\\rename_{{{value}: *}}({key})')

        # join the found tables using \cross keyword
        tables = str.join(' \cross ', tables)

    result = f'{projections if projections is not None else ""}({conditions if conditions is not None else ""}({tables}));'

    return parse.one_statement_from_string(result)


def get_projections(tokens: TokenList, index: int) -> Union[None, str]:
    """
    returns the list of projections
    :param tokens: sql statement tockens
    :type tokens: TokenList
    :param index: index of the last encountered keyword
    :type index: int
    :return: list of projections to make
    :rtype: str
    """

    for token in tokens[index:]:
        if type(token) in [Identifier, IdentifierList]:
            return token.value

        # if you reach a keyword return
        elif token.normalized == 'FROM':
            return None


def get_tables_and_names(tokens: TokenList, index: int) -> list:
    """
    returns the list of tables and there respective aliases
    :param tokens: sql statement tokens
    :type tokens: TokenList
    :param index: token index
    :type index: int
    :return: list of the form [ ('table name', 'alias'), ..., ]
    :rtype: list
    """

    # initialize the result object
    result = []

    # check the tokens from the last keyword
    for token in tokens[index:]:

        # if you reach a new keyword, stop searching
        if token.is_keyword:
            break

        # if you find an identifier
        if type(token) in [Identifier, IdentifierList]:
            identifiers = token.value.split(',')

            for identifier in identifiers:
                name_alias = identifier.strip().split(' ')

                # if the name does not have an alias
                if len(name_alias) == 1:
                    result.append((name_alias[0], name_alias[0]))
                else:
                    result.append((name_alias[0], name_alias[1]))

    return result


def get_conditions(statement: Where) -> str:
    """
    return the conditions in the statement
    :param statement: part of the sql statement
    :type statement: Where
    :return: conditions string
    :rtype: str
    """

    return statement.value.split('where ')[1]
