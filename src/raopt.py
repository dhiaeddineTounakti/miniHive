from collections import deque
from typing import Union

import radb.parse
from radb.ast import RelExpr, ValExprBinaryOp, Select, Rename, AttrRef, RelRef
import queue


def rule_break_up_selections(ra: RelExpr) -> RelExpr:
    '''
    split the complex selection into simple one.
    :param ra: Relation expression
    :type ra: RelExpr
    :return: Relation expression with simple select
    :rtype: RelExpr
    '''

    # Initiate the queue
    Q = queue.Queue()
    # Push the first object
    Q.put_nowait(ra)

    while not Q.empty():
        current_element = Q.get_nowait()
        # Pop the element

        if type(current_element) == Select and not any(
                [type(expr_input) != ValExprBinaryOp for expr_input in current_element.cond.inputs]):
            # Store previous inputs and conditions
            previous_input = current_element.inputs[0]
            previous_conditions = current_element.cond

            # Create new select statement using the first condition
            new_select = Select(input=previous_input, cond=previous_conditions.inputs[1])

            # Replace the current_element input with the newly created select and conditions with the second condition
            current_element.inputs = [new_select]
            current_element.cond = previous_conditions.inputs[0]

            # Append the current element again
            Q.put_nowait(current_element)

        elif len(current_element.inputs) > 0:
            # if the expression has more than one input inspect them.
            for elem in current_element.inputs:
                Q.put_nowait(elem)

    return ra


def rule_push_down_selections(ra: RelExpr, dd: dict) -> RelExpr:
    """
    Push each selection condition as far as possible
    :param ra: relational expression
    :type ra: RelExpr
    :param dd: relations schema
    :type dd: dict
    :return: relation expression with selections pushed down as far as possible
    :rtype: RelExpr
    """
    rename_dict = find_rename(ra)
    map_relation_to_select = find_selection_for_each_relation(ra, dd, rename_dict)

    # Initiate stack for Depth first traversal
    DFS(ra, map_relation_to_select, [])


def DFS(ra: Union[RelExpr, RelRef], map_relation_to_select: dict, list_of_relations: list = None):
    if len(ra.inputs) == 0:
        if type(ra) == RelRef:
            list_of_relations.append(ra.rel)

    for element in ra.inputs:
        DFS(element, map_relation_to_select)



def find_selection_for_each_relation(ra: RelExpr, dd: dict, rename_dict: dict) -> dict:
    """
    Find for relation groups the corresponding slections
    :param ra: relational expression
    :param dd: relation schema
    :param rename_dict: relations aliases.
    :return: dictionary [(relation_a, relation_b): [cond_1, cond_2 ...]
    """
    result = {}
    # Initiate the queue
    Q = queue.Queue()
    Q.put_nowait(ra)

    while not Q.empty():
        current_element = Q.get_nowait()

        if type(current_element) == Select:
            # Get related relations to this selection
            related_relations = get_related_relations(current_element.cond, dd, rename_dict)
            result[related_relations] = current_element.cond

    return result


def get_related_relations(ra: RelExpr, dd: dict, rename_dict: dict) -> tuple:
    """
    Look for the related relations to the current selection
    :param ra: condition expression
    :param dd: relation schema
    :param rename_dict: name and aliases of existing relations
    :return: tuple of the relations related to this condition.
    """
    # List to store intermediate relations
    tmp_res = []
    # Initiate the queue
    Q = queue.Queue()
    Q.put_nowait(ra)

    while not Q.empty():
        current_element = Q.get_nowait()

        if type(current_element) == AttrRef:
            if current_element.rel is not None:
                tmp_res.append(current_element.rel)

            else:
                # Look for the relations that have the attribute and exists in the expression.
                for key, value in dd.items():
                    if current_element.name in value and key in dd.keys:
                        tmp_res.append(key)

        if len(current_element.inputs) > 0:
            for elem in current_element.inputs:
                Q.put_nowait(elem)

    return tuple(tmp_res)


def find_rename(ra: RelExpr) -> dict:
    """
    Create a dict mapping each table to its alias
    :param ra: relational expression
    :return:
    """
    result = {}
    # Initiate the queue
    Q = queue.Queue()
    Q.put_nowait(ra)

    while not Q.empty():
        current_element = Q.get_nowait()

        if type(current_element) == Rename:
            result[current_element.inputs[0].rel] = current_element.relname

        if len(current_element.inputs) > 0:
            for elem in current_element.inputs:
                Q.put_nowait(elem)

    return result
