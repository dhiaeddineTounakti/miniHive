import re
from collections import defaultdict
from typing import Union

import radb.parse
from radb.RAParser import RAParser
from radb.ast import RelExpr, ValExprBinaryOp, Select, Rename, AttrRef, RelRef, Cross, Join
import queue


def rule_break_up_selections(ra: RelExpr) -> RelExpr:
    """
    split the complex selection into simple one.
    :param ra: Relation expression
    :type ra: RelExpr
    :return: Relation expression with simple select
    :rtype: RelExpr
    """

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

    ra = delete_selection_from_expression(ra)

    # Recursively traverse the object and insert the selections where they should be inserted.
    ra, visited_relations = dfs(ra, map_relation_to_select)

    # Check if there are still selections not inserted yet.
    for key, conditions in map_relation_to_select.items():
        ra = make_nested_selections(selection_input=ra, conditions=conditions)

    return ra


def rule_merge_selections(ra: RelExpr) -> RelExpr:
    """
    Push each selection condition as far as possible
    :param ra: relational expression
    :type ra: RelExpr
    :return: relation expression with selections pushed down as far as possible
    :rtype: RelExpr
    """
    # Initiate stack
    stack = [ra]

    while len(stack) > 0:
        current_element = stack.pop()
        # Store current element for later use.
        starting_elem = current_element

        if type(current_element) == Select:
            conditions = []

            # Look for nested selections.
            while type(current_element) == Select:
                conditions.append(current_element.cond)
                current_element = current_element.inputs[0]

            # Insert new selection by updating previous one
            new_select = make_combined_select(selection_input=current_element, conditions=conditions)
            starting_elem.cond = new_select.cond
            starting_elem.inputs = new_select.inputs

        for element in starting_elem.inputs:
            stack.append(element)

    return ra


def rule_introduce_joins(ra: RelExpr) -> RelExpr:
    """
    Introduces Join
    :param ra: relational expression
    :type ra: RelExpr
    :return: relation expression with selections pushed down as far as possible
    :rtype: RelExpr
    """
    # Initiate stack with the encapsulated relational expression in order to be able to modify the expression without
    # the need to create a new object.
    ra = RelExpr(inputs=[ra])
    stack = [ra]

    while len(stack) > 0:
        current_element = stack.pop()
        # Check nested structure to be able to modify object without the need to construct a new one.
        if len(current_element.inputs) > 0 and type(current_element.inputs[0]) == Select and type(
                current_element.inputs[0].inputs[0]) == Cross:
            cross_product = current_element.inputs[0].inputs[0]
            current_element.inputs[0] = Join(left=cross_product.inputs[0], right=cross_product.inputs[1],
                                             cond=current_element.inputs[0].cond)

        for element in current_element.inputs:
            stack.append(element)

    # Strip away the encapsulation
    return ra.inputs[0]


def delete_selection_from_expression(ra: RelExpr) -> RelExpr:
    """
    Deleted selection statements from the RelExpr
    :param ra: relational expression
    :return: relational expression
    """
    regular_expr = '\\\\select_{.{0,100}?}'
    rel_expr_string = str(ra)
    rel_expr_string_no_selection = re.sub(regular_expr, '', rel_expr_string) + ';'
    return radb.parse.one_statement_from_string(rel_expr_string_no_selection)


def make_nested_selections(conditions: list, selection_input: Union[RelRef, RelExpr]) -> RelExpr:
    """
    Combine the conditions into one nested selection.
    :param conditions: list of conditions : [ cond_1, cond_2, ...]
    :param selection_input: the input to use for the selection.
    :return: RelExpr
    """

    # Initiate variable to store previous select
    prev_select = None
    for condition in conditions[::-1]:
        if prev_select is not None:
            new_select = Select(input=prev_select, cond=condition)

        else:
            new_select = Select(input=selection_input, cond=condition)

        prev_select = new_select

    return prev_select


def make_combined_select(conditions: list, selection_input: Union[RelRef, RelExpr]) -> Select:
    """
    Combine the conditions into one complex selection.
    :param conditions: list of conditions : [ cond_1, cond_2, ...]
    :param selection_input: the input to use for the selection.
    :return: complex selection statement.
    """
    prev_condition = None
    for condition in conditions:
        if prev_condition is not None:
            new_condition = ValExprBinaryOp(left=prev_condition, right=condition, op=RAParser.AND)

        else:
            new_condition = condition

        prev_condition = new_condition

    return Select(input=selection_input, cond=prev_condition)


def dfs(ra: [], map_relation_to_select: dict):
    """
    Recursively traverse the RelExpr tree structure and update the object.
    :param ra: relational expression or relation reference
    :param map_relation_to_select: dictionary mapping a relation or a group of relations into select object
    :return:
    """
    list_of_relations_available = []
    if len(ra.inputs) == 0:
        if type(ra) == RelRef:
            list_of_relations_available.append(ra.rel)

    for index, element in enumerate(ra.inputs):
        expr, relations = dfs(element, map_relation_to_select)
        ra.inputs[index] = expr
        list_of_relations_available += relations

    if type(ra) == Rename:
        list_of_relations_available.append(ra.relname)

    # After visiting all inputs update the relational expression
    # Variable to store the keys to delete form the map_relation_to_select
    to_delete = []
    for necessary_relations, conditions in map_relation_to_select.items():
        if set(necessary_relations).issubset(set(list_of_relations_available)):
            # Insert selection
            ra = make_nested_selections(selection_input=ra, conditions=conditions)
            # Remember the key to delete afterwards
            to_delete.append(necessary_relations)

    # Delete used selections from the dictionary
    for key in to_delete:
        map_relation_to_select.pop(key)

    return ra, list_of_relations_available


def find_selection_for_each_relation(ra: RelExpr, dd: dict, rename_dict: dict) -> dict:
    """
    Find for relation groups the corresponding selections
    :param ra: relational expression
    :param dd: relation schema
    :param rename_dict: relations aliases.
    :return: dictionary [(relation_a, relation_b): [cond_1, cond_2 ...]
    """
    result = defaultdict(list)
    # Initiate the queue
    Q = queue.Queue()
    Q.put_nowait(ra)

    while not Q.empty():
        current_element = Q.get_nowait()

        if type(current_element) == Select:
            # Get related relations to this selection
            related_relations = get_related_relations(current_element.cond, dd, rename_dict)
            result[related_relations].append(current_element.cond)

        if len(current_element.inputs) > 0:
            for elem in current_element.inputs:
                Q.put_nowait(elem)

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
                    if current_element.name in value and key in rename_dict:
                        tmp_res.append(rename_dict[key])

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

        elif type(current_element) == RelRef and current_element.rel not in result.keys():
            result[current_element.rel] = current_element.rel

        if len(current_element.inputs) > 0:
            for elem in current_element.inputs:
                Q.put_nowait(elem)

    return result
