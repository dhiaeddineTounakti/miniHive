import collections
import json
from enum import Enum
from typing import Tuple

import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
import radb
import radb.ast
import radb.parse
from luigi.mock import MockTarget
from radb.ast import ValExprBinaryOp, AttrRef, RANumber, RAString
from radb.parse import RAParser as sym

'''
Control where the input data comes from, and where output data should go.
'''


class ExecEnv(Enum):
    LOCAL = 1  # read/write local files
    HDFS = 2  # read/write HDFS
    MOCK = 3  # read/write mock data to an in-memory file system.


'''
Switches between different execution environments and file systems.
'''


class OutputMixin(luigi.Task):
    exec_environment = luigi.EnumParameter(enum=ExecEnv, default=ExecEnv.HDFS)

    def get_output(self, fn):
        if self.exec_environment == ExecEnv.HDFS:
            return luigi.contrib.hdfs.HdfsTarget(fn)
        elif self.exec_environment == ExecEnv.MOCK:
            return MockTarget(fn)
        else:
            return luigi.LocalTarget(fn)


class InputData(OutputMixin):
    filename = luigi.Parameter()

    def output(self):
        return self.get_output(self.filename)


'''
Counts the number of steps / luigi tasks that we need for evaluating this query.
'''


def count_steps(raquery):
    assert (isinstance(raquery, radb.ast.Node))

    if (isinstance(raquery, radb.ast.Select) or isinstance(raquery, radb.ast.Project) or
            isinstance(raquery, radb.ast.Rename)):
        return 1 + count_steps(raquery.inputs[0])

    elif isinstance(raquery, radb.ast.Join):
        return 1 + count_steps(raquery.inputs[0]) + count_steps(raquery.inputs[1])

    elif isinstance(raquery, radb.ast.RelRef):
        return 1

    else:
        raise Exception("count_steps: Cannot handle operator " + str(type(raquery)) + ".")


class RelAlgQueryTask(luigi.contrib.hadoop.JobTask, OutputMixin):
    '''
    Each physical operator knows its (partial) query string.
    As a string, the value of this parameter can be searialized
    and shipped to the data node in the Hadoop cluster.
    '''
    querystring = luigi.Parameter()

    '''
    Each physical operator within a query has its own step-id.
    This is used to rename the temporary files for exhanging
    data between chained MapReduce jobs.
    '''
    step = luigi.IntParameter(default=1)

    '''
    Parameters to use for optimization
    '''
    after_query = luigi.Parameter(default=";")
    optimize = luigi.BoolParameter(default=False)

    '''
    In HDFS, we call the folders for temporary data tmp1, tmp2, ...
    In the local or mock file system, we call the files tmp1.tmp...
    '''

    def output(self):
        if self.exec_environment == ExecEnv.HDFS:
            filename = "tmp" + str(self.step)
        else:
            filename = "tmp" + str(self.step) + ".tmp"
        return self.get_output(filename)


'''
Given the radb-string representation of a relational algebra query,
this produces a tree of luigi tasks with the physical query operators.
'''


def task_factory(raquery, after_query: radb.ast.Node = None, step=1, env=ExecEnv.HDFS, optimize=False,
                 allow_mappers_only=False):
    """
    returns the right task class given a query.
    :param raquery: query
    :param after_query: query to execute in the reducer
    :param step: step count
    :param env: environment where to execute the map reduce tasks
    :param optimize: flag to set optimization on and off
    :param allow_mappers_only: if optimization is on then this controls if we allow returning mapper_only tasks or not
    :return: the given class
    """
    assert (isinstance(raquery, radb.ast.Node))

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step,
                          exec_environment=env,
                          optimize=optimize) if not optimize or allow_mappers_only else task_factory(raquery.inputs[0],
                                                                                                     step=step, env=env,
                                                                                                     optimize=optimize,
                                                                                                     after_query=after_query)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize,
                        after_query=str(after_query) + ";")

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env, optimize=optimize,
                           after_query=str(after_query) + ";")

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step,
                          exec_environment=env,
                          optimize=optimize) if not optimize or allow_mappers_only else task_factory(
            raquery=raquery.inputs[0], env=env, optimize=optimize, after_query=after_query)

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment, optimize=self.optimize,
                             after_query=raquery.inputs[0])
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment, optimize=self.optimize, after_query=raquery.inputs[1])

        self.prev_task1 = type(task1)
        self.prev_task2 = type(task2)
        self.branche1_relations = None
        self.branche2_relations = None
        self.parsed_lines = set()

        return [task1, task2]

    def mapper(self, line):
        relations, tuple = line.split('\t')
        parsed_tuple = json.loads(tuple)
        list_of_relations_tuples = [(relations, parsed_tuple)]
        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond

        # If optimize flag is set then consider the case were Join is the first node after Input Data
        if self.optimize and (self.prev_task1 == InputData or self.prev_task2 == InputData):
            # Delete the other entries
            list_of_relations_tuples = []
            # We need to guess which branch this line is coming from
            relations_from_line = sorted(relations.split(','))
            if self.branche1_relations is None:
                self.branche1_relations = sorted(get_branche_relations(raquery=raquery.inputs[0],
                                                                       get_renames=self.prev_task1 != InputData))

            if self.branche2_relations is None:
                self.branche2_relations = sorted(get_branche_relations(raquery=raquery.inputs[1],
                                                                       get_renames=self.prev_task2 != InputData))

            # If both branches have same relations then process the input on both branches
            if self.branche1_relations == self.branche2_relations and self.prev_task1 == InputData and self.prev_task2 == InputData and line not in self.parsed_lines:
                self.parsed_lines.add(line)
                relations, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple,
                                                           raquery=raquery.inputs[0])
                list_of_relations_tuples.append((relations, parsed_tuple))
                relations, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple,
                                                           raquery=raquery.inputs[1])
                list_of_relations_tuples.append((relations, parsed_tuple))

            elif self.branche1_relations != self.branche2_relations:
                # If the previous task is InputData and the branch relations are the same as the relations from the line
                # Execute the branch operation on this line
                if self.prev_task1 == InputData and relations_from_line == self.branche1_relations:
                    relations, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple,
                                                               raquery=raquery.inputs[0])

                elif self.prev_task2 == InputData and relations_from_line == self.branche2_relations:
                    relations, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple,
                                                               raquery=raquery.inputs[1])

                list_of_relations_tuples.append((relations, parsed_tuple))

        ''' ...................... fill in your code below ........................'''
        for relations, parsed_tuple in list_of_relations_tuples:
            # If after chain folding there is still input data do the join mapper operation
            if relations is not None and parsed_tuple is not None:
                join_attrs = get_join_attr(condition)
                found_values = []

                for join_attr in join_attrs:
                    attributes_to_look_for = []
                    if '.' in join_attr:
                        # If the specific form relations.attribute is already there then take the attribute.
                        attributes_to_look_for += [join_attr]

                    else:
                        # Generate all possible combinations of relation.attribute for later filtering
                        attributes_to_look_for += [relation + "." + join_attr.name for relation in relations.split(",")]

                    # Filter the attributes to leave only the significant ones.
                    for attr in attributes_to_look_for:
                        if attr in parsed_tuple:
                            found_values += [parsed_tuple[attr]]

                # Translate the values of the attributes to string
                found_values = [str(val) for val in found_values]

                # Sort the values to avoid arbitrary order problems, you might sometimes get "val1, val2" and some other times
                # you get "val2, val1" which are two different keys
                sorted(found_values)

                if found_values is not None:
                    yield ",".join(found_values), json.dumps([relations, parsed_tuple])

        ''' ...................... fill in your code above ........................'''

    def reducer(self, key, values):
        ''' ...................... fill in your code below ........................'''
        relations_to_tuples = collections.defaultdict(list)
        for value in values:
            rel, tuple = json.loads(value)
            # Append the tuple to the corresponding relation entry.
            # We use sets here to filter duplicates.
            relations_to_tuples[rel].append(tuple)

        combined_key = []
        lists = []
        for key, value in relations_to_tuples.items():
            combined_key.append(key)
            lists.append(value)

        relations = ",".join(sorted(combined_key))

        if len(lists) > 1:
            for tuples in lists[0]:
                for tuples1 in lists[1]:
                    resulting_tuple = {**tuples, **tuples1}

                    if self.optimize:
                        # Get the query
                        after_query = radb.parse.one_statement_from_string(self.after_query)
                        # Run the chained mappers
                        relations, resulting_tuple = parse_query_tree(relations=relations, tuple=resulting_tuple,
                                                                      raquery=after_query)

                    if relations is not None and resulting_tuple is not None:
                        yield relations, json.dumps(resulting_tuple)

        ''' ...................... fill in your code above ........................'''


def get_branche_relations(raquery: radb.ast.Node, get_renames: bool):
    """
    Go through a branche and return relation names in that branche or the renames.
    :param raquery: branche to process
    :param get_renames: flag to indicate if we want to get renames or not
    :return:
    """
    relations = []
    # If you encounter a relation name return it
    if isinstance(raquery, radb.ast.RelRef):
        return [raquery.rel]
    # If we want renames then return the first rename that encounters you
    elif isinstance(raquery, radb.ast.Rename) and get_renames:
        return [raquery.relname]
    # Otherwise process branches
    for input in raquery.inputs:
        relations += get_branche_relations(raquery=input, get_renames=get_renames)

    return relations


def check_all_keys(keys, key_to_look_for):
    for each_key in keys:
        if each_key.find(key_to_look_for) != -1:
            return True

    return False


def get_join_attr(cond):
    """
    get the attributes of a condition.
    :param cond:
    :return:
    """
    tmp_res = set()
    if type(cond) == AttrRef:
        if cond.rel is not None:
            return {cond.rel + "." + cond.name}
        else:
            return {cond.name}

    for inp in cond.inputs:
        tmp_res.update(get_join_attr(inp))

    return tmp_res


class SelectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Select))
        prev_task = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment,
                                 optimize=self.optimize)
        return [prev_task]

    def mapper(self, line):
        ''' ...................... fill in your code below ........................'''
        raquery = radb.parse.one_statement_from_string(self.querystring)
        relations, tuple = line.split('\t')
        parsed_tuple = json.loads(tuple)
        if self.optimize:
            relations, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple, raquery=raquery)
        relations, parsed_tuple = select(relations=relations, tuple=parsed_tuple, raquery=raquery)

        # You should test if the select have returned something or not.
        if relations is not None and parsed_tuple is not None:
            yield relations, json.dumps(parsed_tuple)
        ''' ...................... fill in your code above ........................'''


def select(relations: str, tuple: dict, raquery: radb.ast.Select) -> Tuple[str, dict]:
    """
    Execute select operation on a line
    :param relations: the relations available
    :param tuple: the tuple
    :param raquery: the query to execute on the line
    :return: the processed line
    """
    assert isinstance(raquery, radb.ast.Select), f'Something went wrong expecting radb.ast.Select got {type(raquery)}'
    condition = raquery.cond
    res_relations, res_tuple = None, None

    if relations is not None and tuple is not None:
        for relation in relations.split(","):
            if test_condition(relation, tuple, condition):
                res_relations, res_tuple = relations, tuple

    return res_relations, res_tuple


def test_condition(relation, parsed_tuple, condition):
    """
    return the result of the condition execution.
    :return:
    """
    operator = condition.op
    value1 = condition.inputs[0]
    value2 = condition.inputs[1]

    if type(value1) == ValExprBinaryOp:
        value1 = test_condition(relation, parsed_tuple, value1)

    if type(value2) == ValExprBinaryOp:
        value2 = test_condition(relation, parsed_tuple, value2)

    value1 = convert(value1, parsed_tuple, relation)
    value2 = convert(value2, parsed_tuple, relation)

    if value1 is None or value2 is None:
        return False

    if operator == sym.EQ:
        return value1 == value2
    elif operator == sym.AND:
        return value1 and value2
    elif operator == sym.OR:
        return value1 or value2
    elif operator == sym.LQ:
        return value1 <= value2
    elif operator == sym.GE:
        return value1 >= value2
    elif operator == sym.GT:
        return value1 > value2
    elif operator == sym.LT:
        return value1 < value2
    elif operator == sym.DIFF:
        return value1 != value2


def convert(value: radb.ast.ValExpr, parsed_tuple: dict, relation: str):
    """
    convert value from radb classes to python classes
    :param parsed_tuple:
    :param relation:
    :param value:
    :return:
    """
    if isinstance(value, RANumber):
        return int(value.val)
    elif isinstance(value, RAString):
        return value.val.replace("'", "")
    elif isinstance(value, AttrRef):
        key = relation + "." + value.name
        if key in parsed_tuple:
            return parsed_tuple[key]
        else:
            return None
    else:
        return value


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))
        prev_task = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment,
                                 optimize=self.optimize)

        return [prev_task]

    def mapper(self, line):
        raquery = radb.parse.one_statement_from_string(self.querystring)

        ''' ...................... fill in your code below ........................'''
        # Split line into a tuple of relations, attributes
        relations, tuple = line.split('\t')
        # Get dict from the line attributes
        parsed_tuple = json.loads(tuple)
        if self.optimize:
            realtions, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple, raquery=raquery)

        relations, parsed_tuple = rename(relations=relations, tuple=parsed_tuple, raquery=raquery)

        yield relations, json.dumps(parsed_tuple)

        ''' ...................... fill in your code above ........................'''


def rename(relations: str, tuple: dict, raquery: radb.ast.Rename) -> tuple:
    """
    Does the rename job needed on each line and return the processed line.
    :param relations: the relation to run the select on.
    :param tuple:
    :param raquery: the query string
    :return: processed line
    """
    assert isinstance(raquery, radb.ast.Rename), f'Something went wrong expecting radb.ast.Rename got {type(raquery)}'

    if relations is not None and tuple is not None:
        tmp_dict = {}
        for key, value in tuple.items():
            for relation in relations.split(','):
                tmp_key = key.replace(relation, raquery.relname)
                tmp_dict[tmp_key] = value

        relations, tuple = raquery.relname, tmp_dict

    return relations, tuple


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))
        prev_task = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment,
                                 optimize=self.optimize,
                                 after_query=raquery.inputs[0])
        self.prev_task = type(prev_task)

        return [prev_task]

    def mapper(self, line):
        relations, tuple = line.split('\t')
        # Convert the json string to a dict
        parsed_tuple = json.loads(tuple)
        raquery = radb.parse.one_statement_from_string(self.querystring)

        if self.optimize and self.prev_task == InputData:
            relations, parsed_tuple = parse_query_tree(relations=relations, tuple=parsed_tuple,
                                                       raquery=raquery.inputs[0])

        if relations is not None and parsed_tuple is not None:
            tmp_dict = {}

            # Get the attributes for the projection
            attrs = raquery.attrs

            ''' ...................... fill in your code below ........................'''
            for attr in attrs:
                if attr.rel is not None:
                    keys = [attr.rel + "." + attr.name]

                else:
                    keys = [relation + "." + attr.name for relation in relations.split(",")]

                for key in keys:
                    if key in parsed_tuple:
                        tmp_dict[key] = parsed_tuple[key]

            if len(tmp_dict.keys()) >= 0:
                yield json.dumps(tmp_dict), relations

        ''' ...................... fill in your code above ........................'''

    def combiner(self, key, values):
        # Execute this to filter duplicates in node level in order to minimize communication costs.
        tmp_value = None
        # Go through all duplicates
        for value in values:
            tmp_value = value

        yield key, tmp_value

    def reducer(self, key, values):
        ''' ...................... fill in your code below ........................'''
        after_query = radb.parse.one_statement_from_string(self.querystring)
        tmp_value = None
        # Go through all duplicates
        for value in values:
            tmp_value = value

        relations = tmp_value
        tuple = json.loads(key)

        # Execute the after_query on the temporary value.
        if self.optimize:
            relations, tuple = parse_query_tree(raquery=after_query, relations=relations, tuple=tuple)

        if relations is not None and tuple is not None:
            yield relations, json.dumps(tuple)
        ''' ...................... fill in your code above ........................'''


def parse_query_tree(relations: str, tuple: dict, raquery: radb.ast.Node) -> tuple:
    """
    Parse the combined operations before the current Map-Reduce job
    :param raquery: query to execute.
    :param relations: relations on which to execute the query
    :param tuple: the tuple on which to execute the query
    :return: pairs of relations and
    """
    assert isinstance(tuple, dict), f'expecting dict got {type(tuple)}'
    # If the previous operation is a map reduce job or the first job then return the line as it is.
    if isinstance(raquery, radb.ast.Project) or isinstance(raquery, radb.ast.RelRef) or isinstance(raquery,
                                                                                                   radb.ast.Join):
        return relations, tuple

    # If it is select or rename run the select method and return the result without writing them to a file.
    if isinstance(raquery, radb.ast.Select):
        relations, tuple = parse_query_tree(relations=relations, tuple=tuple, raquery=raquery.inputs[0])
        return select(relations=relations, tuple=tuple, raquery=raquery)

    if isinstance(raquery, radb.ast.Rename):
        relations, tuple = parse_query_tree(relations=relations, tuple=tuple, raquery=raquery.inputs[0])
        return rename(relations=relations, tuple=tuple, raquery=raquery)


if __name__ == '__main__':
    luigi.run()
