from enum import Enum
import json
import luigi
import luigi.contrib.hadoop
import luigi.contrib.hdfs
from luigi.mock import MockTarget
import radb
import radb.ast
from radb.ast import RelExpr, ValExprBinaryOp, Select, Rename, AttrRef, RelRef, Cross, Join, RANumber, RAString
import radb.parse
from radb.parse import RAParser as sym
from radb import RAParser

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


def task_factory(raquery, step=1, env=ExecEnv.HDFS):
    assert (isinstance(raquery, radb.ast.Node))

    if isinstance(raquery, radb.ast.Select):
        return SelectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.RelRef):
        filename = raquery.rel + ".json"
        return InputData(filename=filename, exec_environment=env)

    elif isinstance(raquery, radb.ast.Join):
        return JoinTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Project):
        return ProjectTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    elif isinstance(raquery, radb.ast.Rename):
        return RenameTask(querystring=str(raquery) + ";", step=step, exec_environment=env)

    else:
        # We will not evaluate the Cross product on Hadoop, too expensive.
        raise Exception("Operator " + str(type(raquery)) + " not implemented (yet).")


class JoinTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Join))

        task1 = task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)
        task2 = task_factory(raquery.inputs[1], step=self.step + count_steps(raquery.inputs[0]) + 1,
                             env=self.exec_environment)

        return [task1, task2]

    def mapper(self, line):
        relations, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)
        condition = raquery.cond

        ''' ...................... fill in your code below ........................'''
        attrs = get_join_attr(raquery.cond)
        tmp_value = []

        for attr in attrs:
            keys = []
            if '.' in attr:
                keys += [attr]

            else:
                keys += [relation + "." + attr.name for relation in relations.split(",")]

            for key in keys:
                if key in json_tuple:
                    tmp_value += [json_tuple[key]]

        if tmp_value is not None:
            yield (",".join(tmp_value), json.dumps([relations, json_tuple]))

        ''' ...................... fill in your code above ........................'''

    def reducer(self, key, values):
        raquery = radb.parse.one_statement_from_string(self.querystring)

        ''' ...................... fill in your code below ........................'''
        rel_list = {}
        for value in values:
            rel, tuple = json.loads(value)
            if rel in rel_list:
                rel_list[rel] += [tuple]

            elif check_all_keys(rel_list.keys(), rel):
                pass

            else:
                rel_list[rel] = [tuple]

        combined_key = []
        lists = []
        for key, value in rel_list.items():
            combined_key.append(key)
            lists.append(value)

        combined_key = ",".join(sorted(combined_key))

        if len(lists) > 1:
            for tuples in lists[0]:
                for tuples1 in lists[1]:
                    resulting_tuple = {**tuples, **tuples1}
                    yield (combined_key, json.dumps(resulting_tuple))

        ''' ...................... fill in your code above ........................'''

def check_all_keys(keys, key_to_look_for):
    for each_key in keys:
        if each_key.find(key_to_look_for)!=-1:
            return True

    return False

def get_join_attr(cond):
    """
    get the attributes of a condtion.
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

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relations, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        condition = radb.parse.one_statement_from_string(self.querystring).cond

        ''' ...................... fill in your code below ........................'''
        for relation in relations.split(","):
            if test_condition(relation, json_tuple, condition):
                yield (relations, json.dumps(json_tuple))

        ''' ...................... fill in your code above ........................'''


def test_condition(relation, json_tuple, condition):
    """
    return the result of the condition execution.
    :return:
    """
    operator = condition.op
    value1 = condition.inputs[0]
    value2 = condition.inputs[1]

    if type(value1) == ValExprBinaryOp:
        value1 = test_condition(relation, json_tuple, value1)

    if type(value2) == ValExprBinaryOp:
        value2 = test_condition(relation, json_tuple, value2)

    value1 = convert(value1, json_tuple, relation)
    value2 = convert(value2, json_tuple, relation)

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


def convert(value, json_tuple, relation):
    """
    convert value from radb classes to python classes
    :param value:
    :return:
    """
    if type(value) == RANumber:
        return int(value.val)
    elif type(value) == RAString:
        return value.val.replace("'", "")
    elif type(value) == AttrRef:
        key = relation + "." + value.name
        if key in json_tuple:
            return json_tuple[key]
        else:
            return None
    else:
        return value


class RenameTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Rename))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relation, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        raquery = radb.parse.one_statement_from_string(self.querystring)

        ''' ...................... fill in your code below ........................'''

        tmp_dict = {}
        for key, value in json_tuple.items():
            tmp_key = key.replace(relation, raquery.relname)
            tmp_dict[tmp_key] = value

        yield (raquery.relname, json.dumps(tmp_dict))

        ''' ...................... fill in your code above ........................'''


class ProjectTask(RelAlgQueryTask):

    def requires(self):
        raquery = radb.parse.one_statement_from_string(self.querystring)
        assert (isinstance(raquery, radb.ast.Project))

        return [task_factory(raquery.inputs[0], step=self.step + 1, env=self.exec_environment)]

    def mapper(self, line):
        relations, tuple = line.split('\t')
        json_tuple = json.loads(tuple)

        attrs = radb.parse.one_statement_from_string(self.querystring).attrs

        ''' ...................... fill in your code below ........................'''
        tmp_dict = {}
        keys = []
        for attr in attrs:
            if attr.rel is not None:
                keys = [attr.rel + "." + attr.name]

            else:
                keys = [relation + "." + attr.name for relation in relations.split(",")]

            for key in keys:
                if key in json_tuple:
                    tmp_dict[key] = json_tuple[key]

        if len(tmp_dict.keys()) >= 0:
            yield (json.dumps(tmp_dict), relations)

        ''' ...................... fill in your code above ........................'''

    def reducer(self, key, values):
        ''' ...................... fill in your code below ........................'''

        tmp_value = None
        for value in values:
            tmp_value = value
            continue

        if tmp_value is not None:
            yield (tmp_value, key)
        ''' ...................... fill in your code above ........................'''


if __name__ == '__main__':
    luigi.run()
