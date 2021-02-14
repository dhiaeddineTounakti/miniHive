import unittest

import ra2mr
from miniHive import eval, clear_local_tmpfiles
from minihive_test import costcounter


class MyTestCase(unittest.TestCase):
    def test_query_1(self):
        query = "select distinct C_NAME, C_ADDRESS from CUSTOMER where C_CUSTKEY=42"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_2(self):
        query = "select distinct C.C_NAME, C.C_ADDRESS from CUSTOMER C where C.C_NATIONKEY=7"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_3(self):
        query = "select distinct * from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_NAME='GERMANY'"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_4(self):
        query = "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_NAME='GERMANY'"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_5(self):
        query = "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and CUSTOMER.C_CUSTKEY=42"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_6(self):
        query = "select distinct CUSTOMER.C_CUSTKEY from CUSTOMER, NATION, REGION where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY = REGION.R_REGIONKEY"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_7(self):
        query = "select distinct CUSTOMER.C_CUSTKEY from REGION, NATION, CUSTOMER where CUSTOMER.C_NATIONKEY=NATION.N_NATIONKEY and NATION.N_REGIONKEY = REGION.R_REGIONKEY"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_8(self):
        query = "select distinct * from ORDERS, CUSTOMER where ORDERS.O_ORDERPRIORITY='1-URGENT' and CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_9(self):
        query = "select distinct * from CUSTOMER,ORDERS,LINEITEM where CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR' and CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def test_query_10(self):
        query = "select distinct * from LINEITEM,ORDERS,CUSTOMER where CUSTOMER.C_CUSTKEY=ORDERS.O_CUSTKEY and ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY and LINEITEM.L_SHIPMODE='AIR'and CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD'"
        non_optimized_res, optimized_res, non_optimized_line_number, optimized_line_number = self.method_name(query)
        self.assertEqual(non_optimized_line_number, optimized_line_number)
        self.assertGreater(non_optimized_res//3, optimized_res)

    def method_name(self, query):
        clear_local_tmpfiles()
        output = eval(sf=0, env=ra2mr.ExecEnv.LOCAL, query=query, optimize=True)
        optimized_lines_number = len(output.readlines())
        output.close()
        optimized_res = costcounter.compute_hdfs_costs()
            
        clear_local_tmpfiles()
        output = eval(sf=0, env=ra2mr.ExecEnv.LOCAL, query=query, optimize=False)
        non_optimized_line_number = len(output.readlines())
        output.close()
        non_optimized_res = costcounter.compute_hdfs_costs()

        return non_optimized_res, optimized_res, non_optimized_line_number, optimized_lines_number


if __name__ == '__main__':
    unittest.main()
