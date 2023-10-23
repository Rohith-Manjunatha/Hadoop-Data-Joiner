#!/usr/bin/python

import unittest
from mrjob.job import MRJob
import logging
import codecs

# dont show logs
logging.disable(logging.CRITICAL)

class TestJoinMapReduce(unittest.TestCase):

    def run_mr_job(self, input_data):
        mr_job = MR_Join_Test()
        input_data_bytes = [codecs.encode(data, 'utf-8') for data in input_data]
        mr_job.sandbox(stdin=input_data_bytes)
        with mr_job.make_runner() as runner:
            runner.run()
            actual_output = list(runner.cat_output())
        return actual_output

    def test_case1(self):
        input1 = "6/30/2018,HOMICIDE,50-59,M,BLK,4"
        input2 = "9/4/1992 4:33,400 W 101ST STREET,AUTO,WASHINGTON HEIGHTS,HOMICIDE,20-29,M,BLK,YES"
        expected_output = '"6/30/2018,HOMICIDE,50-59,M,BLK,4 | 9/4/1992 4:33,400 W 101ST STREET,AUTO,WASHINGTON HEIGHTS,HOMICIDE,20-29,M,BLK,YES"'

        actual_output = self.run_mr_job([input1, input2])

        # Decode the actual output from bytes to string
        actual_output = [codecs.decode(output, 'utf-8').strip() for output in actual_output if output.strip()]
        result = actual_output[0].split('\t')[-1]
        self.assertEqual(result, expected_output, 'Testcase 1 Failed')

    def test_case2(self):
        input1 = "6/30/2015,BATTERY,0-19,F,BLK,13"
        input2 = "12/21/2013 14:52,4600 W ERIE ST,STREET,AUSTIN,BATTERY,20-29,M,BLK,YES"
        expected_output = '"6/30/2015,BATTERY,0-19,F,BLK,13 | 12/21/2013 14:52,4600 W ERIE ST,STREET,AUSTIN,BATTERY,20-29,M,BLK,YES"'

        actual_output = self.run_mr_job([input1, input2])

        # Decode the actual output from bytes to string
        actual_output = [codecs.decode(output, 'utf-8').strip() for output in actual_output if output.strip()]
        result = actual_output[0].split('\t')[-1]
        self.assertEqual(result, expected_output, 'Testcase 2 Failed')

    # trying to join ROBBERY and HOMICIDE
    def test_case3(self):
        input1 = "6/30/2019,ROBBERY,UNKNOWN,F,UNKNOWN,1"
        input2 = "8/30/1992 16:55,10700 S STATE,GARAGE,ROSELAND,HOMICIDE,0-19,M,BLK,NO"
        expected_output = []  # No expected output

        actual_output = self.run_mr_job([input1, input2])

        # Decode the actual output from bytes to string
        actual_output = [codecs.decode(output, 'utf-8').strip() for output in actual_output if output.strip()]
        self.assertEqual(actual_output, expected_output, 'Testcase 3 Failed')

class MR_Join_Test(MRJob):

    def configure_args(self):
        super(MR_Join_Test, self).configure_args()

    def mapper(self, _, line):
        columns = line.split(',')
        common_column1 = columns[1]
        common_column2 = columns[-5]
        yield common_column1, ('D1', line)
        yield common_column2, ('D2', line)

    def reducer(self, key, values):
        dataset1 = []
        dataset2 = []
        for value in values:
            if value[0] == 'D1':
                dataset1.append(value[1])
            elif value[0] == 'D2':
                dataset2.append(value[1])
        if dataset1 and dataset2:  # Ensure there are matching rows
            for row1 in dataset1:
                for row2 in dataset2:
                    if row1 != row2: # Ensure there is no selfjoin
                        yield None, row1 + ' | ' + row2

if __name__ == '__main__':
    unittest.main()

