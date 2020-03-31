import warnings
from unittest import TestCase
from dotenv import load_dotenv
import pandas as pd
from hana2py import utilities as u

load_dotenv()

IRIS = pd.read_csv('hana2py/data/iris.csv')

class Test(TestCase):
    """
    Test utilities module
    """

    def test_millify_float_return_string(self):
        num = IRIS.sepal_length.tolist()[0]*1e3
        millify_num = u.millify(num)
        print(millify_num)
        assert millify_num == '5k'

    def test_get_hana_connection_details(self):
        user, pwd, host, port = u.get_hana_connection_details('user',
                                                              'password',
                                                              'host',
                                                              'port')
        assert user == 'my_user'
        assert pwd == 'mypwd'
        assert host == '192.168.1.1'
        assert port == '8080'

    def test_get_sqlserver_connection_details(self):
        server = u.get_sqlserver_connection_details('server')
        assert server == 'my_server'

    def test_get_sqlserver_none_detail(self):

        with warnings.catch_warnings(record=True) as warn:
            warnings.simplefilter("always")
            u.get_sqlserver_connection_details('incorrect_key')
            assert len(warn) == 1
            assert issubclass(warn[-1].category, UserWarning)
            assert ".env" in str(warn[-1].message)

    def test_sqlserver_engine_exception(self):
        server = None
        with self.assertRaises(Exception):
            u.create_sqlserver_engine(server, True)
