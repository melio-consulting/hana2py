from unittest import TestCase
from hana2py import utilities as u
import pandas as pd

iris = pd.read_csv('hana2py/data/iris.csv')

class Test(TestCase):
    """
    Test utilities module
    """

    def test_millify_float_return_string(self):
        num = iris.sepal_length.tolist()[0]*1e3
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
        with self.assertWarns(UserWarning):
            u.get_sqlserver_connection_details('incorrect_key')

    def test_sqlserver_engine_exception(self):
        server = None
        with self.assertRaises(Exception):
            u.create_sqlserver_engine(server, True)
