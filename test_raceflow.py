from datetime import datetime
import pytz
from unittest import TestCase
from raceflow import RaceFlowConverter


class Test(TestCase):

    def setUp(self) :
        self.converter = RaceFlowConverter(datetime(2021, 8, 7, 7, tzinfo=pytz.UTC))

    def test_update_race_flow_empty(self):
        test_input = {1: {}}
        expected_output = '{}'
        test_output = self.converter.convert_flow_data(test_input)
        self.assertEqual(expected_output, test_output)

    def test_update_race_flow_simple(self):
        test_input = {1: {'Alice': {1628319600: 0, 1628319750: 0.5, 1628319900: 1}}}
        expected_output = '{"A": {"color": "#1f77b4", "data": [{"x": 0.0, "y": 0.0}, {"x": 1.0, "y": 0.0}]}}'
        test_output = self.converter.convert_flow_data(test_input)
        self.assertEqual(expected_output, test_output)
