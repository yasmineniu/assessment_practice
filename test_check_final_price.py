import unittest
import argparse
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import datetime
import sys
import os
from check_final_price import non_negative_demand_number, validate_date, check_final_price, main


class TestCheckFinalPrice(unittest.TestCase):
    def setUp(self):
        # Sample test data
        self.sample_data = pd.DataFrame({
            'cumulative_volume': [100, 200, 300, 400, 500],
            'bid_price': [10.5, 20.0, 30.0, 40.0, 50.0]
        })
        
        # Mock the CSV reading and other dependencies
        self.patcher1 = patch('pandas.read_csv')
        self.patcher2 = patch('check_final_price.select_certain_period')
        self.patcher3 = patch('check_final_price.cumulative_vol_for_certain_period')
        self.patcher4 = patch('check_final_price.logger')
        
        self.mock_read_csv = self.patcher1.start()
        self.mock_select = self.patcher2.start()
        self.mock_cumulative = self.patcher3.start()
        self.mock_logger = self.patcher4.start()
        
        # Configure mocks
        self.mock_select.return_value = "selected_data"
        self.mock_cumulative.return_value = (self.sample_data, 500)
        self.mock_read_csv.return_value = "full_table"

    def tearDown(self):
        self.patcher1.stop()
        self.patcher2.stop()
        self.patcher3.stop()
        self.patcher4.stop()

    def test_valid_dates(self):
        # Test properly formatted dates within the valid range
        valid_dates = [
            "2023-01-01",  # First day
            "2023-01-15",  # Middle of month
            "2023-01-31"   # Last day
        ]
        for date_str in valid_dates:
            with self.subTest(date_str=date_str):
                self.assertEqual(validate_date(date_str), date_str)
    
    def test_invalid_formats(self):
        # Test malformed date strings
        invalid_formats = [
            "2023/01/01",  # Wrong separator
            "01-01-2023",  # Wrong order
            "2023-9-1",    # Missing padding zeros
            "2023-01-32",   # Invalid day
            "2023-00-15",   # Invalid month
            "not-a-date",   # Completely invalid
            "2023-01-01T00:00:00"  # Includes time
        ]
        for date_str in invalid_formats:
            with self.subTest(date_str=date_str):
                with self.assertRaises(argparse.ArgumentTypeError):
                    validate_date(date_str)
    

    def test_non_negative_demand_number_valid(self):
        # Test valid non-negative numbers
        self.assertEqual(non_negative_demand_number("100"), 100.0)
        self.assertEqual(non_negative_demand_number("3.14"), 3.14)
        self.assertEqual(non_negative_demand_number("0"), 0.0)

    def test_non_negative_demand_number_invalid(self):
        # Test invalid inputs
        with self.assertRaises(argparse.ArgumentTypeError):
            non_negative_demand_number("-5")
            
        with self.assertRaises(argparse.ArgumentTypeError):
            non_negative_demand_number("abc")

    def test_check_final_price_normal(self):
        # Test normal cases for check_final_price
        self.assertEqual(check_final_price(self.sample_data, 150, 500), 20.0)
        self.assertEqual(check_final_price(self.sample_data, 300, 500), 30.0)
        self.assertEqual(check_final_price(self.sample_data, 500, 500), 50.0)

    def test_check_final_price_edge_cases(self):
        # Test edge cases for check_final_price
        self.assertEqual(check_final_price(self.sample_data, 0, 500), 10.5)
        self.assertEqual(check_final_price(self.sample_data, 100, 500), 10.5)

    def test_check_final_price_demand_exceeds_max(self):
        # Test when demand exceeds max volume
        with self.assertRaises(ValueError):
            check_final_price(self.sample_data, 501, 500)

    @patch('builtins.print')
    def test_main_success(self, mock_print):
        # Test main function with valid arguments
        test_args = ["check_final_price.py", "--date", "2023-01-01", "--period", "1", "--demand", "300"]
        with patch.object(sys, 'argv', test_args):
            main(argparse.Namespace(date="2023-01-01", period=1, demand=300))
            mock_print.assert_called_with("based on the demand 300, final clearing price is 30.0")

    @patch('argparse.ArgumentParser.parse_args')
    def test_cli_argument_parsing(self, mock_parse):
        # Test CLI argument parsing
        mock_args = MagicMock()
        mock_args.date = "2023-01-01"
        mock_args.period = 1
        mock_args.demand = 300
        mock_parse.return_value = mock_args
        
        # test the if __name__ == "__main__" block
        with patch.object(sys, 'argv', ["check_final_price.py", "--date", "2023-01-01", "--period", "1", "--demand", "300"]):
            import __main__
            if hasattr(__main__, '__file__') and os.path.basename(__main__.__file__) == 'check_final_price.py':
                # Verify the parser was called with expected arguments
                mock_parse.assert_called_once()

if __name__ == '__main__':
    unittest.main()