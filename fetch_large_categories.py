#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script fetches and displays the large categories from MoneyForward API.
"""

import argparse
import requests
import json
from moneyforward_api import session_from_cookie_file, request_large_categories

def main():
    parser = argparse.ArgumentParser(description="Fetch large categories from MoneyForward API.")
    parser.add_argument("--cookie-file", type=str, default="mf_cookies.pkl", help="Path to the cookie file.")
    args = parser.parse_args()

    with session_from_cookie_file(args.cookie_file) as session:
        try:
            large_categories = request_large_categories(session)
            print(json.dumps(large_categories, indent=2, ensure_ascii=False))
        except requests.RequestException as e:
            print(f"Error fetching large categories: {e}")

if __name__ == "__main__":
    main()