#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from glob import glob
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument('-o', '--output', default='my.output.csv')
args, rest_argv = parser.parse_known_args()


dfs = []
for fn in [x for ra in rest_argv for x in glob(ra)]:
  dfs.append(pd.read_csv(fn, index_col=0))

df = pd.concat(dfs).drop_duplicates()
df.to_csv(args.output, index=True, encoding='utf_8_sig')

