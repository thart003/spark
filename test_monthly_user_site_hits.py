from chispa.dataframe_comparer import *

from ..jobs.monthly_user_site_hits.
from collections import namedtuple

MonthlySiteHit = namedtuple("Monthly
MonthlySiteHitsAgg = namedtuple("Monthly

def test_monthly_site_hits(spark):
  ds = "2023-03-01"
  new_month_start = "2023-04-01"
  input_data = [
      MonthlySiteHit(
          month_start=ds,
          hit_array=[0,1,3],
          date_partitions=ds
      ),
      MonthlySiteHit(
          month_start=ds,
          

