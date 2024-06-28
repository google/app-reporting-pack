# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import itertools

import pandas as pd
from scripts import backfill_snapshots


def test_restore_bid_budget_history():
  change_history = pd.DataFrame(
    data=[
      [
        '2024-01-01',
        1234567890,
        10,
        20,
        20,
        10,
        None,
        None,
      ]
    ],
    columns=(
      'change_date',
      'campaign_id',
      'old_budget_amount',
      'new_budget_amount',
      'old_target_cpa',
      'new_target_cpa',
      'old_target_roas',
      'new_target_roas',
    ),
  )
  current_bid_budgets = pd.DataFrame(
    data=[
      [
        1234567890,
        20,
        10,
        None,
      ],
      [
        1234567891,
        20,
        10,
        None,
      ],
    ],
    columns=(
      'campaign_id',
      'budget_amount',
      'target_cpa',
      'target_roas',
    ),
  )
  campaign_ids = [1234567890, 1234567891]
  dates = ['2023-12-31', '2024-01-01', '2024-01-02']
  placeholders = backfill_snapshots._prepare_placeholders(campaign_ids, dates)
  restored_change_history = backfill_snapshots._restore_bid_budget_history(
    change_history, current_bid_budgets, placeholders
  )

  expected_change_history = pd.DataFrame(
    {
      'day': dates * len(campaign_ids),
      'campaign_id': list(
        itertools.chain.from_iterable(
          itertools.repeat(x, len(dates)) for x in campaign_ids
        )
      ),
      'budget_amount': [10, 20, 20, 20, 20, 20],
      'target_cpa': [20, 10, 10, 10, 10, 10],
      'target_roas': [None] * 6,
    }
  )
  assert restored_change_history.equals(expected_change_history)
