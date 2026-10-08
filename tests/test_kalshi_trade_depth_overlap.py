import sqlite3
import unittest

from opportunity_lab.kalshi_trade_depth_overlap import report


class TradeDepthOverlapTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.executescript('''
            CREATE TABLE depth_cohort_trades(id TEXT,ticker TEXT,executed_at REAL);
            CREATE TABLE targeted_public_trades(id TEXT,ticker TEXT,executed_at REAL);
            CREATE TABLE depth_snapshots(ticker TEXT,observed_at REAL);
            CREATE TABLE v12_shadow_signals(ticker TEXT,observed_at REAL);
        ''')

    def test_only_prior_observations_and_deduplicated_public_trades(self):
        self.db.executemany('INSERT INTO depth_cohort_trades VALUES(?,?,?)', [
            ('a', 'A', 100), ('b', 'B', 100), ('c', 'A', 200)])
        self.db.execute("INSERT INTO targeted_public_trades VALUES('a','A',100)")
        self.db.executemany('INSERT INTO depth_snapshots VALUES(?,?)', [
            ('A', 90), ('B', 101), ('A', 201)])
        self.db.executemany('INSERT INTO v12_shadow_signals VALUES(?,?)', [
            ('A', 80), ('B', 90), ('A', 201)])
        result = report(self.db)
        self.assertEqual(result['sampled_public_trades'], 3)
        self.assertEqual(result['prior_depth_within_30s'], 1)
        self.assertEqual(result['prior_v12_signal_within_300s'], 3)
        self.assertEqual(result['both_prior_observations'], 1)
        self.assertEqual(result['both_prior_observation_tickers'], 1)
        self.assertFalse(result['promotion_ready'])

    def test_missing_tables_are_visible(self):
        self.db.execute('DROP TABLE depth_snapshots')
        self.assertEqual(report(self.db)['missing_tables'], ['depth_snapshots'])


if __name__ == '__main__':
    unittest.main()
