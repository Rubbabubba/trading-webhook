import unittest
from opportunity_lab.kalshi_demo_stream_worker import compact_status

class CompactCaptureState(unittest.TestCase):
    def packet(self,state='collecting',acks=2,usable=None):
        return {'state':state,'acknowledged_channels':acks,
                'usable_symbols':['TEST'] if usable is None else usable,
                'formula_comparison':{'state':'collecting','signals':20,
                  'outcome_coverage':{'missed':2,'complete_signals':19},
                  'sqlite_page_cap':4096}}

    def test_capacity_stop_preserves_evidence_but_not_active_capture(self):
        raw=self.packet('capacity_stop');out=compact_status(raw)
        self.assertFalse(out['new_source_inputs_active'])
        self.assertTrue(out['retained_book_counts_only'])
        self.assertEqual(out['usable_symbol_count'],1)
        comp=out['formula_comparison']
        self.assertEqual(comp['state'],'collecting')
        self.assertEqual(comp['source_input_state'],'source_capacity_stop')
        self.assertEqual(comp['outcome_coverage']['missed'],2)
        self.assertEqual(comp['sqlite_page_cap'],4096)
        self.assertNotIn('source_input_state',raw['formula_comparison'])

    def test_acknowledged_collecting_source_can_admit_new_inputs(self):
        out=compact_status(self.packet())
        self.assertTrue(out['new_source_inputs_active'])
        self.assertFalse(out['retained_book_counts_only'])
        self.assertEqual(out['formula_comparison']['source_input_state'],'source_active')

    def test_disconnected_fault_missing_ack_and_missing_book_are_inactive(self):
        cases=[self.packet('connecting'),self.packet('source_or_transport_fault'),
               self.packet(acks=1),self.packet(usable=[]),{}]
        for raw in cases:
            with self.subTest(raw=raw):
                out=compact_status(raw)
                self.assertFalse(out['new_source_inputs_active'])
                self.assertTrue(out['retained_book_counts_only'])

if __name__=='__main__':unittest.main()
