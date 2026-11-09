import json
from pathlib import Path
import tempfile
import unittest
import compare_q0 as c

class ComparisonTests(unittest.TestCase):
    def fixture(self, root, status='COMPLETE', rate=1000):
        root.mkdir()
        (root/'A-r1').mkdir()
        run = {'profile':'A', 'seed':42, 'status':status, 'profile_config':{'rate':rate},
               'environment':{}, 'generator_result':{'oracle':{'admitted':0, 'delivered':1}}}
        (root/'A-r1/run.json').write_text(json.dumps(run))
        (root/'manifest.json').write_text(json.dumps({'runs':[{'directory':'A-r1','status':status}]}))

    def test_pair_preserves_counts_and_semantic_delta(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=Path(d)/'a',Path(d)/'b'
            self.fixture(a); self.fixture(b)
            result=c.compare(a,b)
            self.assertEqual(result['runs'][0]['nats']['counts']['admitted'],0)
            self.assertEqual(result['runs'][0]['comparison'],'EXPLORATORY')
            self.assertIn('unconfirmed',result['semantics'][0])

    def test_parity_mismatch_is_error(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=Path(d)/'a',Path(d)/'b'
            self.fixture(a); self.fixture(b,rate=2000)
            with self.assertRaises(ValueError): c.compare(a,b)

    def test_invalid_not_hidden(self):
        with tempfile.TemporaryDirectory() as d:
            a,b=Path(d)/'a',Path(d)/'b'
            self.fixture(a); self.fixture(b,status='INVALID')
            self.assertEqual(c.compare(a,b)['runs'][0]['comparison'],'INVALID_OR_NOT_RUN')

    def test_missing_artifact_fails(self):
        with tempfile.TemporaryDirectory() as d:
            a=Path(d)/'a'; self.fixture(a)
            (a/'A-r1/run.json').unlink()
            with self.assertRaises(FileNotFoundError): c.read_runs(a)

    def test_offline_offer_analysis(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'events.jsonl'
            p.write_text(''.join(json.dumps({'event':'offered','measured':True,'at_ns':n})+'\n' for n in [0,1000,2000000]))
            self.assertEqual(c.offer_analysis(p)['gaps_over_1ms'],1)

if __name__=='__main__': unittest.main()
