import copy
import unittest
from autopsy import diagnose, experiment

def traces(ingress=10, worker=1):
    return [dict(arrival=0, ingress_start=0, ingress_end=ingress,
                 worker_start=ingress, worker_end=ingress+worker) for _ in range(8)]
class Tests(unittest.TestCase):
    def test_ingress(self): self.assertEqual(diagnose(traces())['cause'], 'ingress')
    def test_worker(self): self.assertEqual(diagnose(traces(1,10))['cause'], 'worker')
    def test_balanced(self): self.assertEqual(diagnose(traces(2,2))['cause'], 'balanced')
    def test_insufficient(self): self.assertEqual(diagnose(traces()[:7])['cause'], 'inconclusive')
    def test_missing(self):
        rows=traces(); del rows[0]['worker_end']; self.assertEqual(diagnose(rows)['cause'], 'inconclusive')
    def test_clock_reversal(self):
        rows=traces(); rows[0]['worker_start']=-1; self.assertEqual(diagnose(rows)['cause'], 'inconclusive')
    def test_nonfinite(self):
        rows=traces(); rows[0]['arrival']=float('nan'); self.assertEqual(diagnose(rows)['cause'], 'inconclusive')
    def test_huge_integer_abstains(self):
        rows=traces();rows[0]['arrival']=10**400;self.assertEqual(diagnose(rows)['cause'],'inconclusive')
    def test_zero_resolution_abstains(self):
        self.assertEqual(diagnose(traces(0,0))['cause'],'inconclusive')
    def test_queue_attribution(self):
        rows=traces(1,1)
        for r in rows:r.update(ingress_start=9,ingress_end=10,worker_start=10,worker_end=11)
        self.assertEqual(diagnose(rows)['cause'], 'ingress')
    def test_http_end_to_end(self):
        run=experiment(requests=8,clients=4,ingress_delay=.002,worker_delay=.001)
        self.assertEqual(len(run['traces']),8);self.assertGreater(run['elapsed_seconds'],0)
        self.assertNotEqual(run['diagnosis']['cause'],'inconclusive')
    def test_capacities_reject(self):
        for value in (0,True,129):
            with self.assertRaises(ValueError):experiment(ingress=value)
    def test_delays_reject(self):
        for value in (-1,float('inf'),float('nan'),10**400,True):
            with self.assertRaises(ValueError):experiment(worker_delay=value)
if __name__=='__main__':unittest.main()
