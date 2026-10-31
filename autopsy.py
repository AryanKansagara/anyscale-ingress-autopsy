"""Measured localhost HTTP pipeline; delays are authored CPU-stage stand-ins."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import statistics
import threading
import time
from urllib.request import urlopen


def diagnose(traces):
    if len(traces) < 8:
        return {'cause': 'inconclusive', 'reason': 'at least eight complete traces required'}
    names = ['arrival', 'ingress_start', 'ingress_end', 'worker_start', 'worker_end']
    for trace in traces:
        if not isinstance(trace,dict):
            return {'cause': 'inconclusive', 'reason': 'malformed trace'}
        if any(type(trace.get(k)) not in (int, float) or not -1e12 <= trace[k] <= 1e12 for k in names):
            return {'cause': 'inconclusive', 'reason': 'missing or nonfinite timestamps'}
        if any(trace[x] > trace[y] for x, y in zip(names, names[1:])):
            return {'cause': 'inconclusive', 'reason': 'clock/order violation'}
    ingress = statistics.mean(t['ingress_end'] - t['arrival'] for t in traces)
    worker = statistics.mean(t['worker_end'] - t['ingress_end'] for t in traces)
    if min(ingress,worker) <= 0:
        return {'cause': 'inconclusive', 'reason': 'zero residence or insufficient clock resolution'}
    cause = 'balanced' if max(ingress, worker) < 1.5 * min(ingress, worker) else ('ingress' if ingress > worker else 'worker')
    return {'cause': cause, 'mean_ingress_queue_and_service_seconds': ingress,
            'mean_worker_queue_and_service_seconds': worker,
            'reason': 'local stage residence ratio; hypothesis, not causal proof'}


def experiment(*, ingress=1, workers=8, ingress_delay=.015, worker_delay=.001, requests=24, clients=12):
    for n in (ingress, workers, requests, clients):
        if type(n) is not int or not 1 <= n <= 128:
            raise ValueError('capacities and request count must be integers from 1 to 128')
    if any(type(d) not in (float, int) or not 0 <= d <= .2 for d in (ingress_delay, worker_delay)):
        raise ValueError('delays must be finite in [0, .2] seconds')
    admission, work = threading.Semaphore(ingress), threading.Semaphore(workers)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def do_GET(self):
            if self.path != '/infer':
                self.send_error(404); return
            trace = {'arrival': time.perf_counter()}
            with admission:
                trace['ingress_start'] = time.perf_counter()
                time.sleep(ingress_delay)
                trace['ingress_end'] = time.perf_counter()
            with work:
                trace['worker_start'] = time.perf_counter()
                time.sleep(worker_delay)
                trace['worker_end'] = time.perf_counter()
            body = json.dumps(trace).encode()
            self.send_response(200); self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body))); self.end_headers(); self.wfile.write(body)
    class Server(ThreadingHTTPServer):
        request_queue_size = 128
    server = Server(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
    thread.start()
    def fetch(_):
        with urlopen(f'http://127.0.0.1:{server.server_port}/infer', timeout=15) as response:
            return json.loads(response.read())
    start = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=clients) as pool:
            traces = list(pool.map(fetch, range(requests)))
        elapsed = time.perf_counter() - start
    finally:
        server.shutdown(); server.server_close(); thread.join()
    return {'configuration': {'ingress': ingress, 'workers': workers, 'ingress_delay': ingress_delay,
                             'worker_delay': worker_delay, 'requests': requests, 'clients': clients},
            'elapsed_seconds': elapsed, 'requests_per_second': requests / elapsed,
            'diagnosis': diagnose(traces), 'traces': traces}


def demo():
    cases = {}
    for name, delays, capacity in [('ingress', (.015, .001), (1, 8)), ('worker', (.001, .015), (8, 1))]:
        i, w = capacity
        runs = {'baseline': experiment(ingress=i, workers=w, ingress_delay=delays[0], worker_delay=delays[1]),
                'increase_ingress': experiment(ingress=8 if i == 1 else 16, workers=w, ingress_delay=delays[0], worker_delay=delays[1]),
                'increase_workers': experiment(ingress=i, workers=8 if w == 1 else 16, ingress_delay=delays[0], worker_delay=delays[1])}
        base = runs['baseline']['elapsed_seconds']
        for run in runs.values(): run['observed_speedup_vs_baseline'] = base / run['elapsed_seconds']
        cases[name] = runs
    return {'schema_version': 1, 'transport': 'real_loopback_http', 'ray_integration': False,
            'gpu_or_model_inference': False, 'workload': 'injected sleep delays; not CPU compute',
            'measurement_scope': 'single-host client wall time, no cold start or distributed clocks', 'cases': cases}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(); parser.add_argument('--output'); args = parser.parse_args()
    result = json.dumps(demo(), indent=2, allow_nan=False)
    if args.output:
        with open(args.output, 'w') as f: f.write(result + '\n')
    print(result)
