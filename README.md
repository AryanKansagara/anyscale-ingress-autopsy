# Ingress Autopsy
Anyscale manages distributed AI infrastructure around Ray. This independent lab tests whether scaling admission or workers addresses a controlled bottleneck.

Run Python 3.11+ with no dependencies: `python -m unittest -v`, then `python autopsy.py --output measured-demo.json`. Requests cross a real localhost HTTP socket; separate bounded ingress/worker semaphores emit monotonic stage timestamps. The demo measures baseline, right intervention, and wrong intervention for both injected causes. Checked-in `measured-demo.json` is an actual run; host scheduling changes the numbers. The diagnosis uses stage residence ratios and abstains on missing, reversed, nonfinite or insufficient traces. Speedup values are observations, not guaranteed assertions.

Ray is unavailable in this environment. There is **no Ray, Anyscale, GPU or model integration**. Sleeps simulate service delay rather than CPU work. Startup, distributed clocks, autoscaling and real serialization are excluded. Python fits HTTP orchestration; no native inference runtime is implemented. Next stage: reproduce the intervention matrix in pinned Ray Serve, add explicit cold-start events and profile real CPU work.

Primary sources motivating the experiment: https://docs.ray.io/en/latest/serve/llm/architecture/overview.html and https://github.com/ray-project/ray/blob/master/doc/source/serve/llm/user-guides/deployment-initialization.md (research 2026-10-31). Ray already provides observability/autoscaling; this fixture is a falsifiable diagnosis exercise, not a claimed missing product feature or affiliation.
