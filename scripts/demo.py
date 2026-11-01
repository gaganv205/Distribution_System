"""Fire a batch of mixed-priority jobs at a running system and watch them complete.
Shows priority ordering and which worker handled each job.
    python scripts/demo.py --url http://localhost:8000 --jobs 12"""
import argparse
import random
import time

import httpx

SAMPLES = [[5.1, 3.5, 1.4, 0.2], [6.7, 3.0, 5.2, 2.3], [5.9, 3.0, 4.2, 1.5], [6.3, 2.9, 5.6, 1.8]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--jobs", type=int, default=12)
    args = ap.parse_args()

    with httpx.Client(base_url=args.url, timeout=10) as c:
        ids = []
        for i in range(args.jobs):
            prio = random.choice(["low", "normal", "high"])
            r = c.post("/jobs", json={"input": {"features": random.choice(SAMPLES)}, "priority": prio})
            r.raise_for_status()
            ids.append(r.json()["job_id"])
        print(f"submitted {len(ids)} jobs; queue now {c.get('/stats').json()}")

        pending = set(ids)
        done = []
        while pending:
            for jid in list(pending):
                job = c.get(f"/jobs/{jid}").json()
                if job["status"] in ("COMPLETED", "FAILED", "CANCELLED"):
                    pending.discard(jid)
                    done.append(job)
            time.sleep(1.0)

        print(f"\n{'started':>8}  {'priority':8} {'status':10} {'worker':28} result")
        t0 = min(j["created_at"] for j in done)
        for j in sorted(done, key=lambda j: j["started_at"] or 0):
            label = (j["result"] or {}).get("label", j["error"])
            print(f"{(j['started_at'] or 0) - t0:7.2f}s  {j['priority']:8} {j['status']:10} "
                  f"{(j['worker_id'] or '-')[:28]:28} {label}")


if __name__ == "__main__":
    main()
