r"""Detector B: two-sided robust CUSUM change detection.

On the standardized sample :math:`z_t = (x_t - \mu_0) / \sigma_0`:

    S+_t = max(0, S+_{t-1} + z_t - k)
    S-_t = max(0, S-_{t-1} - z_t - k)

which is the textbook recursion :math:`S^+_t = \max(0, S^+_{t-1} + x_t - \mu_0 - k)`
expressed in sigma units, so ``k`` and ``h`` are comparable across metrics with very
different scales (rpm vs. kW). An alarm is raised when either statistic exceeds ``h``.

Implementation choices:

* **Warm-up.** ``mu0`` / ``sigma0`` are learned from the first ``warmup`` samples of each
  (device, metric) stream; every device has its own operating point.
* **Robust increments.** ``z_t`` is clipped to ``+/- clip`` so one huge spike cannot
  push the statistic over ``h`` on its own. Spikes are the point detectors' job; CUSUM
  is reserved for small *persistent* shifts.
* **Shift estimate.** While a statistic is positive we accumulate the raw ``z``; at
  alarm time the mean of that run estimates the size of the shift.
* **Re-baselining.** After an alarm the detector re-learns ``mu0`` (configurable). A
  shift is reported once instead of on every following sample, and the detector can then
  catch the *next* change, such as the return to normal after maintenance.
"""

from __future__ import annotations

from detection.base_detector import (
    KIND_SHIFT,
    METRIC_UNITS,
    BaseDetector,
    Finding,
    Welford,
    fmt,
    severity_from_ratio,
)


class CusumDetector(BaseDetector):
    name = "cusum"
    kind = KIND_SHIFT

    def init_state(self) -> dict:
        return {
            "warm": Welford.init(),
            "mu0": None,
            "sigma": None,
            "s_pos": 0.0,
            "s_neg": 0.0,
            "n_pos": 0,
            "n_neg": 0,
            "sum_pos": 0.0,
            "sum_neg": 0.0,
            "alarms": 0,
        }

    def _rebaseline(self, state: dict) -> None:
        alarms = state["alarms"]
        state.clear()
        state.update(self.init_state())
        state["alarms"] = alarms

    def update(self, state: dict, value: float, metric: str) -> Finding | None:
        p = self.params
        if state["mu0"] is None:
            Welford.add(state["warm"], value)
            if state["warm"]["n"] >= p["warmup"]:
                state["mu0"] = state["warm"]["mean"]
                state["sigma"] = max(Welford.std(state["warm"]), 1e-6)
                state["warm"] = None
            return None

        mu0, sigma, k, h = state["mu0"], state["sigma"], p["k"], p["h"]
        z = (value - mu0) / sigma
        zc = max(-p["clip"], min(p["clip"], z))

        state["s_pos"] = max(0.0, state["s_pos"] + zc - k)
        state["s_neg"] = max(0.0, state["s_neg"] - zc - k)
        if state["s_pos"] > 0:
            state["n_pos"] += 1
            state["sum_pos"] += z
        else:
            state["n_pos"], state["sum_pos"] = 0, 0.0
        if state["s_neg"] > 0:
            state["n_neg"] += 1
            state["sum_neg"] += z
        else:
            state["n_neg"], state["sum_neg"] = 0, 0.0

        if state["s_pos"] <= h and state["s_neg"] <= h:
            return None

        upward = state["s_pos"] >= state["s_neg"]
        stat = state["s_pos"] if upward else state["s_neg"]
        n_run = max(state["n_pos"] if upward else state["n_neg"], 1)
        shift_sigma = (state["sum_pos"] if upward else state["sum_neg"]) / n_run
        shift_units = shift_sigma * sigma
        finding = Finding(
            observed=value,
            baseline=mu0,
            score=round(stat, 3),
            severity=severity_from_ratio(abs(shift_sigma), cuts=(1.5, 3.0, 5.0)),
            explanation=(
                f"Persistent {'upward' if upward else 'downward'} shift in {metric}: CUSUM "
                f"S{'+' if upward else '-'} = {stat:.1f} > h = {h:g} over {n_run} samples; "
                f"estimated shift {shift_sigma:+.2f} sigma ({shift_units:+.2f} {METRIC_UNITS.get(metric, '')})"
                f" from baseline {fmt(mu0, metric)}. Statistical evidence of change, not a confirmed failure."
            ),
        )
        state["alarms"] += 1
        if p.get("rebaseline_after_alarm", True):
            self._rebaseline(state)
        else:
            state.update(s_pos=0.0, s_neg=0.0, n_pos=0, n_neg=0, sum_pos=0.0, sum_neg=0.0)
        return finding
