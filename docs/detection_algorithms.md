# Detection algorithms

All detectors implement one interface (`detection/base_detector.py`):

```python
class BaseDetector:
    def init_state(self) -> dict: ...                 # fresh state for one (device, metric)
    def update(self, state, value, metric) -> Finding | None
```

State is a plain JSON-serializable `dict`. Every (device, metric) stream has independent
state for every detector. `DetectionEngine` (`detection/engine.py`) runs all enabled
detectors on all five metrics and turns findings into standardized results:

`device_id, plant_id, metric, algorithm, kind, observed, baseline, score, severity, explanation, event_id, detected_at`

Parameters live in `config/detection_config.yaml`.

## Threshold (fixed operating limits)

Flags a value outside hard physical operating limits (e.g. temperature above 92 °C).
Stateless, instantly explainable, and blind to anything that stays inside the limits. It is
the reference point the statistical detectors are measured against.

Severity: excess as a share of the operating band (<5% medium, <15% high, else critical).

## Z-score (adaptive)

$$Z = \frac{x - \mu}{\sigma}$$

`μ` and `σ²` are learned from a 60-sample warm-up, then tracked with an exponentially
weighted moving average (`α = 0.01`, a time constant of about 100 samples). Before a sample
updates the baseline it is **clipped** to `μ ± 3σ`, so a single spike cannot inflate `σ` and
mask the next anomaly. An alarm fires when `|Z| > 5`.

Trade-off: the adaptive baseline absorbs slow drifts. A ramp of +10 °C over 20 minutes never
produces a large `Z`, because `μ` follows it. That gap is what CUSUM and the drift detector
cover, and the benchmark shows it clearly.

## CUSUM (persistent mean shift)

On the standardized sample $z_t = (x_t - \mu_0)/\sigma_0$:

$$S^+_t = \max(0,\ S^+_{t-1} + z_t - k) \qquad S^-_t = \max(0,\ S^-_{t-1} - z_t - k)$$

This is the textbook recursion $S^+_t = \max(0, S^+_{t-1} + x_t - \mu_0 - k)$ written in
sigma units, so one pair of `k`/`h` works for metrics on very different scales (RPM vs kW).
An alarm is raised when either statistic exceeds `h`.

| Parameter | Value | Meaning |
|---|---|---|
| `warmup` | 300 samples | per-device `μ0`, `σ0` |
| `k` | 0.5 σ | allowance: shifts below 0.5 σ are ignored |
| `h` | 12 σ | decision threshold |
| `clip` | 4 σ | robust increments: one spike adds at most 3.5 to S |
| `rebaseline_after_alarm` | true | re-learn `μ0` after an alarm |

Three implementation choices matter:

* **Robust increments.** Clipping `z_t` at ±4 means isolated spikes never trigger CUSUM on
  their own. In the benchmark CUSUM recalls 0% of single-sample temperature spikes, which is
  intended: those belong to the point detectors.
* **Shift estimate.** While a statistic is positive the raw `z` values are accumulated. At
  alarm time their mean estimates the size of the shift ("estimated shift +0.70 σ"), which
  sets the severity and goes into the explanation.
* **Re-baselining.** After an alarm the detector learns the new operating point. The shift
  is reported once instead of on every following sample, and the next change (such as the
  return to normal after maintenance) can still be detected.

**Tuning with evidence.** The first configuration (`warmup = 120`, `h = 10`) produced 0.77
false alerts per device-hour on pure noise. With 120 samples the estimate of `μ0` is off by
about 0.09 σ, which effectively shrinks `k` and makes false alarms far more frequent.
A sweep with `benchmarks/detection_evaluation.py` showed that `warmup = 300`, `h = 12` cuts
false alerts to about 0.07 per device-hour for roughly 8 s of extra detection delay.

> A CUSUM alarm is **evidence of a statistical change, not proof of a defective machine.**
> Alerts carry that sentence, and only an operator can mark an alert as a confirmed failure
> (`POST /alerts/{id}/resolve {"confirmed_failure": true}`).

## Drift (two-window behavioural drift)

Compares how a device *used to* behave (a reference window, 300 samples) with how it behaves
*now* (a sliding 120-sample window), using robust statistics only:

* location shift: $(\tilde{x}_{recent} - \tilde{x}_{ref}) / \hat\sigma_{ref}$
* scale ratio: $\hat\sigma_{recent} / \hat\sigma_{ref}$, with $\hat\sigma = 1.4826 \cdot \mathrm{MAD}$

Medians and MADs ignore isolated spikes. The test runs every 20 samples, and a drift is
raised after 3 consecutive positive checks (|shift| > 1.5 σ or ratio > 2). After a confirmed
drift the recent window **becomes the new reference**, so the detector adapts to the new
concept instead of alarming forever. The explanation includes the OLS trend over the recent
window.

It is the only detector that sees a change in *variability* (bearing wear makes vibration
noisier before it makes it higher).

### Plugging in research algorithms

`drift_detector.py` is deliberately independent of Spark and of the other detectors. An
evolving-stream method (for example an AbruptCUSUM- or TNStream-style detector) can
implement `BaseDetector`, be registered in `DETECTOR_CLASSES`, enabled in the YAML, and
evaluated with `python -m benchmarks.detection_evaluation` without touching ingestion,
Spark or the database. The drift detector shipped here is a robust two-window baseline,
**not** an implementation of those research methods.

## Point anomaly vs persistent change vs concept drift

| Kind | Produced by | Example |
|---|---|---|
| `point_anomaly` | threshold, zscore | one reading of 91 °C on a 65 °C machine |
| `persistent_shift` | cusum | pressure settles 0.7 kPa lower and stays there |
| `concept_drift` | drift | vibration median rises and its spread doubles over 25 minutes |

## Alert management

`detection/alert_manager.py`:

* **Identity:** `alert_id = sha1(device | metric | algorithm | event_id)`. Replaying the same
  events after a crash yields the same ids, and `ON CONFLICT DO NOTHING` makes the insert
  idempotent.
* **Cooldown / de-duplication:** per (device, metric, algorithm), in event time (threshold
  60 s, z-score 60 s, CUSUM 300 s, drift 900 s). A repeat inside the cooldown is suppressed
  unless it is more severe, and the next emitted alert reports how many were suppressed.
* **Lifecycle:** `open → acknowledged → resolved`, with `confirmed_failure` true / false /
  unknown recorded at resolution.
