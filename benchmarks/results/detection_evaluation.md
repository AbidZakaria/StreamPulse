| Detector | Alerts | Precision | Recall (all types) | False-positive rate | False alerts / device-hour | Median delay |
|---|---:|---:|---:|---:|---:|---:|
| threshold | 74 | 100.0% | 11.8% | 0.0000% | 0.000 | 0 s |
| zscore | 464 | 98.7% | 84.4% | 0.0001% | 0.017 | 0 s |
| cusum | 1053 | 97.5% | 75.6% | 0.0004% | 0.072 | 4 s |
| drift | 441 | 94.1% | 38.2% | 0.0017% | 0.072 | 329 s |

Recall by anomaly type (median detection delay in brackets):

| Anomaly type | Category | Episodes | threshold | zscore | cusum | drift |
|---|---|---:|---:|---:|---:|---:|
| temperature_spike | sudden | 109 | 19% (0 s) | 99% (0 s) | 0% | 1% (3 s) |
| vibration_burst | sudden | 92 | 16% (0 s) | 100% (0 s) | 92% (3 s) | 0% |
| pressure_drop | sudden | 82 | 11% (0 s) | 98% (0 s) | 94% (3 s) | 45% (83 s) |
| power_surge | sudden | 65 | 18% (0 s) | 100% (0 s) | 97% (3 s) | 0% |
| temperature_drift | gradual | 88 | 0% | 94% (2293 s) | 100% (114 s) | 100% (318 s) |
| vibration_degradation | gradual | 72 | 4% (1130 s) | 1% (456 s) | 99% (177 s) | 94% (452 s) |

Gradual anomalies detected *while still developing* (first alert before the episode ended; the recall above also credits alerts on the snap back to baseline at the end of an episode):

| Anomaly type | threshold | zscore | cusum | drift |
|---|---:|---:|---:|---:|
| temperature_drift | 0% | 0% | 100% | 100% |
| vibration_degradation | 4% | 1% | 99% | 94% |
