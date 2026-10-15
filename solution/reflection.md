# Reflection (Data Siege Defense)

### 1. Which fault types were hardest to catch, and why?

The hardest fault types to detect were the **subtle-tier statistical anomalies**, specifically:
- **Subtle `data_batch` shifts and spikes** (`distribution_shift`, `null_spike`, `freshness_lag`, `volume_drop`/`volume_spike`): In the subtle tier, metrics sit directly within the normal operational distribution of clean batches. For instance, subtle null rates (~0.0076–0.0083) and row counts (469–519) fell squarely inside the observed clean envelope (clean batches naturally reached null rates up to 0.0105 and row counts between 448 and 554).
- **Subtle `lineage_run` runtime anomalies**: Subtle execution runtimes (~4480–4740 ms) completely overlapped with normal execution runtimes (clean runs spanned 3471 to 4805 ms against a 3$\sigma$ baseline max of 5134.98 ms).
- **Subtle `embedding_batch` staleness and drift**: Subtle document ages (~31–37 days) and centroid shifts (~0.028–0.031) overlapped with clean batches (clean doc ages reached 37.5 days and centroid shifts reached 0.0387).

**Why they were hardest**:
Under the scoring formula $\text{Score} = 100 \times (0.5 \cdot \text{TPR} - 0.3 \cdot \text{FPR} - 0.2 \cdot \min(\text{cost\_overage}, 1))$, false alarms carry a heavy penalty ($-0.3 \cdot \text{FPR}$). In a 200-event stream with 146 clean events, each false positive costs ~0.205 points. Lowering static thresholds to capture subtle variations causes dozens of false alarms across clean events, which rapidly decimates the net score.

In contrast, **deterministic and structural faults** were caught with 100% precision and recall:
- **`contract_checkpoint`**: Schema hash mismatches (`actual_schema_hash != 'sha256:5f2a'`), data type mutations (`actual_amount_type != 'float'`), and SLA freshness delays ($> 11.11$ min or $> \text{declared freshness}$) were 100% caught across all phases with zero false positives.
- **`lineage_run` structural breaks**: Orphan outputs (`actual_downstream_count == 0`) and missing upstream dependencies (omission of `raw.customers`) provided crisp binary signals.
- **`feature_materialization`**: Calibrating the mean shift threshold to $0.70\sigma$ (accounting for clean tail variance of ~0.47$\sigma$ vs. minimum true skew of $0.97\sigma$) achieved 100% TPR and 0% FPR across all phases.

---

### 2. What would you change about your cost/coverage tradeoff, if you had another pass?

1. **Stateful Sequential Tracking via `ctx.state`**:
   - Rather than relying solely on static baseline cutoffs, we would implement **online sequential change detection** (such as CUSUM or EWMA filter) persisted in `ctx.state`. Many subtle faults (e.g. repeated subtle null rate increases or persistent slight delays) become statistically distinguishable when evaluated as a temporal run rather than independent point-in-time samples.

2. **Phase-Adaptive Budget Metering**:
   - In the **private phase**, a budget of 320 credits for 200 events provided sufficient headroom for full inspection (costing 300 credits for 0.0 overage).
   - In the **public phase**, however, 160 events with a 220-credit budget meant full inspection cost 240 credits (0.0909 overage). While the TPR gain (+0.5 weight) outweighed the minor overage penalty ($-0.2 \times 0.0909 \approx -0.018$), in tighter budget scenarios we could introduce **tiered verification**: calling cheaper tools first (1.0 credit checks/lineage) and conditionally invoking expensive 2.0-credit AI infra tools based on metadata indicators or periodic sampling.
