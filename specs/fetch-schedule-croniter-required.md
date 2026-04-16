# `is_fetch_due` should fail loudly when croniter is missing

## Problem

`dvx.run.dvc_files.is_fetch_due` silently returns `True` if `croniter`
isn't importable:

```python
try:
    from croniter import croniter
    cron = croniter(schedule, last)
    next_fire = cron.get_next(datetime)
    ...
    return now >= next_fire
except Exception:
    # If croniter not installed or invalid expression, treat as due
    return True
```

Since `croniter` is an *optional* extra (`dvx[cron]`) and isn't in the
default install, every project that uses a cron-expression `schedule`
(as opposed to a preset like `"daily"`) gets a silent schedule bypass:
**every `dvx run` treats the stage as due**.

Downstream symptom in the `nj-crashes` project: a refresh stage with
`schedule: "10 15 * * *"` fired on every dispatch — producing no-op
"Refresh NJSP data" commits hours before the actual cron time.

## Proposal

### 1. Fail loudly if `croniter` is missing

`croniter` stays opt-in (`dvx[cron]` extra). Projects using cron
expressions are expected to install it. Replace the bare
`except Exception: return True` with targeted handling that fails loudly
when croniter is missing but a cron expression is used:

```python
interval = _SCHEDULE_INTERVALS.get(schedule)
if interval is not None:
    return now >= last + interval  # Preset, no croniter needed

# Cron expression — croniter is required
try:
    from croniter import croniter
except ImportError as e:
    raise RuntimeError(
        f"Cron-expression schedule {schedule!r} requires croniter. "
        "Install `dvx[cron]` or add croniter to your project deps."
    ) from e

try:
    cron = croniter(schedule, last)
except (ValueError, KeyError) as e:
    raise ValueError(f"Invalid cron expression in schedule: {schedule!r}") from e

next_fire = cron.get_next(datetime)
if next_fire.tzinfo is None:
    next_fire = next_fire.replace(tzinfo=timezone.utc)
return now >= next_fire
```

Key changes:
- Preset schedules (`"daily"`, `"hourly"`, `"weekly"`) never hit the
  croniter path — they work without croniter.
- Cron expressions raise `RuntimeError` if croniter is missing (clear
  error message pointing at the fix).
- Invalid cron expressions raise `ValueError` (currently they silently
  fall back to "always due").

### 2. Document

Add a `schedule` section to docs/README covering:
- Preset names vs cron expressions
- Cron expressions require `dvx[cron]` extra
- Examples for common cases (daily at a specific time, multi-fire
  windows, etc.)

## Test plan

Add to `tests/test_dvc_files.py`:

1. Preset schedule + no croniter → works (mock out `croniter` import).
2. Cron schedule + no croniter → `RuntimeError` with clear message.
3. Invalid cron schedule → `ValueError` with the bad expression in msg.
4. Valid cron schedule → behaves correctly before/after next_fire.
5. Mixed case sanity: `"daily"`, `"0 15 * * *"`, `"*/10 15-16 * * *"`.

## Out of scope

- Adding a `freshness_check` hook (for "poll the source before deciding
  to run"). Worth a separate spec.
- Expanding `schedule` to support executable/script values.
