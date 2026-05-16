"""
Heartbeat 2.0: Autonomic + Cognitive layer for Hermes Gateway.

Design principles:
- No direct GatewayRunner reference; only snapshot callbacks.
- All state written to files; never mutates GatewayRunner state.
- Fail-safe: errors are logged, never raised into the main loop.
- Lightweight: all work is async/non-blocking or offloaded to threads.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_HERMES_HOME = Path(os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes")))
_STATE_PATH = _HERMES_HOME / "heartbeat_state.json"
_DECISION_LOG_PATH = _HERMES_HOME / "heartbeat_decisions.jsonl"
_CONFIG_KEY = "heartbeat"

# Intervals
_AUTONOMIC_INTERVAL_S = 30
_COGNITIVE_INTERVAL_S = 300
_COGNITIVE_ACTION_TIMEOUT_S = 300

# Thresholds
_STUCK_THRESHOLD_MIN = 30
_STUCK_RECOVERY_MIN = 45
_CACHE_BLOAT_THRESHOLD = 64
_IDLE_SHORT_MIN = 10
_IDLE_LONG_MIN = 30
_DECISION_HISTORY_MAX = 50
_DECISION_LOG_MAX_LINES = 1000

# Actions
_ACTIONS: list[str] = ["WORK", "EVOLVE", "REST", "CONNECT", "REPORT"]
_ACTION_PRIORITY = {a: i for i, a in enumerate(_ACTIONS)}

# Score weights
_PENDING_BOOST_WORK = 10
_PENDING_BOOST_EVOLVE = 2
_PENDING_PENALTY_REST = 5
_PENDING_PENALTY_REPORT = 10

_CACHE_BLOAT_BOOST_WORK = 2
_CACHE_BLOAT_BOOST_EVOLVE = 3
_CACHE_BLOAT_BOOST_REST = 10
_CACHE_BLOAT_PENALTY_REPORT = 5

_FAILED_BOOST_WORK = 4
_FAILED_BOOST_EVOLVE = 10
_FAILED_BOOST_REST = 2
_FAILED_BOOST_CONNECT = 5
_FAILED_PENALTY_REPORT = 5

_IDLE_BOOST_WORK = 5
_IDLE_BOOST_EVOLVE = 5
_IDLE_BOOST_REST = 3
_IDLE_BOOST_CONNECT = 2
_IDLE_BOOST_REPORT = 1

_LONG_IDLE_BOOST_EVOLVE = 2
_LONG_IDLE_BOOST_REST = 5
_LONG_IDLE_BOOST_CONNECT = 8
_LONG_IDLE_BOOST_REPORT = 3
_LONG_IDLE_BOOST_WORK = 1

_REPETITION_PENALTY = 3


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True, slots=True)
class GatewaySnapshot:
    """Lightweight read-only snapshot of gateway state."""

    running: bool
    uptime_seconds: float
    active_sessions: int
    running_agents: Dict[str, float]
    agent_cache_size: int
    agent_cache_keys: List[str]
    failed_platforms: List[str]
    pending_approvals: int
    queued_events: int
    provider_health: Dict[str, Any]


@dataclasses.dataclass(frozen=True, slots=True)
class Decision:
    action: str
    scores: Dict[str, float]
    reason: str
    timestamp: float


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    try:
        from hermes_cli.config import load_config as _load_full_config

        return _load_full_config().get(_CONFIG_KEY, {})
    except Exception:
        return {}


def _cfg_bool(cfg: dict, key: str, default: bool = False) -> bool:
    return cfg.get(key, default)


def _cfg_int(cfg: dict, key: str, default: int) -> int:
    try:
        return int(cfg.get(key, default))
    except (ValueError, TypeError):
        return default


def _cfg_float(cfg: dict, key: str, default: float) -> float:
    try:
        return float(cfg.get(key, default))
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def _write_state(state: dict) -> None:
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")
        tmp.replace(_STATE_PATH)
    except Exception:
        logger.debug("heartbeat state write failed", exc_info=True)


def _append_decision(decision: Decision) -> None:
    try:
        _DECISION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            {
                "ts": decision.timestamp,
                "action": decision.action,
                "scores": decision.scores,
                "reason": decision.reason,
            },
            default=str,
        )
        with _DECISION_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        logger.debug("decision log append failed", exc_info=True)


# ---------------------------------------------------------------------------
# Autonomic layer
# ---------------------------------------------------------------------------

class AutonomicLoop:
    """Runs at a fixed short interval; never makes decisions, only senses & records."""

    def __init__(self, get_snapshot: Callable[[], GatewaySnapshot], cost_summary_fn: Callable[[], Optional[dict]] = None):
        self._get_snapshot = get_snapshot
        self._cost_summary_fn = cost_summary_fn
        cfg = _load_config()
        self._interval = _cfg_float(cfg, "autonomic_interval_seconds", _AUTONOMIC_INTERVAL_S)
        self._stuck_threshold = _cfg_int(cfg, "stuck_threshold_minutes", _STUCK_THRESHOLD_MIN)
        self._stuck_recovery = _cfg_int(cfg, "stuck_recovery_minutes", _STUCK_RECOVERY_MIN)
        self._task: Optional[asyncio.Task] = None
        self._stuck_notified: set[str] = set()
        self._start_time = time.time()
        self._tick_count = 0

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop())
        logger.info("AutonomicLoop started (interval=%.0fs)", self._interval)

    def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Autonomic tick error", exc_info=True)
            await asyncio.sleep(self._interval)

    async def _tick(self) -> None:
        snap = self._get_snapshot()
        if not snap.running:
            return

        self._tick_count += 1
        now = time.time()
        stuck = self._detect_stuck(snap, now)
        warmth = self._compute_warmth(snap)

        state = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": round(now - self._start_time, 1),
            "active_sessions": snap.active_sessions,
            "running_agents_count": len(snap.running_agents),
            "agent_cache_size": snap.agent_cache_size,
            "failed_platforms": snap.failed_platforms,
            "pending_approvals": snap.pending_approvals,
            "queued_events": snap.queued_events,
            "stuck_sessions": stuck,
            "warmth_actions": warmth,
            "provider_health": snap.provider_health,
            "tick_count": self._tick_count,
        }

        # Inject cost summary if callback provided
        if self._cost_summary_fn:
            try:
                cost = self._cost_summary_fn()
                if cost:
                    state["cost_24h"] = {
                        "session_count": cost.get("session_count", 0),
                        "total_input_tokens": cost.get("total_input_tokens", 0),
                        "total_output_tokens": cost.get("total_output_tokens", 0),
                        "total_cache_read_tokens": cost.get("total_cache_read_tokens", 0),
                        "total_cache_write_tokens": cost.get("total_cache_write_tokens", 0),
                        "estimated_cost_usd": cost.get("total_estimated_cost_usd", 0),
                    }
            except Exception:
                state["cost_24h"] = None  # graceful degradation

        _write_state(state)

        for sess, elapsed_min in stuck:
            if elapsed_min >= self._stuck_recovery and sess not in self._stuck_notified:
                self._stuck_notified.add(sess)
                await self._attempt_interrupt(sess)

    def _detect_stuck(self, snap: GatewaySnapshot, now: float) -> List[tuple[str, float]]:
        stuck: List[tuple[str, float]] = []
        for sess, start_ts in snap.running_agents.items():
            elapsed_min = (now - start_ts) / 60.0
            if elapsed_min >= self._stuck_threshold:
                stuck.append((sess, round(elapsed_min, 1)))
        return stuck

    def _compute_warmth(self, snap: GatewaySnapshot) -> List[str]:
        """Recommend session keys to touch for warmth retention."""
        return [key for key in snap.agent_cache_keys if ":dm:" in key or ":telegram:" in key]

    async def _attempt_interrupt(self, session_key: str) -> None:
        logger.warning("Autonomic: attempting interrupt for stuck session %s", session_key)

        def _interrupt() -> None:
            try:
                from agent.interrupt import send_interrupt

                send_interrupt(session_key)
            except Exception:
                logger.debug("Interrupt send failed for %s", session_key, exc_info=True)

        threading.Thread(
            target=_interrupt, daemon=True, name=f"hb-interrupt-{session_key[:20]}"
        ).start()


# ---------------------------------------------------------------------------
# Cognitive layer
# ---------------------------------------------------------------------------

class CognitiveLoop:
    """Runs when the gateway is idle; makes decisions and acts on them."""

    def __init__(self, get_snapshot: Callable[[], GatewaySnapshot]):
        self._get_snapshot = get_snapshot
        cfg = _load_config()
        self._interval = _cfg_float(cfg, "cognitive_interval_seconds", _COGNITIVE_INTERVAL_S)
        self._action_timeout = _cfg_float(cfg, "cognitive_action_timeout_seconds", _COGNITIVE_ACTION_TIMEOUT_S)
        self._enabled = _cfg_bool(cfg, "cognitive_enabled", True)
        self._task: Optional[asyncio.Task] = None
        self._last_action: Optional[str] = None
        self._last_action_ts = 0.0
        self._decision_history: List[Decision] = []

    def start(self) -> None:
        if not self._enabled:
            logger.info("CognitiveLoop disabled via config")
            return
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop())
        logger.info("CognitiveLoop started (interval=%.0fs)", self._interval)

    def stop(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
        self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("Cognitive tick error", exc_info=True)
            await asyncio.sleep(self._interval)

    async def _tick(self) -> None:
        snap = self._get_snapshot()
        if not snap.running or snap.running_agents:
            return

        decision = self._decide(snap)
        _append_decision(decision)
        self._decision_history.append(decision)
        if len(self._decision_history) > _DECISION_HISTORY_MAX:
            self._decision_history.pop(0)

        self._last_action = decision.action
        self._last_action_ts = time.time()

        await self._act(decision, snap)

    def _decide(self, snap: GatewaySnapshot) -> Decision:
        scores: Dict[str, float] = {a: 0.0 for a in _ACTIONS}
        now = time.time()
        pending = snap.queued_events + snap.pending_approvals
        idle_min = (now - self._last_action_ts) / 60.0

        if pending > 0:
            scores["WORK"] += _PENDING_BOOST_WORK
            scores["EVOLVE"] += _PENDING_BOOST_EVOLVE
            scores["REST"] -= _PENDING_PENALTY_REST
            scores["REPORT"] -= _PENDING_PENALTY_REPORT

        if snap.agent_cache_size > _CACHE_BLOAT_THRESHOLD:
            scores["WORK"] += _CACHE_BLOAT_BOOST_WORK
            scores["EVOLVE"] += _CACHE_BLOAT_BOOST_EVOLVE
            scores["REST"] += _CACHE_BLOAT_BOOST_REST
            scores["REPORT"] -= _CACHE_BLOAT_PENALTY_REPORT

        if snap.failed_platforms:
            scores["WORK"] += _FAILED_BOOST_WORK
            scores["EVOLVE"] += _FAILED_BOOST_EVOLVE
            scores["REST"] += _FAILED_BOOST_REST
            scores["CONNECT"] += _FAILED_BOOST_CONNECT
            scores["REPORT"] -= _FAILED_PENALTY_REPORT

        if idle_min > _IDLE_SHORT_MIN:
            scores["WORK"] += _IDLE_BOOST_WORK
            scores["EVOLVE"] += _IDLE_BOOST_EVOLVE
            scores["REST"] += _IDLE_BOOST_REST
            scores["CONNECT"] += _IDLE_BOOST_CONNECT
            scores["REPORT"] += _IDLE_BOOST_REPORT

        if not snap.running_agents and idle_min > _IDLE_LONG_MIN:
            scores["WORK"] += _LONG_IDLE_BOOST_WORK
            scores["EVOLVE"] += _LONG_IDLE_BOOST_EVOLVE
            scores["REST"] += _LONG_IDLE_BOOST_REST
            scores["CONNECT"] += _LONG_IDLE_BOOST_CONNECT
            scores["REPORT"] += _LONG_IDLE_BOOST_REPORT

        if self._last_action:
            scores[self._last_action] -= _REPETITION_PENALTY

        best = max(scores, key=lambda a: (scores[a], -_ACTION_PRIORITY[a]))
        reason = f"pending={pending}, cache={snap.agent_cache_size}, failed={snap.failed_platforms}, idle={idle_min:.0f}min"
        return Decision(action=best, scores=scores, reason=reason, timestamp=now)

    async def _act(self, decision: Decision, snap: GatewaySnapshot) -> None:
        logger.info("Cognitive: decided %s (%s)", decision.action, decision.reason)
        try:
            await asyncio.wait_for(
                self._execute_action(decision, snap), timeout=self._action_timeout
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Cognitive action %s timed out after %.0fs",
                decision.action,
                self._action_timeout,
            )
        except Exception:
            logger.debug("Cognitive action %s failed", decision.action, exc_info=True)

    async def _execute_action(self, decision: Decision, snap: GatewaySnapshot) -> None:
        handlers = {
            "WORK": self._act_work,
            "EVOLVE": self._act_evolve,
            "REST": self._act_rest,
            "CONNECT": self._act_connect,
            "REPORT": self._act_report,
        }
        handler = handlers.get(decision.action)
        if handler:
            await handler(snap)

    async def _act_work(self, _snap: GatewaySnapshot) -> None:
        def _work() -> None:
            try:
                from cron.scheduler import tick as cron_tick

                cron_tick(verbose=False)
            except Exception:
                logger.debug("WORK: cron_tick failed", exc_info=True)

        await asyncio.to_thread(_work)

    async def _act_evolve(self, _snap: GatewaySnapshot) -> None:
        def _evolve() -> None:
            try:
                if not _DECISION_LOG_PATH.exists():
                    return
                lines = _DECISION_LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
                if len(lines) <= _DECISION_LOG_MAX_LINES:
                    return
                _DECISION_LOG_PATH.write_text(
                    "\n".join(lines[-_DECISION_LOG_MAX_LINES:]) + "\n", encoding="utf-8"
                )
                logger.info("EVOLVE: pruned decision log to %d entries", _DECISION_LOG_MAX_LINES)
            except Exception:
                logger.debug("EVOLVE: log pruning failed", exc_info=True)

        await asyncio.to_thread(_evolve)

    async def _act_rest(self, snap: GatewaySnapshot) -> None:
        _write_state({"rest_request": True, "agent_cache_size": snap.agent_cache_size})
        logger.info("REST: requested cache relief (cache_size=%d)", snap.agent_cache_size)

    async def _act_connect(self, _snap: GatewaySnapshot) -> None:
        logger.info("CONNECT: health summary available in %s", _STATE_PATH)

    async def _act_report(self, _snap: GatewaySnapshot) -> None:
        logger.debug("REPORT: state already current")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class HeartbeatV2:
    """Main controller: owns Autonomic + Cognitive loops."""

    def __init__(
        self,
        snapshot_fn: Callable[[], GatewaySnapshot],
        config: Any = None,
        cost_summary_fn: Callable[[], Optional[dict]] = None,
    ):
        self._snapshot_fn = snapshot_fn
        self._config = config
        self._shutdown_event = asyncio.Event()
        self.autonomic = AutonomicLoop(snapshot_fn, cost_summary_fn=cost_summary_fn)
        self.cognitive = CognitiveLoop(snapshot_fn)

    def start(self) -> None:
        self.autonomic.start()
        self.cognitive.start()

    def stop(self) -> None:
        self.autonomic.stop()
        self.cognitive.stop()
        self._shutdown_event.set()

    async def loop(self) -> None:
        """Coroutine suitable for ``asyncio.create_task()``.

        Runs ``start()`` and waits until ``stop()`` is called.
        """
        self.start()
        await self._shutdown_event.wait()

    def is_under_backpressure(self) -> bool:
        """Return True if the gateway is busy and cron jobs should be throttled."""
        try:
            snap = self._snapshot_fn()
        except Exception:
            return False
        # Backpressure heuristics: active agents, queued work, or high cache
        if snap.running_agents:
            return True
        if snap.queued_events > 0:
            return True
        if snap.agent_cache_size > 128:
            return True
        return False


def build_heartbeat_snapshot(runner: Any) -> GatewaySnapshot:
    """Build a snapshot from a GatewayRunner without holding a strong ref."""
    now = time.time()

    def _safe_int(obj: Any, attr: str, default: int = 0) -> int:
        try:
            val = getattr(obj, attr, None)
            return len(val) if val is not None else default
        except Exception:
            return default

    def _safe_dict(obj: Any, attr: str) -> dict:
        try:
            val = getattr(obj, attr, None)
            return dict(val) if val is not None else {}
        except Exception:
            return {}

    def _safe_list(obj: Any, attr: str) -> list:
        try:
            val = getattr(obj, attr, None)
            return list(val) if val is not None else []
        except Exception:
            return []

    running = bool(getattr(runner, "_running", False))
    active_sessions = _safe_int(runner, "session_store")
    running_agents = _safe_dict(runner, "_running_agents_ts")
    cache = getattr(runner, "_agent_cache", None)
    cache_size = len(cache) if cache is not None else 0
    cache_keys = list(cache.keys()) if cache is not None else []

    failed_raw = _safe_list(runner, "_failed_platforms")
    failed_platforms = [
        p.value if hasattr(p, "value") else str(p) for p in failed_raw
    ]

    pending_approvals = _safe_int(runner, "_pending_approvals")

    queued = 0
    try:
        queued_events = getattr(runner, "_queued_events", {})
        queued = sum(len(v) for v in queued_events.values())
    except Exception:
        pass

    uptime = 0.0
    try:
        boot = getattr(runner, "_boot_wall_time", 0)
        uptime = now - boot if boot else 0.0
    except Exception:
        pass

    return GatewaySnapshot(
        running=running,
        uptime_seconds=uptime,
        active_sessions=active_sessions,
        running_agents=running_agents,
        agent_cache_size=cache_size,
        agent_cache_keys=cache_keys,
        failed_platforms=failed_platforms,
        pending_approvals=pending_approvals,
        queued_events=queued,
        provider_health={},
    )


# Backward-compat alias
make_snapshot = build_heartbeat_snapshot
