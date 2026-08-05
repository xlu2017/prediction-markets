export function reconnectDelayMs(
  attempt: number,
  baseMs: number,
  maximumMs: number,
  random: () => number = Math.random,
): number {
  if (!Number.isSafeInteger(attempt) || attempt < 0) throw new Error("attempt must be nonnegative");
  if (baseMs <= 0 || maximumMs < baseMs) throw new Error("invalid reconnect delay bounds");
  const exponential = Math.min(maximumMs, baseMs * 2 ** Math.min(attempt, 30));
  const jitter = 0.8 + Math.min(1, Math.max(0, random())) * 0.4;
  return Math.min(maximumMs, Math.round(exponential * jitter));
}

export class StaleDetector {
  private lastObservationAt: number | null = null;
  private incidentOpen = false;

  public constructor(private readonly staleAfterMs: number) {
    if (staleAfterMs <= 0) throw new Error("staleAfterMs must be positive");
  }

  public observe(nowMs: number): void {
    this.lastObservationAt = nowMs;
    this.incidentOpen = false;
  }

  public check(nowMs: number): boolean {
    if (
      this.lastObservationAt === null ||
      this.incidentOpen ||
      nowMs - this.lastObservationAt <= this.staleAfterMs
    ) {
      return false;
    }
    this.incidentOpen = true;
    return true;
  }
}
