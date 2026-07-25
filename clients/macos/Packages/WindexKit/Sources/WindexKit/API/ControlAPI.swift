import Foundation

/// Writes for the operational surface — starting and stopping things.
///
/// Two conventions worth knowing before wiring a button to any of these:
///
/// * **Most of these are DESIRED-STATE, not commands.** Turning a loop off keeps
///   it off: `windex up` and the watchdog both honour the flag, so it will not
///   come back on its own. The UI should reflect a persisted intent, not a
///   fire-and-forget action.
/// * **The long ones return immediately.** `systemUp`, `restartLoops` and
///   `refresh` spawn a detached process and hand back a pid. A 200 means
///   *accepted*, not *finished* — watch `activity()` or the event stream for
///   completion rather than treating the response as the end of the operation.
extension WindexClient {

    // MARK: - Jobs

    /// Start a job with typed arguments.
    ///
    /// `arguments` are validated against the job's own `Param` schema (from
    /// ``jobs()``), which uses `enforce: "reject"` — an out-of-range value is a
    /// 422, never silently clamped. Throws `.http(status: 409)` when the job is
    /// already running.
    @discardableResult
    public func startJob(_ name: String,
                         arguments: [String: JSONValue] = [:]) async throws -> ActionResult {
        try await send("POST", "/v1/jobs/\(escape(name))/start", surface: .admin,
                       body: arguments, as: ActionResult.self)
    }

    @discardableResult
    public func stopJob(_ name: String) async throws -> ActionResult {
        try await send("POST", "/v1/jobs/\(escape(name))/stop", surface: .admin,
                       as: ActionResult.self)
    }

    // MARK: - Indexing control

    /// Pause or resume the whole pipeline.
    ///
    /// Paused is a normal operator state, not a fault — `DESIGN.md` is explicit
    /// that it must not be styled as an error.
    @discardableResult
    public func setIndexing(_ action: IndexingAction) async throws -> ControlState {
        try await send("POST", "/v1/control/\(action.rawValue)", surface: .admin,
                       as: ControlState.self)
    }

    public enum IndexingAction: String, Sendable, CaseIterable {
        case start, pause
    }

    /// Embedding throughput profile. Read by embedders at each pass, so it takes
    /// effect within about a minute without restarting anything.
    @discardableResult
    public func setThrottle(_ profile: ThrottleProfile) async throws -> ThrottleState {
        try await send("POST", "/v1/throttle/\(profile.rawValue)", surface: .admin,
                       as: ThrottleState.self)
    }

    public enum ThrottleProfile: String, Sendable, CaseIterable {
        case polite, full
        /// Revert to whatever the environment configured.
        case env
    }

    // MARK: - Loops

    /// Turn one source's embed loop on or off (desired state).
    @discardableResult
    public func setLoop(source: String, enabled: Bool) async throws -> ActionResult {
        try await send("POST", "/v1/loops/\(escape(source))", surface: .admin,
                       body: ["enabled": JSONValue.bool(enabled)], as: ActionResult.self)
    }

    /// Turn a source's auto-ingest on or off (desired state).
    ///
    /// Distinct from the embed loop: off means the refresh sweep and the
    /// scheduler skip *fetching* it, while a manual "check now" still runs and
    /// anything already staged still embeds.
    @discardableResult
    public func setIngest(source: String, enabled: Bool) async throws -> ActionResult {
        try await send("POST", "/v1/ingest/\(escape(source))", surface: .admin,
                       body: ["enabled": JSONValue.bool(enabled)], as: ActionResult.self)
    }

    /// Bulk on/off for every embed loop — "start all" / "stop all".
    @discardableResult
    public func setAllLoops(enabled: Bool) async throws -> ActionResult {
        try await send("POST", "/v1/system/loops", surface: .admin,
                       body: ["enabled": JSONValue.bool(enabled)], as: ActionResult.self)
    }

    // MARK: - System

    /// Reconcile to desired state: start every enabled loop that is down.
    /// Detached — returns a pid, not a completion.
    @discardableResult
    public func systemUp() async throws -> ActionResult {
        try await send("POST", "/v1/system/up", surface: .admin, as: ActionResult.self)
    }

    /// Bounce the loops: stop every one, then bring the enabled ones back.
    @discardableResult
    public func restartLoops() async throws -> ActionResult {
        try await send("POST", "/v1/system/restart", surface: .admin,
                       as: ActionResult.self)
    }

    /// Kick off a freshness sweep, optionally scoped to some sources.
    /// Detached; its own guard skips if a sweep is already running.
    @discardableResult
    public func refresh(sources: [String] = []) async throws -> ActionResult {
        let body: [String: JSONValue] = sources.isEmpty
            ? [:]
            : ["sources": .array(sources.map(JSONValue.string))]
        return try await send("POST", "/v1/system/refresh", surface: .admin,
                              body: body, as: ActionResult.self)
    }

    /// Recompute the cached corpus statistics.
    @discardableResult
    public func refreshStats() async throws -> ActionResult {
        try await send("POST", "/v1/system/refresh-stats", surface: .admin,
                       as: ActionResult.self)
    }

    // MARK: - Schedule

    /// Create or edit a schedule entry.
    ///
    /// Editing preserves fields left nil; creating requires `kind` and `target`.
    /// A `weekday` makes it weekly, otherwise it is daily. 422 on an invalid
    /// entry.
    @discardableResult
    public func upsertSchedule(
        name: String,
        kind: String? = nil,
        target: String? = nil,
        hour: Int? = nil,
        minute: Int? = nil,
        weekday: Int? = nil,
        enabled: Bool? = nil
    ) async throws -> ScheduleEntry {
        var body: [String: JSONValue] = [:]
        if let kind { body["kind"] = .string(kind) }
        if let target { body["target"] = .string(target) }
        if let hour { body["hour"] = .int(hour) }
        if let minute { body["minute"] = .int(minute) }
        if let weekday { body["weekday"] = .int(weekday) }
        if let enabled { body["enabled"] = .bool(enabled) }
        return try await send("PUT", "/v1/schedule/\(escape(name))", surface: .admin,
                              body: body, as: ScheduleEntry.self)
    }

    @discardableResult
    public func deleteSchedule(name: String) async throws -> ActionResult {
        try await send("DELETE", "/v1/schedule/\(escape(name))", surface: .admin,
                       as: ActionResult.self)
    }

    /// Run a scheduled entry now, off-cycle. Does not affect its schedule.
    @discardableResult
    public func runScheduleNow(name: String) async throws -> ActionResult {
        try await send("POST", "/v1/schedule/\(escape(name))/run", surface: .admin,
                       as: ActionResult.self)
    }
}
