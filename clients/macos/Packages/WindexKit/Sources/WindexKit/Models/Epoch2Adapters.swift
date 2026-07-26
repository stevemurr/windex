import Foundation

private func date(_ raw: String) -> Date {
    (try? roundTrip(raw, as: FlexibleDate.self).date) ?? .distantPast
}

private func object<T: Encodable>(_ value: T) throws -> [String: JSONValue] {
    guard case .object(let result) = try roundTrip(value, as: JSONValue.self) else {
        return [:]
    }
    return result
}

extension PipelineWire {
    public func summary(deploymentCount: Int = 0) -> PipelineSummary {
        PipelineSummary(
            name: name,
            title: title,
            description: description,
            headVersion: version ?? 0,
            headHash: specHash ?? "",
            builtin: builtin,
            archived: archivedAt != nil,
            deploymentCount: deploymentCount
        )
    }
}

extension PipelineRevisionWire {
    public func revision(title: String = "", description: String = "") throws -> PipelineRevision {
        let decoded = try spec.additionalProperties.decode(PipelineSpec.self)
        let decorated = PipelineSpec(
            schema: decoded.schema,
            title: title,
            description: description,
            parameters: decoded.parameters,
            flows: decoded.flows,
            refreshFlows: decoded.refreshFlows
        )
        return PipelineRevision(
            reference: PipelineRevisionReference(
                pipeline: pipelineName,
                version: version,
                specHash: specHash
            ),
            parentVersion: parentRevisionId,
            spec: decorated,
            registryVersion: registryVersion,
            author: author,
            note: note
        )
    }
}

extension PipelineLayoutWire {
    public func flowLayout(pipeline: String, version: Int) throws -> PipelineFlowLayout {
        let payload = try layout.additionalProperties.decode(
            [String: JSONValue].self
        )
        let positions: [String: PipelineNodePosition]
        if let raw = payload["nodes"] {
            positions = (try? roundTrip(raw, as: [String: PipelineNodePosition].self)) ?? [:]
        } else {
            positions = [:]
        }
        let groups = payload["groups"].flatMap {
            try? roundTrip($0, as: [String: [String]].self)
        } ?? [:]
        let annotations = payload["annotations"].flatMap {
            try? roundTrip($0, as: [String: String].self)
        } ?? [:]
        return PipelineFlowLayout(
            pipeline: pipeline,
            version: version,
            flow: flow,
            positions: positions,
            groups: groups,
            annotations: annotations,
            etag: etag
        )
    }
}

extension SourceWire {
    public func deployment(status: SourceStatus = .init()) throws -> SourceDeployment {
        let values = try self.values.additionalProperties.decode([String: JSONValue].self)
        let originValues = try origin.additionalProperties.decode([String: JSONValue].self)
        let originText = originValues["url"]?.stringValue
            ?? originValues["ingress"]?.stringValue
            ?? originValues["type"]?.stringValue
            ?? "configured"
        let ingressValues = try ingress?.additionalProperties.decode(
            [String: JSONValue].self
        )
        let sourceIngress = ingressValues.map {
            SourceIngress(
                path: $0["url"]?.stringValue ?? "",
                authenticationRequired: $0["authentication_required"]?.boolValue ?? false,
                maxDocuments: $0["max_documents"]?.intValue ?? 0,
                maxTextBytes: $0["max_text_bytes"]?.intValue ?? 0,
                modes: $0["modes"]?.arrayValue?.compactMap(\.stringValue) ?? []
            )
        }
        return SourceDeployment(
            name: name,
            title: title,
            description: description,
            origin: originText,
            pipeline: .init(pipeline: pipelineName, version: pipelineVersion,
                            specHash: pipelineHash),
            search: .init(searchName: searchName, idPrefix: idPrefix,
                          collectionKey: collectionKey, searchProfile: searchProfile,
                          includeInAll: includeInAll),
            stateNamespace: stateNamespace,
            enabled: enabled,
            paused: paused,
            archived: archivedAt != nil,
            generation: generation,
            ingress: sourceIngress,
            configuration: .init(
                configuredValues: values,
                effectiveValues: values,
                missingRequired: ready ? [] : ["configuration"],
                valuesHash: valuesHash
            ),
            status: status
        )
    }
}

extension RunWire {
    public func summary() throws -> SourceRunSummary {
        let payload = try progress.additionalProperties.decode([String: JSONValue].self)
        let fraction = payload["fraction"]?.doubleValue
            ?? payload["progress"]?.doubleValue
            ?? {
                guard let completed = payload["completed"]?.doubleValue,
                      let total = payload["total"]?.doubleValue, total > 0 else { return nil }
                return completed / total
            }()
        return SourceRunSummary(
            id: id,
            sourceName: sourceName,
            pipeline: .init(pipeline: pipelineName, version: pipelineVersion,
                            specHash: pipelineHash),
            state: SourceActivityState(rawValue: state) ?? .idle,
            progress: fraction,
            flow: flowName,
            queuedAt: queuedAt,
            finishedAt: finishedAt,
            error: error
        )
    }
}

extension RunOutputWire {
    public func decodedValue() throws -> JSONValue {
        try roundTrip(value, as: JSONValue.self)
    }

    public var artifactID: String? {
        (try? decodedValue().objectValue?["artifact_id"]?.stringValue) ?? nil
    }
}

extension Components.Schemas.OperationalEventModel {
    public func operationalEvent() throws -> OperationalEvent {
        OperationalEvent(
            sequence: Int64(seq),
            timestamp: date(ts),
            level: level == "warn"
                ? .warning
                : (OperationalEventLevel(rawValue: level) ?? .info),
            component: component,
            sourceName: sourceName,
            pipelineName: pipelineName,
            pipelineVersion: pipelineVersion,
            runID: runId,
            taskID: taskId,
            node: node,
            module: module,
            event: event,
            message: message,
            data: try data.additionalProperties.decode([String: JSONValue].self)
        )
    }
}

extension SourceSettingsWire {
    public func settingsScope() throws -> SettingsScope {
        SettingsScope(
            scope: source,
            fields: try fields.map { try roundTrip($0, as: SettingsField.self) }
        )
    }
}

extension GlobalSettingsWire {
    public func settingsScope() throws -> SettingsScope {
        let decoded = try values.additionalProperties.decode([String: JSONValue].self)
        let fields: [SettingsField] = try decoded.keys.sorted().map { key in
            let value = decoded[key] ?? .null
            let kind: String
            let editor: String
            switch value {
            case .bool: kind = "bool"; editor = "checkbox"
            case .int: kind = "int"; editor = "number"
            case .double: kind = "float"; editor = "number"
            case .array: kind = "url_list"; editor = "json"
            case .object: kind = "str"; editor = "json"
            default: kind = "str"; editor = "textfield"
            }
            let descriptor: JSONValue = .object([
                "key": .string(key),
                "kind": .string(kind),
                "title": .string(key.replacingOccurrences(of: "_", with: " ").capitalized),
                "editor": .string(editor),
                "value": value,
                "origin": .string("operator")
            ])
            return try roundTrip(descriptor, as: SettingsField.self)
        }
        return SettingsScope(scope: scope, fields: fields)
    }
}

extension SourceStatusWire {
    public func status(
        runs: [SourceRunSummary],
        nextTrigger: String? = nil
    ) throws -> SourceStatus {
        let raw = try object(self)
        let documents = raw["documents"]?.objectValue ?? [:]
        func count(_ key: String) -> Int {
            documents[key]?.intValue
                ?? documents[key]?.objectValue?["count"]?.intValue
                ?? 0
        }
        let currentID = raw["current_run"]?.objectValue?["id"]?.intValue
        let latestID = raw["latest_run"]?.objectValue?["id"]?.intValue
        let current = currentID.flatMap { id in runs.first { $0.id == id } }
        let latest = latestID.flatMap { id in runs.first { $0.id == id } }
        let activity: SourceActivityState
        if paused { activity = .paused }
        else if current?.state == .running { activity = .running }
        else if current?.state == .queued { activity = .queued }
        else if recentError != nil { activity = .failed }
        else { activity = .idle }
        return SourceStatus(
            activity: activity,
            counts: .init(
                staged: count("staged"),
                pendingEmbedding: count("embedding"),
                searchable: count("searchable"),
                failed: count("failed"),
                asOf: documents["searchable"]?.objectValue?["as_of"]?.stringValue
            ),
            currentRun: current,
            latestRun: latest,
            nextTrigger: nextTrigger,
            recentError: recentError
        )
    }
}

extension OverviewWire {
    public func snapshot(sourceDeployments: [SourceDeployment],
                         recentFailures: [OperationalEvent] = []) throws -> OverviewSnapshot {
        let raw = try object(self)
        let health = raw["health"]?.objectValue ?? [:]
        let runProjection = raw["runs"]?.objectValue ?? [:]
        let runCounts = runProjection["counts"]?.objectValue ?? [:]
        let totals = raw["totals"]?.objectValue ?? [:]
        let workers = raw["workers"]?.objectValue ?? [:]
        let schedules = Dictionary(uniqueKeysWithValues:
            (raw["schedules"]?.arrayValue ?? []).compactMap { row
                -> (String, String?)? in
                guard let value = row.objectValue,
                      let source = value["source"]?.stringValue else { return nil }
                return (source, value["next_trigger"]?.stringValue)
            }
        )
        let sourceRows = raw["sources"]?.arrayValue ?? []
        let sourceMetadata: [String: [String: JSONValue]] = Dictionary(
            uniqueKeysWithValues: sourceRows.compactMap { row
                -> (String, [String: JSONValue])? in
            guard let object = row.objectValue,
                  let name = object["name"]?.stringValue
                    ?? object["source"]?.stringValue else { return nil }
            return (name, object)
        })
        return OverviewSnapshot(
            revision: Int64(revision),
            generatedAt: date(asOf),
            documents: totals["documents"]?.intValue ?? 0,
            searchable: totals["searchable"]?.intValue ?? 0,
            vectors: totals["vectors"]?.intValue,
            indexedLastHour: totals["indexed_last_hour"]?.intValue ?? 0,
            runs: .init(
                running: runCounts["running"]?.intValue ?? 0,
                queued: runCounts["queued"]?.intValue ?? 0,
                blocked: runCounts["blocked"]?.intValue ?? 0,
                failed: runCounts["failed"]?.intValue ?? 0,
                succeeded: runCounts["succeeded"]?.intValue ?? 0,
                cancelled: runCounts["cancelled"]?.intValue ?? 0
            ),
            sources: sourceDeployments.map { source in
                let metadata = sourceMetadata[source.name]
                return OverviewSourceStatus(
                    source: source,
                    documents: metadata?["documents"]?.intValue ?? 0,
                    searchable: metadata?["searchable"]?.intValue ?? 0,
                    lastIndexedAt: metadata?["last_indexed_at"]?.stringValue,
                    nextTrigger: schedules[source.name] ?? nil
                )
            },
            services: health.compactMap { name, value in
                guard ["service", "postgres", "vector", "storage"].contains(name)
                else { return nil }
                let status = value.stringValue ?? "unknown"
                return OverviewServiceStatus(
                    name: name,
                    available: status == "ok",
                    detail: status == "ok" ? nil : health["\(name)_error"]?.stringValue
                )
            }.sorted { $0.name < $1.name },
            workerLanes: (workers["lanes"]?.objectValue ?? [:]).map { name, value in
                OverviewWorkerLane(
                    name: name,
                    states: (value.objectValue ?? [:]).mapValues { $0.intValue ?? 0 }
                )
            }.sorted { $0.name < $1.name },
            blockedPreconditions: (workers["blocked_preconditions"]?.arrayValue ?? [])
                .compactMap { value in
                    guard let item = value.objectValue else { return nil }
                    return OverviewBlockedPrecondition(
                        preconditions: item["preconditions"]?.arrayValue?
                            .compactMap(\.stringValue) ?? [],
                        reason: item["reason"]?.stringValue,
                        tasks: item["tasks"]?.intValue ?? 0
                    )
                },
            activeRuns: Self.runStatuses(runProjection["active"]?.arrayValue ?? []),
            recentRuns: Self.runStatuses(runProjection["recent"]?.arrayValue ?? []),
            recentDocuments: (raw["recent_documents"]?.arrayValue ?? []).compactMap {
                value in
                guard let item = value.objectValue,
                      let id = item["id"]?.stringValue,
                      let source = item["source"]?.stringValue,
                      let indexedAt = item["indexed_at"]?.stringValue else { return nil }
                return OverviewRecentDocument(
                    id: id,
                    source: source,
                    title: item["title"]?.stringValue ?? id,
                    indexedAt: indexedAt
                )
            },
            recentFailures: recentFailures
        )
    }

    private static func runStatuses(_ values: [JSONValue]) -> [OverviewRunStatus] {
        values.compactMap { value in
            guard let item = value.objectValue,
                  let id = item["id"]?.intValue,
                  let pipeline = item["pipeline_name"]?.stringValue,
                  let version = item["pipeline_version"]?.intValue,
                  let flow = item["flow_name"]?.stringValue,
                  let state = item["state"]?.stringValue else { return nil }
            return OverviewRunStatus(
                id: id,
                sourceName: item["source_name"]?.stringValue,
                pipelineName: pipeline,
                pipelineVersion: version,
                flowName: flow,
                state: state,
                progress: item["progress"]?.objectValue?["fraction"]?.doubleValue,
                finishedAt: item["finished_at"]?.stringValue,
                error: item["error"]?.stringValue
            )
        }
    }
}
