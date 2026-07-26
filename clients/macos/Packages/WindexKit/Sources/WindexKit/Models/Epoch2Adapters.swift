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
        if let raw = payload["positions"] {
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
    public func status(runs: [SourceRunSummary]) throws -> SourceStatus {
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
            nextTrigger: raw["next_trigger"]?.stringValue,
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
        let runs = runProjection["counts"]?.objectValue ?? runProjection
        let totals = raw["totals"]?.objectValue ?? [:]
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
            serviceVersion: health["version"]?.stringValue ?? "",
            uptimeSeconds: health["uptime_s"]?.intValue ?? 0,
            documentsPerMinute: (totals["indexed_last_hour"]?.doubleValue ?? 0) / 60,
            indexedDocuments: totals["indexed_documents"]?.intValue
                ?? totals["documents"]?.intValue ?? 0,
            stagedDocuments: totals["staged_documents"]?.intValue
                ?? totals["staged"]?.intValue ?? 0,
            pendingEmbedding: totals["pending_embedding"]?.intValue ?? 0,
            runs: .init(
                active: runs["active"]?.intValue ?? runs["running"]?.intValue ?? 0,
                queued: runs["queued"]?.intValue ?? 0,
                blocked: runs["blocked"]?.intValue ?? 0,
                failed: runs["failed"]?.intValue ?? 0
            ),
            sources: sourceDeployments.map { source in
                let metadata = sourceMetadata[source.name]
                return OverviewSourceStatus(
                    source: source,
                    lastSuccess: metadata?["last_success"]?.stringValue,
                    lastFailure: metadata?["last_failure"]?.stringValue
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
            recentFailures: recentFailures
        )
    }
}
