import Foundation
import OpenAPIRuntime

// Clean names for the generated epoch-2 control-plane DTOs.
public typealias Health = Components.Schemas.Health
public typealias Capabilities = Components.Schemas.Capabilities
public typealias Registry = Components.Schemas.RegistryResponse
public typealias PipelineWire = Components.Schemas.PipelineModel
public typealias PipelinesWire = Components.Schemas.PipelinesResponse
public typealias PipelineRevisionWire = Components.Schemas.PipelineRevisionModel
public typealias PipelineRevisionsWire = Components.Schemas.PipelineRevisionsResponse
public typealias PipelineValidationWire = Components.Schemas.ValidationReport
public typealias SourceValidationWire = Components.Schemas.DeploymentReport
public typealias PipelineTaskPreviewWire = Components.Schemas.TaskPreviewResponse
public typealias PipelineLayoutWire = Components.Schemas.LayoutResponse
public typealias SourceWire = Components.Schemas.SourceModel
public typealias SourcesWire = Components.Schemas.SourcesResponse
public typealias SourceSettingsWire = Components.Schemas.SettingsProjection
public typealias SourceStatusWire = Components.Schemas.SourceStatusResponse
public typealias SourceUpgradePreviewWire = Components.Schemas.UpgradePreviewResponse
public typealias SourceTriggersWire = Components.Schemas.TriggersResponse
public typealias RunWire = Components.Schemas.RunModel
public typealias RunsWire = Components.Schemas.RunsResponse
public typealias QueuedRunWire = Components.Schemas.QueuedRunResponse
public typealias RunEventsWire = Components.Schemas.EventsResponse
public typealias RunOutputsWire = Components.Schemas.RunOutputsResponse
public typealias OverviewWire = Components.Schemas.OverviewResponse
public typealias GlobalSettingsWire = Components.Schemas.OperatorSettingsResponse
public typealias SecretsWire = Components.Schemas.SecretsResponse
public typealias LogEventsWire = Components.Schemas.EventsResponse
public typealias LogFacetsWire = Components.Schemas.FacetsResponse
public typealias OperationalEventWire = Components.Schemas.OperationalEventModel
public typealias ActionWire = Components.Schemas.ActionResponse

/// The endpoint intentionally returns an open identity object.
public typealias WhoAmI = [String: JSONValue]

extension Health {
    public var isOK: Bool { status == "ok" }
    public var isWindex: Bool { service == "windex" }
    public var needsToken: Bool { authRequired }
    public var isSupportedEpoch: Bool { contractEpoch == 2 }
}

extension OpenAPIObjectContainer {
    public func decode<T: Decodable>(_ type: T.Type = T.self) throws -> T {
        do {
            let data = try JSONEncoder().encode(self)
            return try JSONDecoder().decode(T.self, from: data)
        } catch {
            throw WindexError.decoding(underlying: error)
        }
    }
}

func roundTrip<T: Encodable, U: Decodable>(_ value: T, as type: U.Type = U.self) throws -> U {
    do {
        return try JSONDecoder().decode(U.self, from: JSONEncoder().encode(value))
    } catch {
        throw WindexError.decoding(underlying: error)
    }
}
