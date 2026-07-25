import Foundation
import Security

protocol TokenStoring: Sendable {
    func loadToken(for profile: ConnectionProfile) throws -> String?
    func saveToken(_ token: String, for profile: ConnectionProfile) throws
    func deleteToken(for profile: ConnectionProfile) throws
}

struct KeychainTokenStore: TokenStoring, Sendable {
    private let service: String

    init(service: String = "com.stevemurr.windex.admin-token") {
        self.service = service
    }

    func loadToken(for profile: ConnectionProfile) throws -> String? {
        var query = baseQuery(for: profile)
        query[kSecReturnData] = true
        query[kSecMatchLimit] = kSecMatchLimitOne

        var result: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &result)
        switch status {
        case errSecSuccess:
            guard let data = result as? Data,
                  let token = String(data: data, encoding: .utf8) else {
                throw KeychainTokenError.invalidData
            }
            return token
        case errSecItemNotFound:
            return nil
        default:
            throw KeychainTokenError.status(status)
        }
    }

    func saveToken(_ token: String, for profile: ConnectionProfile) throws {
        let data = Data(token.utf8)
        let query = baseQuery(for: profile)
        let attributes: [CFString: Any] = [
            kSecValueData: data,
            // Explicitly WhenUnlocked: an admin credential should not be
            // available while the Mac is locked.
            kSecAttrAccessible: kSecAttrAccessibleWhenUnlocked,
        ]

        let updateStatus = SecItemUpdate(
            query as CFDictionary, attributes as CFDictionary)
        if updateStatus == errSecSuccess {
            return
        }
        guard updateStatus == errSecItemNotFound else {
            throw KeychainTokenError.status(updateStatus)
        }

        var insertion = query
        attributes.forEach { insertion[$0.key] = $0.value }
        let addStatus = SecItemAdd(insertion as CFDictionary, nil)
        guard addStatus == errSecSuccess else {
            throw KeychainTokenError.status(addStatus)
        }
    }

    func deleteToken(for profile: ConnectionProfile) throws {
        let status = SecItemDelete(baseQuery(for: profile) as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeychainTokenError.status(status)
        }
    }

    private func baseQuery(for profile: ConnectionProfile) -> [CFString: Any] {
        [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: profile.credentialAccount,
        ]
    }
}

enum KeychainTokenError: LocalizedError {
    case invalidData
    case status(OSStatus)

    var errorDescription: String? {
        switch self {
        case .invalidData:
            "The saved token is not valid UTF-8."
        case .status(let status):
            SecCopyErrorMessageString(status, nil) as String?
                ?? "Keychain error \(status)."
        }
    }
}
