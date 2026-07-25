# Releasing the macOS app

## Prerequisites

- A Developer ID Application certificate installed in the login Keychain.
- The Apple Developer team id in `APPLE_TEAM_ID`.
- For notarization, a `notarytool` Keychain profile:

  ```sh
  xcrun notarytool store-credentials windex-notary
  export NOTARY_PROFILE=windex-notary
  ```

The repository contains no signing credentials. CI builds and tests unsigned;
release archives are made only on an authorized Mac.

## Archive

From `clients/macos`:

```sh
export APPLE_TEAM_ID=ABCDE12345
export NOTARY_PROFILE=windex-notary
Tools/release.sh 0.1.0 1
```

The script archives with hardened runtime, exports a Developer ID app, submits
the zip for notarization, staples the ticket, and recreates the distributable at
`build/release/Windex-<version>.zip`.

If `NOTARY_PROFILE` is unset, the script creates a signed but unnotarized zip and
says so.

## Before publishing

1. Run the package and Xcode test commands in `README.md`.
2. Pair the signed app directly to the production LAN address.
3. Confirm macOS presents and remembers the local-network permission.
4. Exercise Overview, Search, Sources, Runs, Recipes, and Marketplace.
5. Verify the archive:

   ```sh
   codesign --verify --deep --strict --verbose=2 build/release/export/Windex.app
   spctl --assess --type execute --verbose=2 build/release/export/Windex.app
   xcrun stapler validate build/release/export/Windex.app
   ```
