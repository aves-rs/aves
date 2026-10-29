---
source: Context7 API + docs.rs
library: Lettre
package: lettre
version: 0.11.23
verified: 2026-10-29
official_docs: https://docs.rs/crate/lettre/0.11.23/source/src/transport/smtp/async_transport.rs
---

### AsyncSmtpTransport::test_connection

Tests the connection to the SMTP server. **Authenticates during connection establishment** when credentials are configured on the transport, then performs a NOOP round-trip. This method is async and returns a boolean indicating success.

**Signature**
`pub async fn test_connection(&self) -> Result<bool, Error>`

**Returns**
- **Result<bool, Error>** - `Ok(true)` if authentication and NOOP succeed, `Ok(false)` if NOOP fails after successful authentication, or an error if connection or authentication fails.

**Implementation details (from source)**
- Opens connection via `self.inner.connection().await`
- **Authentication occurs during connection establishment**: configured credentials trigger `auth` before NOOP (see `AsyncSmtpClient::connection` at async_transport.rs)
- Calls `conn.test_connected().await` (sends NOOP)
- Closes connection with `quit()` if not using pool feature
- Returns `Ok(false)` when NOOP fails after successful authentication — this is a probe failure, not a transport error