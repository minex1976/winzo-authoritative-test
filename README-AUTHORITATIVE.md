# Winzo authoritative-server test setup

## Files
- `index.html` — player app; existing game UI/animations preserved.
- `admin.html` — separate admin approval panel.
- `server.js` — authoritative Node/WebSocket server.
- `package.json` — server dependencies.
- `.devcontainer/devcontainer.json` — Codespaces setup.
- `.github/workflows/pages.yml` — GitHub Pages deployment.
- `.env.example` — environment variable template.

## Codespaces test
1. Put the repository in a GitHub Codespace.
2. Set the Codespace port 8080 visibility to **Public**.
3. Set these environment variables in the Codespace terminal before starting the server:
   - `FIREBASE_SERVICE_ACCOUNT`
   - `BOT_TOKEN`
   - `ADMIN_KEY`
   - `ALLOWED_ORIGIN` (for Codespaces testing, the forwarded GitHub Pages/Codespaces origin must be allowed; the server already permits `*.github.dev` in development mode.)
4. Run `npm install` and `npm start`.
5. Open the forwarded port URL. The WebSocket URL is the same host with `wss://`.
6. In `index.html`, set `window.WINZO_SERVER_WS_URL` before the main script, or open the page with `?server=wss://YOUR-CODESPACE-8080.app.github.dev`.

## Admin
Open `admin.html` with the same `?server=` parameter and enter the `ADMIN_KEY` value. The player browser never receives the admin key.

## Money flow
- Player deposit/withdrawal requests are sent to `server.js` over WebSocket.
- Deposit stays `pending` until admin approval; approval credits `playWallet`.
- Withdrawal reserves `pendingWithdrawal` when requested.
- Withdrawal approval moves the reserved amount out of `mainWallet`.
- Withdrawal rejection releases the reservation.
- The player browser does not directly create or approve transaction records.

## Realtime game flow
- WebSocket is the single authoritative realtime game-state channel.
- Firebase remains persistence.
- A temporary WebSocket close no longer deletes the player's server-side round state.
- Game-state REST polling is intentionally not used.
