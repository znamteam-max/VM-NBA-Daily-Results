# cron-job.org setup for NBA minute polling

Production scheduling is intentionally external. GitHub Actions `schedule` is disabled.

## 1. GitHub token

Create a dedicated **fine-grained personal access token** for cron-job.org:

- Resource owner: `znamteam-max`
- Repository access: **Only select repositories** → `VM-NBA-Daily-Results`
- Repository permissions → **Actions: Read and write**
- Give it a short/finite expiration and rotate it when needed.

Do not put this token into the repository or workflow file.

## 2. cron-job.org request

Create a cron job with:

- Title: `VM NBA Live Results`
- URL: `https://api.github.com/repos/znamteam-max/VM-NBA-Daily-Results/actions/workflows/nba-live.yml/dispatches`
- Method: `POST`
- Schedule: every minute. It is safe to run 24/7 because the workflow itself only polls for results for planned trigger times from 22:00 through 10:00 Moscow time.

Custom headers:

- `Authorization: Bearer <DEDICATED_FINE_GRAINED_PAT>`
- `Accept: application/vnd.github+json`
- `X-GitHub-Api-Version: 2026-03-10`
- `Content-Type: application/json`

Request body:

```json
{"ref":"main","inputs":{"external_cron":true,"external_cron_epoch":"%cjo:unixtime%"}}
```

cron-job.org replaces `%cjo:unixtime%` with the planned execution timestamp. The workflow uses that timestamp (not delayed runner start time) to enforce the Moscow 22:00–10:00 window.

## 3. Safety behavior

- One external call = exactly one NBA poll.
- At most one never-posted final may be sent per minute.
- If a GitHub runner starts more than 180 seconds after the planned cron-job.org time, that stale call is discarded rather than replayed later.
- Existing `posted/` markers prevent duplicate Telegram posts.
- A successful result writes its marker back to `main` after the tick.
- Pushes to the repository run smoke/regression tests only; they never send result posts.

## 4. Test

Use cron-job.org **Test run** once after saving. A successful GitHub workflow dispatch normally returns HTTP `204` (or `200` if GitHub is configured to return run details). Then confirm a new `workflow_dispatch` run appears under GitHub Actions.
