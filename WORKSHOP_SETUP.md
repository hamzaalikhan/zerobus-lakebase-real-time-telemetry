# Workshop Setup — Data-Centric Edition

## Overview

This setup guide is for the **data-centric** workshop variant. It uses Declarative Automation Bundles
(DABs) to deploy both the DataCart Storefront app and the Lakebase Autoscaling project before
labs start, so workshop time isn't burned on UI clicks.

## Prerequisites

- **Databricks Workspace**: any workspace with Lakebase Autoscaling and Databricks Apps support
- **Databricks CLI**: v0.229.0+ (only required if deploying via terminal — workspace-UI deploy needs no local CLI)
- **Unity Catalog**: enabled in your workspace (required for Labs 4.1 and 5.1)
- **A SQL warehouse**: needed for the federated queries in Lab 4.1 (any size)
- **Workspace files** and **serverless compute** must be enabled in the workspace (admin settings) for the workspace-UI deploy flow to work

> **Note:** the React frontend is pre-built and included in `frontend/dist/`. No Node.js or npm
> is required for setup — the bundle deploys the static assets directly.

## What the Bundle Deploys

```
datacart-storefront/                  ← bundle root (databricks.yml)
├── databricks.yml                    →  bundle target + workspace path
└── resources/
    ├── lakebase_instance.yml         →  postgres_projects: lakebase-workshop-<your-user-id>
    │                                       (0.5–2 CU autoscaling, 300s scale-to-zero, PG 17)
    └── datacart_storefront.app.yml   →  Databricks App: storefront-<your-user-id>
                                          (env vars LAKEBASE_PROJECT, ENDPOINT_NAME, DB_SCHEMA)
```

`bundle deploy` provisions the Lakebase Autoscaling project AND uploads the app source files in
one step. The bundle does **not** create the Postgres schema, seed tables, or grant database
permissions — those happen in Labs 1.1 and 2.1, which is part of the actual workshop content.

Both the Lakebase project and the app name are auto-derived per user from
`${workspace.current_user.id}` (e.g. `lakebase-workshop-6530815146371371` and
`storefront-6530815146371371`), so multiple attendees can deploy into the same workspace without
colliding. The app's `description` field carries the deployer's full name so each attendee can
spot their own app in the Apps list UI.

## Path A: Deploy from the workspace UI (no CLI required)

1. **Add the repo as a Databricks Git folder.** In the workspace, open your home folder → Add → Git folder → paste the repo URL → Create.
2. Navigate into the Git folder → `lakebase-in-a-box-workshop-data-centric` → `datacart-storefront`.
3. Click `databricks.yml` to open it in the editor.
4. Click the **deployments icon** in the editor toolbar.
5. In the **Deployments** pane choose target **`dev`** → click **Deploy**.
6. Confirm in the **Deploy to dev** dialog → click **Deploy** again.

This creates the Lakebase project and the app shell, and uploads the source files to:

```
/Workspace/Users/<your-email>/.bundle/datacart-storefront-data-centric/dev/files/
```

**Push the source onto the running app** — clicking ▶ run on the app in the Bundle resources
pane only starts the compute; it doesn't push the source. To push and start, do one of:

- **From a local terminal (recommended)**: `cd datacart-storefront && databricks bundle run datacart_storefront --profile <your-profile>`
- **Or in the workspace**: Compute → Apps → `storefront-<your-user-id>` → **Deploy** button → set source path to `/Workspace/Users/<your-email>/.bundle/datacart-storefront-data-centric/dev/files` → click Deploy.

After source is deployed, app status moves to `RUNNING` and the URL becomes accessible.

## Path B: Deploy from a local terminal

Make sure you have the Databricks CLI installed and authenticated:

```bash
databricks auth login --host <workspace-url> --profile <your-profile>
```

Then:

```bash
cd datacart-storefront

# Validate
databricks bundle validate --profile <your-profile>

# Provision Lakebase project + create app + upload source
databricks bundle deploy --profile <your-profile>

# Push source onto the app and start it
databricks bundle run datacart_storefront --profile <your-profile>
```

If your default CLI profile already targets the right workspace, the `--profile` flag is optional.

## Run the Labs

After deployment, the app shows "Loading…" until the database is set up:

1. **Lab 1.1** (`1.1 Lab - Discover and Seed the Lakebase Project`) — discovers the deployed project, creates the `ecommerce` schema, seeds 5 tables.
2. **Lab 2.1** (`2.1 Lab - Roles Permissions and Connect Storefront`) — grants the storefront's service principal access to the schema. The "Loading…" disappears and you see products + a working cart.
3. **Remaining labs** in order: 3.1 → 4.1 → 5.1 → 6.1 → 6.2 → 6.3 → 7.1 → 8 → 9.

## Re-deploying After Edits

```bash
databricks bundle deploy --profile <your-profile>
databricks bundle run datacart_storefront --profile <your-profile>    # pushes new source onto running app
```

`bundle deploy` is incremental — only changed files are uploaded.

## Tearing Down

```bash
databricks bundle destroy --profile <your-profile>
```

This removes the app **and** the Lakebase Autoscaling project (the postgres_projects resource
is destroyed natively by the bundle, no manual cleanup needed).

## Troubleshooting

### "Project not found" in Lab 1.1

Verify the project deployed successfully:

```bash
databricks postgres list-projects --profile <your-profile>
```

You should see one named `projects/lakebase-workshop-<your-user-id>`. If absent, re-run `bundle deploy`.

### Storefront stays on "Loading…" after Lab 2.1

Look at the app logs:

```bash
databricks apps logs storefront-<your-user-id> --profile <your-profile>
```

Common causes:
- The SP didn't get `USAGE` / `SELECT` / `INSERT` grants. Re-run Lab 2.1.
- The app started before the schema existed. Run Lab 1.1, then re-run `databricks bundle run datacart_storefront` to push fresh source.
- Lakebase Autoscaling doesn't have native Apps resource binding yet — the app's `server/db.py` discovers PGHOST from the SDK at runtime. If logs show `password authentication failed`, the SP's Postgres role hasn't been created yet — Lab 2.1's grants (executed as the project owner) trigger that.

### `bundle validate` errors about `root_path`

The bundle pins `root_path` to `${workspace.current_user.userName}`, so it auto-derives. If you see this error, your workspace might not have user-derived paths enabled — open `databricks.yml` and hardcode `root_path` to your `/Workspace/Users/<your-email>/.bundle/...` path.

### The federated query in Lab 4.1 errors with "connection refused"

Foreign catalog connections require Lakehouse Federation to be enabled on your SQL warehouse. Use a serverless SQL warehouse if you don't have classic warehouses configured for federation.

### The Lakehouse Sync option doesn't appear in the UI (Lab 5.1)

Lakehouse Sync is gated by region and feature flag — confirm the **Sync to Unity Catalog** option is visible on your project's page. If not, ask your Databricks contact to enable the feature on this workspace.
