# Bonus Labs — Advanced Lakebase Operations

These optional labs go beyond the core workshop loop (**collect → aggregate →
present**) and explore Lakebase as an operational Postgres platform: live
federation, Git-style database branching, schema migration, disaster recovery,
and monitoring.

They are **not required** for the main workshop and have no bearing on the
Zerobus telemetry loop. Run them once you've completed the core labs
(`1.1` → `4.1` in the workshop root) and want to go deeper on Lakebase's
day-2 operational capabilities.

## Contents

| Lab | Topic | What you'll learn |
|-----|-------|-------------------|
| **1.1** | Register Lakebase in Unity Catalog | Expose Lakebase as a UC foreign catalog for live, federated reads from a SQL warehouse |
| **2.1** | Parallel Development with Branching | Create isolated dev branches with point-in-time copies of production data |
| **3.1** | Schema Migration to Production | Promote schema changes from a feature branch to production via Migration Replay |
| **4.1** | Branch Reset | Reset a branch back to its parent's state |
| **5.1** | Point-in-Time Recovery & Snapshots | Recover from a disaster by restoring to a point in time before the incident |
| **6.1** | Monitoring | Observe Lakebase health and query activity via Postgres system views |

## Prerequisites

All bonus labs assume you've completed the core setup and seeding:

- **Core Lab 1.1** (Discover and Seed) — creates the `ecommerce` schema every
  bonus lab reads from.
- **Core Lab 2.1** (Roles & Connect Storefront) — sets up the Postgres roles the
  federation and branching labs rely on.

Within this folder, run the branching chain in order: **2.1** (create branches)
→ **3.1** (promote to production) → **4.1** (reset). Labs **1.1**, **5.1**, and
**6.1** are independent and only require the core setup above.
