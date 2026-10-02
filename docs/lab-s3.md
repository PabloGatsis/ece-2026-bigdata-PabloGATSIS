# Lab: Object storage with S3 — results

Bucket `user-p-gatsis-ece`, endpoint `https://s3.seaweedfs.adm.adaltas.cloud` (SeaweedFS).

| Step |  |
| --- | --- |
| Upload (PUT) | `upload: ./users.csv to s3://user-p-gatsis-ece/bronze/users.csv` (7351 B) |
| List | `ls --recursive` returned `bronze/users.csv`, `lab-1/deployment.yaml`, `lab-1/service.yaml` |
| Download (GET) | round-trip `diff` -> `identical` |
| Move / rename | `mv` = server-side COPY + DELETE, not atomic |
| Delete | single key removed; no directory exists |
| Metadata | ETag `6a28331580e96326f4ae39eac53d02a8` == `md5sum users.csv` |
| User metadata | `{"source":"dataset_users.py","version":"1","rows":"50"}` |
| Presigned GET | `curl` fetched the CSV with no credentials |
| Presigned PUT | generated with boto3 (2366 chars) |
| Multipart | 200 MiB -> ETag suffix `-25` = 25 parts of 8 MiB |
| Versioning | 3 versions; older restored by id; `rm` created a delete marker, data kept |
| Lifecycle | 3 rules accepted and read back |
| Bucket policy | `DeleteObject` -> **AccessDenied** (enforced for authenticated users here) |
| Consistency | read-after-write immediate |
| Cleanup | versions + markers deleted, versioning suspended, lifecycle removed |

## Kubernetes Job

`job-upload-bronze.yaml` uploads both datasets to the bronze layer from inside the cluster
(datasets in a ConfigMap, credentials in a Secret).

```
$ kubectl logs job/upload-bronze
upload: ../data/users.csv to s3://user-p-gatsis-ece/bronze/users.csv
upload: ../data/orders.csv to s3://user-p-gatsis-ece/bronze/orders.csv
2026-10-02 10:04:44     331568 bronze/orders.csv
2026-10-02 10:04:38       7351 bronze/users.csv
```

 `limits.cpu: 500m` was added. The namespace ResourceQuota
(`onyxia-quota`) requires requests and limits for both cpu and memory; without it no pod is
ever created and the Job sits at `0/1` with `must specify limits.cpu for: upload`.

## Questions

**Why a Secret and not a ConfigMap?** Both inject the same way, but RBAC is per resource type:
`get configmaps` is commonly granted, `get secrets` is restricted. ConfigMaps show in plain text
in `kubectl get -o yaml` and `describe`; Secrets are redacted and can be encrypted at rest in etcd.
A Secret is not encrypted by default — the gain is narrower permissions and redaction.

**Temporary credentials — what happens tomorrow?** The Job fails. The key starts with `ASIA`
(STS session credentials) and the Secret stores a snapshot, so once the session expires every S3
call returns `ExpiredToken`. Production uses workload identity (IRSA/OIDC) so the pod fetches and
refreshes its own short-lived token, or a secrets manager (Vault, External Secrets) that rotates it.

**Turning it into a daily ingestion.** Wrap the same pod template in a `CronJob`
(`schedule: "0 2 * * *"`, `concurrencyPolicy: Forbid`). The schedule is the easy part; it also needs
workload identity instead of a snapshot, data read from the source system rather than a 1 MiB
ConfigMap, date-partitioned keys (`bronze/users/dt=YYYY-MM-DD/`) so history is not overwritten and
re-runs are idempotent, and alerting on failure. Once tasks have ordering, it belongs in an
orchestrator rather than a CronJob.
