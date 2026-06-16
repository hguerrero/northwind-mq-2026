# Kong Event Gateway — Demo Script
## Account: Northwind Financial
### Version: 1.0 | Audience: Technical evaluators, platform/infosec architects

---

## The Story

> Northwind Financial is a mid-size financial services firm with three business units:
> **Retail Banking NY**, **Wealth Management LA**, and a shared **Payment Rail Core**.
> They run a self-managed Kafka cluster inside a locked-down internal network — it works,
> but every service talks directly to Kafka with no isolation, no tenant boundaries, and no
> policy enforcement. As the firm opens new channels and onboards external partners, the
> team realises their Kafka cluster is a flat, shared bus where a misconfigured consumer
> can see everything.
>
> This demo follows their journey: from raw, unprotected Kafka → to a fully governed
> event fabric managed by Kong Event Gateway — one step at a time.

---

## Environment Bootstrap (one-time setup)

Run these steps once before any demo rehearsal or live session. They are not repeated
between scenes.

### Step 0a — Generate the data plane TLS certificate

The data plane authenticates to Konnect using a self-signed certificate. Generate the
key pair and place it in `kongctl/certs/`:

```bash
openssl req -new -x509 -nodes -newkey rsa:2048 \
  -subj "/CN=event-gateway/C=US" \
  -keyout kongctl/certs/key.crt \
  -out    kongctl/certs/tls.crt
```

> `key.crt` stays local — the data plane process uses it at runtime.
> `tls.crt` (the public certificate) is uploaded to Konnect in the next step.

### Step 0b — Register the certificate in Konnect

Apply `kongctl/data_plane_certificate.yaml`. This uploads `tls.crt` to Konnect and
returns a `KONG_KONNECT_GATEWAY_CLUSTER_ID` you'll need for the env file.

```bash
# Ensure KONNECT_TOKEN is set first
export KONNECT_TOKEN=<your-personal-access-token>

kongctl apply -f kongctl/data_plane_certificate.yaml
```

After the apply succeeds, retrieve the gateway cluster ID from Konnect (UI or CLI):

```bash
# Via Konnect UI: Event Gateways → northwind-event-gateway → Data Plane → copy Cluster ID
# Or via API:
kongctl get event-gateway northwind-event-gateway --output json --jq '.id' --jq-raw-output
```

### Step 0c — Configure `konnect.env`

Copy the example file, then populate it in three parts:

```bash
cp konnect.env.example konnect.env
```

**Connection settings** — edit manually:

```bash
KONG_KONNECT_REGION=us               # e.g. us, eu, au
KONG_KONNECT_DOMAIN=konghq.com
KONG_KONNECT_GATEWAY_CLUSTER_ID=<cluster-id-from-step-0b>
```

**TLS identity** — load the cert and key from the files generated in step 0a using `printf`
so multi-line PEM content is handled correctly:

```bash
printf 'KONG_KONNECT_CLIENT_CERT="%s"\n' "$(cat kongctl/certs/tls.crt)" >> konnect.env
printf 'KONG_KONNECT_CLIENT_KEY="%s"\n'  "$(cat kongctl/certs/key.crt)"  >> konnect.env
```

> The `printf` approach preserves newlines inside the PEM block. Do not paste the cert
> manually into the file — the line breaks will be stripped and the data plane will fail
> to authenticate.

### Step 0d — Start the data plane

With the cert in place and `konnect.env` populated, bring up the gateway data plane:

```bash
docker compose up -d
```

Verify it connected to Konnect:

```bash
docker compose logs keg-data-plane | grep -i "connected\|ready\|error"
```

You should see the data plane report as connected in the Konnect UI under
**Event Gateways → northwind-event-gateway → Data Planes**.

---

## Pre-Demo Checklist

- [ ] Certs generated and in `kongctl/certs/` (`key.crt` + `tls.crt`)
- [ ] `kongctl/data_plane_certificate.yaml` applied — data plane cert registered in Konnect
- [ ] `konnect.env` populated with region, domain, and `KONG_KONNECT_GATEWAY_CLUSTER_ID`
- [ ] Kafka stack running: `docker compose -f kafka/docker-compose.yaml up -d`
- [ ] Topics created: `kafkactl get topics` returns the full list from `kafka/config/topics.txt`
- [ ] Gateway data plane running and connected: `docker compose up -d`
- [ ] `KONNECT_TOKEN` exported in shell; `kongctl` pointing to `northwind` namespace
- [ ] Backup: recorded walkthrough in `recordings/` folder
- [ ] Terminal split: one pane for `kongctl apply`, one for `kafkactl` producer/consumer

---

## Opening (2 minutes)

> _Frame the demo around their pain, not the product._

"Northwind Financial has a problem shared by almost every financial services firm that
adopted Kafka early: it became the backbone of the firm before anyone designed governance
into it. Today I'm going to show you five specific things:

1. **Network isolation** — put the gateway in front of Kafka without touching a single broker config.
2. **Tenant segmentation** — give each business unit a clean, isolated view of the bus.
3. **Auth at the edge** — move credential management off the cluster and onto the gateway.
4. **ACL enforcement** — move topic access rules off the cluster and start enforcing them based on identity claims.
5. **Data policies** — schema enforcement and field-level encryption in motion, with zero changes to producers or consumers.

I'll do every step live with `kongctl` — the same declarative YAML-based tool your team would use in CI/CD."

---

## Scene 1 — The Status Quo (Before the Gateway)

**Pain:** Everything is flat. Every service that can reach the network can see every topic.

**What to show:**

```bash
# Connect directly to Kafka and list all topics
kafkactl get topics
```

Walk through the list with the audience:

```
infosec.security.fraud.anomaly-pings.v1           ← fraud signals, no access control
nw.ledger.transactions.high-value-wire-transfers.v1 ← wire transfers, in plaintext
WEALTH_LA.clients.sentiment-signals.v1             ← client data, visible to anyone
RETAIL_NY.payments.card-dispatch.v1                ← card issuance, open to all consumers
nw.treasury.payments.priority-instructions.v1      ← treasury movements, no auth required
```

**Talking point:**
> "Every topic on the bus is addressable by any consumer that has network access. There's no
> tenant boundary. A retail branch app misconfigured with the wrong bootstrap servers could
> read wealth management client sentiment data. The infosec fraud signals are reachable by
> any process in the network. This isn't a Kafka problem — Kafka does exactly what it was
> designed to do. The gap is that the governance layer was never built."

**Key message:** _The cluster is healthy. The perimeter isn't._

**Transition:**
> "Let's fix the perimeter first. We'll put the gateway in front of Kafka — without changing
> a single broker configuration or producer application."

---

## Scene 2 — Network Isolation: The Gateway Goes In

**Requirement:** Isolate the Kafka cluster from direct client access without disrupting existing producers.

**Pain:** Ops team can't take the cluster down or modify broker configs during business hours.

### Step 2a — Register the backend cluster and bring up the first virtual cluster

Apply phase 1: backend cluster registration, a listener on `19092–19190`, and a single
flat passthrough virtual cluster. A listener and virtual cluster are always required — the
gateway won't serve traffic without them. At this stage there's no namespace, no auth, no
policies — just the gateway acting as a transparent proxy.

```bash
kongctl apply -f kongctl/phase-1-backend-only.yaml
```

**Talking point:**
> "The backend cluster registration tells the gateway where the brokers are. We haven't
> touched the brokers themselves. Alongside it we've declared a passthrough virtual cluster
> and wired a listener to it — the gateway won't route traffic without that pair. The data
> plane container running on your edge is now the only bootstrap endpoint clients need."

### Step 2b — Show the data plane is up

```bash
# Kafka brokers are no longer directly reachable by clients
# Connect via the gateway instead
kafkactl get topics --context northwind-core
```

**Talking point:**
> "Same topics, same data — but now the path goes through the gateway. The internal network
> firewall can now block direct Kafka port access (9092) for external zones. The brokers
> didn't change. The apps didn't change. The gateway absorbed the perimeter."

**Key message:** _Network isolation in one `kongctl apply`. Zero broker changes. Zero downtime._

**Transition:**
> "The perimeter is up. But it's still one flat surface. All clients see the same topics.
> Let's build tenant boundaries."

---

## Scene 3 — Tenant Isolation: Virtual Clusters and Namespaces

**Requirement:** Give RETAIL_NY and WEALTH_LA isolated views of the event bus.

**Pain:** Business units are stepping on each other's consumer groups. Retail branch apps
occasionally drift into Wealth topics because bootstrap servers are the same.

### Step 3a — Add virtual clusters with namespace prefixes

```yaml
# kongctl/phase-2-virtual-clusters.yaml  (extends phase-1)
    listeners:
      - ref: core-listener
        name: core-listener
        addresses: ["0.0.0.0"]
        ports: ["19092-19190"]
        policies:
          - ref: forward-to-northwind-core
            type: forward_to_virtual_cluster
            name: forward-to-northwind-core
            config:
              type: port_mapping
              destination:
                id: !ref Northwind-Core
              advertised_host: localhost
              bootstrap_port: none
              min_broker_id: 0

      - ref: retail-banking-ny-listener
        name: retail-banking-ny-listener
        addresses: ["0.0.0.0"]
        ports: ["19192-19290"]
        policies:
          - ref: forward-to-retail-banking-ny
            type: forward_to_virtual_cluster
            name: forward-to-retail-banking-ny
            config:
              type: port_mapping
              destination:
                id: !ref retail-banking-ny
              advertised_host: localhost
              bootstrap_port: none
              min_broker_id: 0

      - ref: wealth-management-la-listener
        name: wealth-management-la-listener
        addresses: ["0.0.0.0"]
        ports: ["19292-19390"]
        policies:
          - ref: forward-to-wealth-management-la
            type: forward_to_virtual_cluster
            name: forward-to-wealth-management-la
            config:
              type: port_mapping
              destination:
                id: !ref wealth-management-la
              advertised_host: localhost
              bootstrap_port: none
              min_broker_id: 0

    virtual_clusters:
      - ref: retail-banking-ny
        name: retail-banking-ny
        destination:
          id: !ref Northwind-Core-Event-Hub
        acl_mode: passthrough           # ACLs added in the next scene
        dns_label: retail-banking-ny
        namespace:
          mode: hide_prefix
          prefix: "RETAIL_NY."
          additional:
            topics:
              - type: exact_list
                exact_list:
                  - backend: nw.analytics.forecasting.market-weather.v1
              - type: glob
                glob: "infosec.security.*"
        authentication:
          - type: anonymous

      - ref: wealth-management-la
        name: wealth-management-la
        destination:
          id: !ref Northwind-Core-Event-Hub
        acl_mode: passthrough
        dns_label: wealth-management-la
        namespace:
          mode: hide_prefix
          prefix: "WEALTH_LA."
          additional:
            topics:
              - type: exact_list
                exact_list:
                  - backend: nw.analytics.forecasting.market-weather.v1
              - type: glob
                glob: "infosec.security.*"
        authentication:
          - type: anonymous

      - ref: Northwind-Core
        name: Northwind-Core
        destination:
          id: !ref Northwind-Core-Event-Hub
        acl_mode: passthrough
        dns_label: northwind-core
        authentication:
          - type: anonymous
```

```bash
kongctl apply -f kongctl/phase-2-virtual-clusters.yaml
```

### Step 3b — Show the namespace magic

```bash
# Retail client — connects to port 19192 (retail-banking-ny)
kafkactl get topics --context retail-banking-ny
```

Expected output (prefix hidden):
```
markets.exchange-ticker.v1
branches.commuter-foot-traffic.v1
core.accounts.status.v2
core.customers.weather-impact-profiles.v1
payments.card-dispatch.v1
analytics.forecasting.market-weather.v1    ← nw.analytics.forecasting.market-weather.v1 (shared, injected)
infosec.security.fraud.anomaly-pings.v1    ← infosec.security.fraud.anomaly-pings.v1 (injected via glob)
```

```bash
# Wealth client — connects to port 19292 (wealth-management-la)
kafkactl get topics --context wealth-management-la
```

Expected output:
```
advisor.daily-client-activity.v1
portfolios.esg-allocation-adjustments.v1
clients.sentiment-signals.v1
infosec.security.fraud.risk-scores.v3      ← WEALTH_LA.infosec.security.fraud.risk-scores.v3 (namespaced)
analytics.forecasting.market-weather.v1    ← WEALTH_LA.analytics.forecasting.market-weather.v1 (namespaced)
infosec.security.fraud.anomaly-pings.v1    ← infosec.security.fraud.anomaly-pings.v1 (injected via glob)
```

**Talking point:**
> "Notice `analytics.forecasting.market-weather.v1` appears in both virtual clusters under
> the same logical name — but they don't point at the same backend topic.
>
> For Retail, it's the shared `nw.analytics.forecasting.market-weather.v1`, injected into
> the Retail view via the `additional.topics` list. For Wealth, it's the tenant-specific
> `WEALTH_LA.analytics.forecasting.market-weather.v1`, surfaced through the namespace prefix.
> Two different backend topics, one shared logical name, zero coordination required between
> the Retail and Wealth engineering teams. The gateway handles the routing per tenant.
>
> The same pattern applies to infosec topics: `infosec.security.fraud.anomaly-pings.v1`
> is a shared backend topic injected into both views via the `infosec.security.*` glob,
> while `infosec.security.fraud.risk-scores.v3` is Wealth-only — it exists as
> `WEALTH_LA.infosec.security.fraud.risk-scores.v3` on the backend and appears in Wealth's
> view through the namespace prefix. Retail never sees it."

**Key message:** _Each tenant gets a clean logical namespace. The same topic name can exist in multiple tenants without collision — the gateway translates silently._

**Transition:**
> "The tenants are isolated at the network and topic level. But authentication is still
> anonymous — anyone who can reach those ports gets in. Let's move auth to the gateway."

---

## Scene 4 — Auth at the Gateway: Credentials Stay at the Edge

**Requirement:** Different authentication mechanisms per business unit, without touching the Kafka cluster.

**Pain:** The Kafka cluster currently has hundreds of SASL credentials. The infosec team
wants to consolidate credential management but can't deprecate cluster-level auth overnight.

### Step 4a — Add SASL/PLAIN termination for Wealth Management

The key: `mediation: terminate` — credentials are validated at the gateway. They never reach Kafka.

```yaml
# kongctl/phase-3-auth.yaml  (diff from phase-2)

    virtual_clusters:
      # Retail stays anonymous — branch systems use certificate-based network auth
      - ref: retail-banking-ny
        # ... same as before ...
        authentication:
          - type: anonymous

      # Wealth Management adds SASL/PLAIN with gateway termination
      - ref: wealth-management-la
        # ... same as before ...
        authentication:
          - type: anonymous
          - type: sasl_plain
            mediation: terminate          # ← credentials stop here; Kafka never sees them
            principals:
              - username: wealth-advisors
                password: secret
```

```bash
kongctl apply -f kongctl/phase-3-auth.yaml
```

### Step 4b — Demonstrate auth enforcement

```bash
# Anonymous access still works (for internal systems)
kafkactl get topics --context wealth-management-la

# Wealth advisor authenticates with SASL/PLAIN
kafkactl get topics --context wealth-advisors
```

**Talking point:**
> "The `mediation: terminate` setting means the gateway validates these credentials itself.
> The Kafka cluster behind it still runs with anonymous auth — we didn't add a single SASL
> user to Kafka. The broker doesn't know `wealth-advisors` exists. That credential lives
> only in the gateway config, managed in version control, deployed via `kongctl`.
> When the infosec team rotates that password, they update one line of YAML and run
> `kongctl apply`. No Kafka admin required."

**Key message:** _Auth is now a gateway concern. Credential rotation doesn't require Kafka downtime or broker reconfiguration._

**Transition:**
> "We have identity at the edge. Now let's put that identity to work — let's move the
> access control rules off the broker and onto the gateway, and make them conditional
> on who's authenticated."

---

## Scene 5 — Gateway-Enforced ACLs: Claim-Based Access Control

**Requirement:** Move topic ACLs from the Kafka cluster to the gateway, enforce them
based on authenticated identity.

**Pain:** Kafka ACLs are managed by a separate team, change requests take 3-5 days,
and they can't express conditional logic — you either have access or you don't.

### Step 5a — Enable ACL enforcement on virtual clusters

```yaml
# kongctl/phase-4-acls.yaml  (diff from phase-3)

    virtual_clusters:
      - ref: retail-banking-ny
        acl_mode: enforce_on_gateway    # ← ACLs now evaluated here, not at the broker
        cluster_policies:
          - ref: acl-retail-banking-ny
            type: acls
            name: acl-retail-banking-ny
            config:
              rules:
                # Allow all topic operations by default
                - action: allow
                  resource_type: topic
                  operations: [describe, read, write]
                  resource_names:
                    - match: "*"
                # Deny infosec topics for all retail connections
                - action: deny
                  resource_type: topic
                  operations: [describe, read, write]
                  resource_names:
                    - match: "infosec.security.*"
                # Allow consumer groups
                - action: allow
                  resource_type: group
                  operations: [describe, read, write, create]
                  resource_names:
                    - match: "*"

      - ref: wealth-management-la
        acl_mode: enforce_on_gateway
        cluster_policies:
          # Policy for anonymous / non-advisor principals
          - ref: acl-wealth-management-la
            type: acls
            name: acl-wealth-management-la
            condition: "context.auth.principal.name != 'wealth-advisors'"
            config:
              rules:
                - action: allow
                  resource_type: topic
                  operations: [describe, read, write]
                  resource_names:
                    - match: "*"
                # Non-advisors cannot touch infosec topics
                - action: deny
                  resource_type: topic
                  operations: [describe, read, write]
                  resource_names:
                    - match: "infosec.security.*"

          # Policy exclusively for the wealth-advisors principal
          - ref: acl-wealth-management-la-advisors
            type: acls
            name: acl-wealth-management-la-advisors
            condition: "context.auth.principal.name == 'wealth-advisors'"
            config:
              rules:
                # Advisors get full access — including infosec topics
                - action: allow
                  resource_type: topic
                  operations: [describe, read, write]
                  resource_names:
                    - match: "*"
```

```bash
kongctl apply -f kongctl/phase-4-acls.yaml
```

### Step 5b — Show the difference

```bash
# Anonymous wealth client — infosec topics are blocked
kafkactl consume infosec.security.fraud.risk-scores.v3 --context wealth-management-la
# → TOPIC_AUTHORIZATION_FAILED
```

```bash
# wealth-advisors principal — access granted by conditional ACL
kafkactl consume infosec.security.fraud.risk-scores.v3 --context wealth-advisors
# → messages flow
```

**Talking point:**
> "Notice the `condition` field on the two policies:
> `context.auth.principal.name != 'wealth-advisors'` and
> `context.auth.principal.name == 'wealth-advisors'`.
> The gateway evaluates which policy applies based on the authenticated identity.
> The same claim expressions work with JWT tokens too — you can write conditions like
> `context.auth.claims.role == 'senior-advisor'` or
> `context.auth.claims.clearance_level >= 3`.
>
> Compare this to Kafka ACLs: they can allow or deny a principal on a topic. They can't
> express conditional logic. They can't reference JWT claims. And they live in ZooKeeper
> or KRaft metadata — they're not in your Git repo, they don't go through code review,
> and a rotation on one cluster doesn't propagate to another.
>
> With `acl_mode: enforce_on_gateway`, the Kafka cluster stops evaluating ACLs entirely.
> One place to manage them. One tool to deploy them."

**Key message:** _ACLs are now code — stored in Git, reviewed in PRs, deployed with `kongctl apply`. Conditional logic based on authenticated claims, not just static principal names._

**Transition:**
> "Identity and access are handled. The last layer is data integrity — making sure what
> flows through the bus is what it's supposed to be, and that the most sensitive data
> is encrypted in transit and at rest on the broker."

---

## Scene 6 — Data Policies: Schema Validation and Field Encryption

**Requirement:** Prevent schema drift on fraud signals. Encrypt wire transfer payloads at the producer.

**Pain:** A bad deploy last quarter pushed malformed JSON to the fraud risk topic and
poisoned a downstream ML model's training set. Wire transfers land in Kafka as plaintext —
a concern for the compliance team ahead of a SOC 2 Type II audit.

### Step 6a — Register the schema in Apicurio

Before the produce policy can validate anything, the schema must exist in the registry.
The subject name follows Confluent convention: `{client-visible-topic-name}-value`.

```bash
curl -s -X POST \
  http://localhost:8080/apis/ccompat/v7/subjects/infosec.security.fraud.risk-scores.v3-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\": \"JSON\", \"schema\": $(cat kafka/config/schemas/fraud_risk_scores.json | jq -Rs .)}"
```

Verify it registered:

```bash
curl -s http://localhost:8080/apis/ccompat/v7/subjects/infosec.security.fraud.risk-scores.v3-value/versions/latest \
  | jq '.schema | fromjson | .title'
# → "FraudRiskScore"
```

### Step 6b — Apply the schema validation produce policy

The `WEALTH_LA.infosec.security.fraud.risk-scores.v3` topic now has a contract in the
registry. The produce policy rejects any message that doesn't match.

```yaml
# kongctl/phase-5-policies.yaml  (diff from phase-4)

    schema_registries:
      - ref: apicurio-schema-registry
        type: confluent
        name: Apicurio Schema Registry
        config:
          schema_type: json
          endpoint: http://apicurio-registry:8080/apis/ccompat/v7
          timeout_seconds: 8

    virtual_clusters:
      - ref: wealth-management-la
        produce_policies:
          - ref: fraud-risk-produce-schema-validation
            type: schema_validation
            name: fraud-risk-produce-schema-validation
            enabled: true
            condition: "context.topic.name == 'infosec.security.fraud.risk-scores.v3'"
            config:
              type: confluent_schema_registry
              schema_registry:
                id: !ref apicurio-schema-registry
              value_validation_action: reject
```

```bash
kongctl apply -f kongctl/phase-5-policies.yaml
```

**Demonstrate rejection:**

```bash
# Produce a well-formed message — succeeds
echo '{"score":0.87,"account_id":"NW-001234","reason":"velocity_spike","evaluated_at":"2026-06-12T14:32:45Z","metadata":{"model_version":"risk-v3.4.1","region":"us-east-1","alert_id":"ALRT-9f2c7d1b"}}' | kafkactl produce infosec.security.fraud.risk-scores.v3 --context wealth-advisors-schema

# Produce a malformed message (score is a string, unknown field "acct", missing required fields)
# — rejected at the gateway before reaching the broker
echo '{"score": "HIGH", "acct": "NW-001234"}' | kafkactl produce infosec.security.fraud.risk-scores.v3 --context wealth-advisors-schema
# → INVALID_RECORD — schema validation failed
```

**Talking point:**
> "The gateway intercepted that produce request before it hit the broker. The malformed
> message never reached Kafka. The downstream ML model never saw it. The schema registry
> already had the contract — we just told the gateway to enforce it.
>
> Notice the `condition` — this policy only fires when the topic name matches. The same
> virtual cluster can have different produce policies for different topics. You don't need
> a separate cluster per compliance requirement."

### Step 6c — Field-level encryption on wire transfers

Wire transfers flow through the `Northwind-Core` virtual cluster. The produce policy
encrypts the message value before it lands on the broker. The consume policy decrypts
transparently on the way out.

```yaml
# Added to Northwind-Core virtual cluster in phase-5-policies.yaml

      - ref: Northwind-Core
        name: Northwind-Core
        destination:
          id: !ref Northwind-Core-Event-Hub
        acl_mode: passthrough

        produce_policies:
          - ref: wire-transfer-encryption-policy
            type: encrypt
            name: wire_transfer_encryption_policy
            condition: 'context.topic.name == "nw.ledger.transactions.high-value-wire-transfers.v1"'
            config:
              failure_mode: error
              part_of_record:
                - value
              encryption_key:
                type: static
                key:
                  id: !ref transaction-encryption-key

        consume_policies:
          - ref: wire-transfer-decryption-policy
            type: decrypt
            name: wire_transfer_decryption_policy
            condition: 'context.topic.name == "nw.ledger.transactions.high-value-wire-transfers.v1"'
            config:
              failure_mode: error
              part_of_record:
                - value
              key_sources:
                - type: static
                  key:
                    id: !ref transaction-encryption-key
```

**Demonstrate encrypt/decrypt:**

```bash
# Produce a wire transfer event through the gateway (port 19092 = Northwind-Core)
echo '{
  "transaction_id": "TXN-20240612-001",
  "customer_id": "NW-C-88421",
  "account_number": "****-9821",
  "full_name": "Eleanor Hartwell",
  "type": "buy",
  "instrument": "US10Y",
  "quantity": 500,
  "price_usd": 98.72,
  "settled_at": "2024-06-12T14:30:00Z"
}' | kafkactl produce nw.ledger.transactions.high-value-wire-transfers.v1 --context northwind-core
```

Now read the raw bytes directly from Kafka (bypassing the gateway) to show the payload is encrypted:

```bash
# Read raw from Kafka — payload is encrypted gibberish
docker exec -it kafka_cluster-kafka1-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic nw.ledger.transactions.high-value-wire-transfers.v1 --from-beginning
# → ÄxØ9...�ïþ (encrypted bytes — not readable)

# Read through the gateway — decrypted transparently
kafkactl consume nw.ledger.transactions.high-value-wire-transfers.v1 --context northwind-core -b
# → {"transaction_id": "TXN-20240612-001", "full_name": "Eleanor Hartwell", ...}
```

**Talking point:**
> "The producer didn't add any encryption logic. The consumer didn't add any decryption
> logic. The gateway owns that responsibility — and the key is managed centrally via the
> `static_keys` block in the config, loaded from an environment variable at startup.
>
> If you look at the raw Kafka topic, the data is opaque. An attacker who gains access
> to the broker's storage sees nothing useful. Only consumers routing through the gateway —
> with the decryption policy applied — see plaintext. This is exactly the kind of control
> your SOC 2 auditor is looking for in a data-in-use and data-at-rest story.
>
> The encryption key itself never touches the broker. Rotate it by updating the env var
> and restarting the data plane. No Kafka admin. No downtime."

**Key message:** _Schema validation stops bad data at the source. Field encryption means the broker never holds plaintext for sensitive events. Both are policy config — no app code changes._

---

## Closing (2 minutes)

```
"Let's recap what we built today — and how we built it:

  Phase 1 → Backend cluster registration.       1 kongctl apply.
  Phase 2 → Virtual clusters + namespaces.      1 kongctl apply.
  Phase 3 → Auth termination at the edge.       1 kongctl apply.
  Phase 4 → Claim-based ACL enforcement.        1 kongctl apply.
  Phase 5 → Schema validation + encryption.     1 kongctl apply.

Each phase added capability without touching a broker config or redeploying a producer.
Every policy is YAML. Every YAML is in Git. Every Git change goes through your normal
code review process.

The Northwind infosec team can now own the ACL policies. The data governance team can
own the schema registry integration. The compliance team can own the encryption key
lifecycle. None of them need Kafka cluster access to do their jobs.

That's the governance model we're proposing."
```

---

## Anticipated Questions

| Question | Response |
|---|---|
| "Does `insecure_allow_anonymous_virtual_cluster_auth: true` mean the cluster is insecure?" | "That flag tells the gateway to connect to Kafka as anonymous internally — because the gateway is the trusted edge. Kafka doesn't need to enforce client auth because no client reaches Kafka directly anymore. The gateway is the only client." |
| "What happens if the gateway goes down?" | "The data plane is stateless and horizontally scalable. In production you'd run multiple instances behind a load balancer. The control plane (Konnect) is SaaS and separate from the data path — a control plane outage doesn't affect in-flight traffic." |
| "Can we use JWT/OIDC instead of SASL/PLAIN?" | "Yes — the `sasl_oauthbearer` auth type with `mediation: terminate` works the same way. You configure your IdP endpoint, and the gateway validates the JWT. Conditions in ACL policies and produce/consume policies can reference any JWT claim." |
| "How do we rotate the encryption key without losing access to old messages?" | "The `key_sources` field on the decrypt policy accepts a list — old and new keys simultaneously. You add the new key, re-encrypt at your own pace, then retire the old one. No downtime, no message loss." |
| "What's the performance overhead?" | "The gateway adds microseconds of latency for policy evaluation. Encryption adds measurable overhead for large payloads, but the fan-out of key derivation is batched. We validate throughput targets during POV with your actual message sizes and volume." |
| "Can we manage this with Terraform instead of kongctl?" | "Yes — Konnect has a full Terraform provider. `kongctl` is the declarative CLI that mirrors Terraform's workflow for teams that prefer YAML over HCL. Both talk to the same Konnect API." |

---

## Recovery

**If the demo breaks:**
- Fallback: `recordings/keg-demo-full.mp4` — full walkthrough pre-recorded
- Have `kongctl/phase-5-policies.yaml` open in the editor — walk through the final config as a whiteboard

**If a topic is missing:**
```bash
kafkactl create topic WEALTH_LA.infosec.security.fraud.risk-scores.v3 --partitions 3
```

**If schema registry is unreachable:**
- Switch to the `schema_validation` produce policy with a hard-coded JSON schema (inline) rather than the registry reference — demonstrate the same rejection behaviour

---

## File Index

| File | Purpose |
|---|---|
| `kongctl/phase-1-backend-only.yaml` | Scene 2: Backend cluster registration |
| `kongctl/phase-2-virtual-clusters.yaml` | Scene 3: Virtual clusters + namespaces |
| `kongctl/phase-3-auth.yaml` | Scene 4: SASL/PLAIN termination |
| `kongctl/phase-4-acls.yaml` | Scene 5: Gateway-enforced ACLs |
| `kongctl/phase-5-policies.yaml` | Scene 6: Schema validation + encryption |
| `kafka/config/topics.txt` | Topic list for initial setup |
| `kafka/config/schemas/fraud_risk_scores.json` | JSON Schema for fraud risk scores — register in Apicurio before Scene 6 |
| `kafka/config/schemas/high_value_wire_transfers.avsc` | Avro schema for wire transfer topic (reference) |
| `docker-compose.yaml` | Gateway data plane |
| `kafka/docker-compose.yaml` | Kafka cluster (3-broker) |
