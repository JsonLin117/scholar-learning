# ACID vs BASE — Study Notes 2026-05-11

## Key Takeaways
1. ACID = strong consistency (all-or-nothing), BASE = eventual consistency (available but stale)
2. CAP mapping: CP ≈ ACID, AP ≈ BASE; P is unavoidable in distributed systems
3. Microservices: avoid cross-service 2PC → use Saga Pattern (local ACID + compensating txn)
4. NewSQL (Spanner TrueTime, CockroachDB HLC) breaks traditional CAP binary
5. SA's job: draw system boundaries — ACID inside the line, BASE outside

## ODM Application
- SAP ERP orders/payments → ACID
- Cross-region inventory dashboard → BASE  
- IoT sensor data → BASE
- Architecture: CQRS (ACID core → CDC → BASE analytics)

## SA Decision Framework
Ask: Can data tolerate temporary inconsistency? Need 24/7? Read/write volume? Cross-region? Budget?
