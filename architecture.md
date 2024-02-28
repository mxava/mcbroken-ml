# Architecture
Everything you need to know about the architecture.
Latest version can be viewed [here](https://github.com/mxava/mcbroken-ml/blob/main/README.md)

## SVC Relationship Diagram
```mermaid
erDiagram
    PUBLIC_INTERNET }o--|| K8S_CLUSTER : inbound_connections
    K8S_CLUSTER ||--|{ FRONTEND_SVC : hosts
    K8S_CLUSTER ||--|{ ML_MODEL : hosts
    K8S_CLUSTER ||--|{ BACKEND_QUEUE_SVC : hosts
    K8S_CLUSTER ||--|{ POSTGRESQL_DB : hosts
    DEV_TOOLS ||--|{ BACKEND_QUEUE_SVC : accesses
    DEV_TOOLS ||--|| K8S_CLUSTER : accesses
```

## SVC Sequence Diagram
```mermaid
sequenceDiagram
   participant PUBLIC_INTERNET
   participant FRONTEND_SVC
   participant BACKEND_SVC
   participant SQL_DB

   PUBLIC_INTERNET->>FRONTEND_SVC: sends request
   FRONTEND_SVC->>BACKEND_SVC: sends request
   BACKEND_SVC->>SQL_DB: query database
   SQL_DB->>BACKEND_SVC: return query
   BACKEND_SVC->>FRONTEND_SVC: returns response
   FRONTEND_SVC->>PUBLIC_INTERNET: returns response
```
