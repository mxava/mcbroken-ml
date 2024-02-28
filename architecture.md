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
    ML_MODEL }|--|{ POSTGRESQL_DB : accesses
    BACKEND_QUEUE_SVC }|--|{ POSTGRESQL_DB : accesses
    DEV_TOOLS ||--|{ BACKEND_QUEUE_SVC : accesses
    DEV_TOOLS ||--|| K8S_CLUSTER : accesses
```

## SVC Sequence Diagram - Frontend
```mermaid
sequenceDiagram
   participant PUBLIC_INTERNET
   participant FRONTEND_SVC
   participant BACKEND_SVC
   participant SQL_DB

   PUBLIC_INTERNET->>FRONTEND_SVC: sends request
   FRONTEND_SVC->>ML_MODEL: sends request
   ML_MODEL->>SQL_DB: query database
   SQL_DB->>BACKEND_SVC: return query
   BACKEND_SVC->>FRONTEND_SVC: returns response
   FRONTEND_SVC->>PUBLIC_INTERNET: returns response
```

## SVC Sequence Diagram - Backend
### Queue Handling
```mermaid
sequenceDiagram
   participant QUEUE
   participant BACKEND_SVC
   participant SQL_DB

   Note over BACKEND_SVC: orders queue by ascending time_modified
   QUEUE-->>BACKEND_SVC: fetches lowest item in queue
   Note over BACKEND_SVC,SQL_DB: does not not overwrite where<br/>DB last_modified >= queue time_modified
   BACKEND_SVC->>SQL_DB: updates database
   SQL_DB->>BACKEND_SVC: transaction completes
   BACKEND_SVC->>QUEUE: updates queue order
   QUEUE-->>BACKEND_SVC: fetches next item in queue
```
